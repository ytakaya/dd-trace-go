// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.

package tracer

import (
	"os"
	"strconv"
	"sync"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/internal"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"

	"github.com/DataDog/datadog-go/statsd"
)

var _ ddtrace.Tracer = (*tracer)(nil)

// tracer creates, buffers and submits Spans which are used to time blocks of
// computation. They are accumulated and streamed into an internal payload,
// which is flushed to the agent whenever its size exceeds a specific threshold
// or when a certain interval of time has passed, whichever happens first.
//
// tracer operates based on a worker loop which responds to various request
// channels. It additionally holds two buffers which accumulates error and trace
// queues to be processed by the payload encoder.
type tracer struct {
	*config
	*payload

	exitChan chan struct{}  // stop notifier
	stopped  chan struct{}  // will be closed when the worker exits
	climit   chan struct{}  // buffered channel that limits the number of concurrent outgoing connections
	wg       sync.WaitGroup // waits for all connections to finish before exiting the tracer.

	// spanChan receives finished spans to be added to the payload.
	spanChan chan *span

	// prioritySampling holds an instance of the priority sampler.
	prioritySampling *prioritySampler

	// pid of the process
	pid string
}

const (
	// flushInterval is the interval at which the payload contents will be flushed
	// to the transport.
	flushInterval = 2 * time.Second

	// payloadMaxLimit is the maximum payload size allowed and should indicate the
	// maximum size of the package that the agent can receive.
	payloadMaxLimit = 2 * 1024 * 1024 // 9.5 MB

	// payloadSizeLimit specifies the maximum allowed size of the payload before
	// it will trigger a flush to the transport.
	payloadSizeLimit = payloadMaxLimit / 2

	// concurrentConnectionLimit specifies the maximum number of concurrent outgoing
	// connections allowed.
	concurrentConnectionLimit = 100
)

// Start starts the tracer with the given set of options. It will stop and replace
// any running tracer, meaning that calling it several times will result in a restart
// of the tracer by replacing the current instance with a new one.
func Start(opts ...StartOption) {
	if internal.Testing {
		return // mock tracer active
	}
	internal.SetGlobalTracer(newTracer(opts...))
}

// Stop stops the started tracer. Subsequent calls are valid but become no-op.
func Stop() {
	internal.SetGlobalTracer(&internal.NoopTracer{})
	log.Flush()
}

// Span is an alias for ddtrace.Span. It is here to allow godoc to group methods returning
// ddtrace.Span. It is recommended and is considered more correct to refer to this type as
// ddtrace.Span instead.
type Span = ddtrace.Span

// StartSpan starts a new span with the given operation name and set of options.
// If the tracer is not started, calling this function is a no-op.
func StartSpan(operationName string, opts ...StartSpanOption) Span {
	return internal.GetGlobalTracer().StartSpan(operationName, opts...)
}

// Extract extracts a SpanContext from the carrier. The carrier is expected
// to implement TextMapReader, otherwise an error is returned.
// If the tracer is not started, calling this function is a no-op.
func Extract(carrier interface{}) (ddtrace.SpanContext, error) {
	return internal.GetGlobalTracer().Extract(carrier)
}

// Inject injects the given SpanContext into the carrier. The carrier is
// expected to implement TextMapWriter, otherwise an error is returned.
// If the tracer is not started, calling this function is a no-op.
func Inject(ctx ddtrace.SpanContext, carrier interface{}) error {
	return internal.GetGlobalTracer().Inject(ctx, carrier)
}

// payloadQueueSize is the buffer size of the trace channel.
const payloadQueueSize = 1000

func newUnstartedTracer(opts ...StartOption) *tracer {
	c := new(config)
	defaults(c)
	for _, fn := range opts {
		fn(c)
	}
	if c.transport == nil {
		c.transport = newTransport(c.agentAddr, c.httpRoundTripper)
	}
	if c.propagator == nil {
		c.propagator = NewPropagator(nil)
	}
	if c.logger != nil {
		log.UseLogger(c.logger)
	}
	if c.debug {
		log.SetLevel(log.LevelDebug)
	}
	t := &tracer{
		config:           c,
		payload:          newPayload(),
		exitChan:         make(chan struct{}),
		spanChan:         make(chan *span, payloadQueueSize),
		stopped:          make(chan struct{}),
		prioritySampling: newPrioritySampler(),
		pid:              strconv.Itoa(os.Getpid()),
		climit:           make(chan struct{}, concurrentConnectionLimit),
	}
	if c.runtimeMetrics {
		statsd, err := statsd.NewBuffered(t.config.dogstatsdAddr, 40)
		if err != nil {
			log.Warn("Runtime metrics disabled: %v", err)
		} else {
			log.Debug("Runtime metrics enabled.")
			go t.reportMetrics(statsd, defaultMetricsReportInterval)
		}
	}
	return t
}

func newTracer(opts ...StartOption) *tracer {
	t := newUnstartedTracer(opts...)

	go func() {
		defer close(t.stopped)
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		t.worker(ticker.C)
	}()

	return t
}

// worker receives finished traces to be added into the payload, as well
// as periodically flushes traces to the transport.
func (t *tracer) worker(tick <-chan time.Time) {
	for {
		select {
		case span := <-t.spanChan:
			t.addToPayload(span)

		case <-tick:
			t.flush()

		case <-t.exitChan:
		loop:
			// the loop ensures that the payload channel is fully drained
			// before the final flush to ensure no traces are lost (see #526)
			for {
				select {
				case span := <-t.spanChan:
					t.addToPayload(span)
				default:
					break loop
				}
			}
			t.flush()
			return
		}
	}
}

func (t *tracer) pushSpan(s *span) {
	select {
	case <-t.stopped:
		return
	default:
	}
	select {
	case t.spanChan <- s:
		// OK
	default:
		log.Error("payload queue full, dropping span")
	}
}

// StartSpan creates, starts, and returns a new Span with the given `operationName`.
func (t *tracer) StartSpan(operationName string, options ...ddtrace.StartSpanOption) ddtrace.Span {
	var opts ddtrace.StartSpanConfig
	for _, fn := range options {
		fn(&opts)
	}
	var startTime int64
	if opts.StartTime.IsZero() {
		startTime = now()
	} else {
		startTime = opts.StartTime.UnixNano()
	}
	var context *spanContext
	if opts.Parent != nil {
		if ctx, ok := opts.Parent.(*spanContext); ok {
			context = ctx
		}
	}
	id := opts.SpanID
	if id == 0 {
		id = random.Uint64()
	}
	// span defaults
	span := &span{
		Name:     operationName,
		Service:  t.config.serviceName,
		Resource: operationName,
		SpanID:   id,
		TraceID:  id,
		Start:    startTime,
		taskEnd:  startExecutionTracerTask(operationName),
	}
	if context != nil {
		// this is a child span
		span.TraceID = context.traceID
		span.ParentID = context.spanID
		if context.hasSamplingPriority() {
			span.setMetric(keySamplingPriority, float64(context.samplingPriority()))
		}
		if context.span != nil {
			// local parent, inherit service
			context.span.RLock()
			span.Service = context.span.Service
			context.span.RUnlock()
		} else {
			// remote parent
			if context.origin != "" {
				// mark origin
				span.setMeta(keyOrigin, context.origin)
			}
		}
	}
	span.context = newSpanContext(span, context)
	if context == nil || context.span == nil {
		// this is either a root span or it has a remote parent (local root)
		span.setMetric(keyRootSpan, 1)
		span.setMeta(ext.Pid, t.pid)
		if t.hostname != "" {
			span.setMeta(keyHostname, t.hostname)
		}
		if id := getContainerID(); id != "" {
			span.setMeta(keyContainerID, id)
		}
		if _, ok := opts.Tags[ext.ServiceName]; !ok && t.config.runtimeMetrics {
			// this is a root span in the global service; runtime metrics should
			// be linked to it:
			span.setMeta("language", "go")
		}
	}
	// add tags from options
	for k, v := range opts.Tags {
		span.SetTag(k, v)
	}
	// add global tags
	for k, v := range t.config.globalTags {
		span.SetTag(k, v)
	}
	if context == nil {
		// this is a brand new trace, sample it
		t.sample(span)
	}
	return span
}

// Stop stops the tracer.
func (t *tracer) Stop() {
	select {
	case <-t.stopped:
		return
	default:
		t.exitChan <- struct{}{}
		<-t.stopped
		t.wg.Wait()
	}
}

// Inject uses the configured or default TextMap Propagator.
func (t *tracer) Inject(ctx ddtrace.SpanContext, carrier interface{}) error {
	return t.config.propagator.Inject(ctx, carrier)
}

// Extract uses the configured or default TextMap Propagator.
func (t *tracer) Extract(carrier interface{}) (ddtrace.SpanContext, error) {
	return t.config.propagator.Extract(carrier)
}

// flush will push any currently buffered traces to the server.
func (t *tracer) flush() {
	if t.payload.itemCount() == 0 {
		return
	}
	t.climit <- struct{}{}
	t.wg.Add(1)
	go func(p *payload) {
		defer func() {
			<-t.climit
			t.wg.Done()
		}()
		size, count := p.size(), p.itemCount()
		log.Debug("Sending payload: size: %d spans: %d\n", size, count)
		rc, err := t.config.transport.send(p)
		if err != nil {
			log.Error("lost %d spans: %v", count, err)
		}
		if err == nil {
			t.prioritySampling.readRatesJSON(rc) // TODO: handle error?
		}
	}(t.payload)
	t.payload = newPayload()
}

// addToPayload pushes the span onto the payload. If the payload becomes
// larger than the threshold as a result, it sends a flush request.
func (t *tracer) addToPayload(s *span) {
	if err := t.payload.push(s); err != nil {
		log.Error("error encoding msgpack: %v", err)
	}
	// TODO: explore using a sync.Pool of spans
	if t.payload.size() > payloadSizeLimit {
		// getting large
		t.flush()
	}
}

// sampleRateMetricKey is the metric key holding the applied sample rate. Has to be the same as the Agent.
const sampleRateMetricKey = "_sample_rate"

// Sample samples a span with the internal sampler.
func (t *tracer) sample(span *span) {
	if span.context.hasSamplingPriority() {
		// sampling decision was already made
		return
	}
	sampler := t.config.sampler
	if !sampler.Sample(span) {
		span.context.drop = true
		return
	}
	if rs, ok := sampler.(RateSampler); ok && rs.Rate() < 1 {
		span.setMetric(sampleRateMetricKey, rs.Rate())
	}
	t.prioritySampling.apply(span)
}
