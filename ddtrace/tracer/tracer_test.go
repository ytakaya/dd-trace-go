// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.

package tracer

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/internal"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"

	"github.com/stretchr/testify/assert"
	"github.com/tinylib/msgp/msgp"
)

func (t *tracer) newEnvSpan(service, env string) *span {
	return t.StartSpan("test.op", SpanType("test"), ServiceName(service), ResourceName("/"), Tag(ext.Environment, env)).(*span)
}

func (t *tracer) newRootSpan(name, service, resource string) *span {
	return t.StartSpan(name, SpanType("test"), ServiceName(service), ResourceName(resource)).(*span)
}

func (t *tracer) newChildSpan(name string, parent *span) *span {
	if parent == nil {
		return t.StartSpan(name).(*span)
	}
	return t.StartSpan(name, ChildOf(parent.Context())).(*span)
}

// TestTracerFrenetic does frenetic testing in a scenario where the tracer is started
// and stopped in parallel with spans being created.
func TestTracerCleanStop(t *testing.T) {
	if testing.Short() {
		return
	}
	log.SetLevel(log.LevelWarn)
	var wg sync.WaitGroup
	transport := newDummyTransport()

	n := 5000

	wg.Add(3)
	for j := 0; j < 3; j++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				span := StartSpan("test.span")
				child := StartSpan("child.span", ChildOf(span.Context()))
				time.Sleep(time.Millisecond)
				child.Finish()
				time.Sleep(time.Millisecond)
				span.Finish()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			Start(withTransport(transport))
			time.Sleep(time.Millisecond)
			Start(withTransport(transport), WithSampler(NewRateSampler(0.99)))
			Start(withTransport(transport), WithSampler(NewRateSampler(0.99)))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			Stop()
			Stop()
			Stop()
			time.Sleep(time.Millisecond)
			Stop()
			Stop()
			Stop()
		}
	}()

	wg.Wait()
}

func TestTracerStart(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		Start()
		defer Stop()
		if _, ok := internal.GetGlobalTracer().(*tracer); !ok {
			t.Fail()
		}
	})

	t.Run("testing", func(t *testing.T) {
		internal.Testing = true
		Start()
		defer Stop()
		if _, ok := internal.GetGlobalTracer().(*tracer); ok {
			t.Fail()
		}
		if _, ok := internal.GetGlobalTracer().(*internal.NoopTracer); !ok {
			t.Fail()
		}
		internal.Testing = false
	})

	t.Run("deadlock/api", func(t *testing.T) {
		Stop()
		Stop()

		Start()
		Start()
		Start()

		// ensure at least one worker started and handles requests
		internal.GetGlobalTracer().(*tracer).spanChan <- &span{}

		Stop()
		Stop()
		Stop()
		Stop()
	})

	t.Run("deadlock/direct", func(t *testing.T) {
		tr, _, _, stop := startTestTracer(t)
		defer stop()
		go func() {
			defer close(tr.stopped)
			tr.worker(make(chan time.Time))
		}()
		tr.spanChan <- &span{} // wait for a worker to be up
		select {
		case <-tr.stopped:
			t.Fatal("stopped channel should be open")
		default:
			// OK
		}
		tr.Stop()
		select {
		case <-tr.stopped:
			// OK
		default:
			t.Fatal("stopped channel should be closed")
		}
		tr.Stop()
		tr.Stop()
	})
}

func TestTracerStartSpan(t *testing.T) {
	t.Run("generic", func(t *testing.T) {
		tracer := newTracer()
		span := tracer.StartSpan("web.request").(*span)
		assert := assert.New(t)
		assert.NotEqual(uint64(0), span.TraceID)
		assert.NotEqual(uint64(0), span.SpanID)
		assert.Equal(uint64(0), span.ParentID)
		assert.Equal("web.request", span.Name)
		assert.Equal("tracer.test", span.Service)
		assert.Contains([]float64{
			ext.PriorityAutoReject,
			ext.PriorityAutoKeep,
		}, span.Metrics[keySamplingPriority])
	})

	t.Run("priority", func(t *testing.T) {
		tracer := newTracer()
		span := tracer.StartSpan("web.request", Tag(ext.SamplingPriority, ext.PriorityUserKeep)).(*span)
		assert.Equal(t, float64(ext.PriorityUserKeep), span.Metrics[keySamplingPriority])
	})

	t.Run("name", func(t *testing.T) {
		tracer := newTracer()
		span := tracer.StartSpan("/home/user", Tag(ext.SpanName, "db.query")).(*span)
		assert.Equal(t, "db.query", span.Name)
		assert.Equal(t, "/home/user", span.Resource)
	})
}

func TestTracerRuntimeMetrics(t *testing.T) {
	t.Run("on", func(t *testing.T) {
		tp := new(testLogger)
		tracer := newTracer(WithRuntimeMetrics(), WithLogger(tp), WithDebugMode(true))
		defer tracer.Stop()
		assert.Contains(t, tp.Lines()[0], "DEBUG: Runtime metrics enabled")
	})

	t.Run("off", func(t *testing.T) {
		tp := new(testLogger)
		tracer := newTracer(WithLogger(tp), WithDebugMode(true))
		defer tracer.Stop()
		assert.Len(t, tp.Lines(), 0)
		s := tracer.StartSpan("op").(*span)
		_, ok := s.Meta["language"]
		assert.False(t, ok)
	})

	t.Run("spans", func(t *testing.T) {
		tracer := newTracer(WithRuntimeMetrics(), WithServiceName("main"))
		defer tracer.Stop()

		s := tracer.StartSpan("op").(*span)
		assert.Equal(t, s.Meta["language"], "go")

		s = tracer.StartSpan("op", ServiceName("secondary")).(*span)
		_, ok := s.Meta["language"]
		assert.False(t, ok)
	})
}

func TestTracerStartSpanOptions(t *testing.T) {
	tracer := newTracer()
	now := time.Now()
	opts := []StartSpanOption{
		SpanType("test"),
		ServiceName("test.service"),
		ResourceName("test.resource"),
		StartTime(now),
		WithSpanID(420),
	}
	span := tracer.StartSpan("web.request", opts...).(*span)
	assert := assert.New(t)
	assert.Equal("test", span.Type)
	assert.Equal("test.service", span.Service)
	assert.Equal("test.resource", span.Resource)
	assert.Equal(now.UnixNano(), span.Start)
	assert.Equal(uint64(420), span.SpanID)
	assert.Equal(uint64(420), span.TraceID)
}

func TestTracerStartChildSpan(t *testing.T) {
	t.Run("own-service", func(t *testing.T) {
		assert := assert.New(t)
		tracer := newTracer()
		root := tracer.StartSpan("web.request", ServiceName("root-service")).(*span)
		child := tracer.StartSpan("db.query",
			ChildOf(root.Context()),
			ServiceName("child-service"),
			WithSpanID(69)).(*span)

		assert.NotEqual(uint64(0), child.TraceID)
		assert.NotEqual(uint64(0), child.SpanID)
		assert.Equal(root.SpanID, child.ParentID)
		assert.Equal(root.TraceID, child.ParentID)
		assert.Equal(root.TraceID, child.TraceID)
		assert.Equal(uint64(69), child.SpanID)
		assert.Equal("child-service", child.Service)
	})

	t.Run("inherit-service", func(t *testing.T) {
		assert := assert.New(t)
		tracer := newTracer()
		root := tracer.StartSpan("web.request", ServiceName("root-service")).(*span)
		child := tracer.StartSpan("db.query", ChildOf(root.Context())).(*span)

		assert.Equal("root-service", child.Service)
	})
}

func TestTracerBaggagePropagation(t *testing.T) {
	assert := assert.New(t)
	tracer := newTracer()
	root := tracer.StartSpan("web.request").(*span)
	root.SetBaggageItem("key", "value")
	child := tracer.StartSpan("db.query", ChildOf(root.Context())).(*span)
	context := child.Context().(*spanContext)

	assert.Equal("value", context.baggage["key"])
}

func TestStartSpanOrigin(t *testing.T) {
	assert := assert.New(t)

	tracer := newTracer()

	carrier := TextMapCarrier(map[string]string{
		DefaultTraceIDHeader:  "1",
		DefaultParentIDHeader: "1",
		originHeader:          "synthetics",
	})
	ctx, err := tracer.Extract(carrier)
	assert.Nil(err)

	// first child contains tag
	child := tracer.StartSpan("child", ChildOf(ctx))
	assert.Equal("synthetics", child.(*span).Meta[keyOrigin])

	// secondary child doesn't
	child2 := tracer.StartSpan("child2", ChildOf(child.Context()))
	assert.Empty(child2.(*span).Meta[keyOrigin])

	// but injecting its context marks origin
	carrier2 := TextMapCarrier(map[string]string{})
	err = tracer.Inject(child2.Context(), carrier2)
	assert.Nil(err)
	assert.Equal("synthetics", carrier2[originHeader])
}

func TestPropagationDefaults(t *testing.T) {
	assert := assert.New(t)

	tracer := newTracer()
	root := tracer.StartSpan("web.request").(*span)
	root.SetBaggageItem("x", "y")
	root.SetTag(ext.SamplingPriority, -1)
	ctx := root.Context().(*spanContext)
	headers := http.Header{}

	// inject the spanContext
	carrier := HTTPHeadersCarrier(headers)
	err := tracer.Inject(ctx, carrier)
	assert.Nil(err)

	tid := strconv.FormatUint(root.TraceID, 10)
	pid := strconv.FormatUint(root.SpanID, 10)

	assert.Equal(headers.Get(DefaultTraceIDHeader), tid)
	assert.Equal(headers.Get(DefaultParentIDHeader), pid)
	assert.Equal(headers.Get(DefaultBaggageHeaderPrefix+"x"), "y")
	assert.Equal(headers.Get(DefaultPriorityHeader), "-1")

	// retrieve the spanContext
	propagated, err := tracer.Extract(carrier)
	assert.Nil(err)
	pctx := propagated.(*spanContext)

	// compare if there is a Context match
	assert.Equal(ctx.traceID, pctx.traceID)
	assert.Equal(ctx.spanID, pctx.spanID)
	assert.Equal(ctx.baggage, pctx.baggage)
	assert.Equal(*ctx.priority, -1.)

	// ensure a child can be created
	child := tracer.StartSpan("db.query", ChildOf(propagated)).(*span)

	assert.NotEqual(uint64(0), child.TraceID)
	assert.NotEqual(uint64(0), child.SpanID)
	assert.Equal(root.SpanID, child.ParentID)
	assert.Equal(root.TraceID, child.ParentID)
	assert.Equal(*child.context.priority, -1.)
}

func TestTracerSamplingPriorityPropagation(t *testing.T) {
	assert := assert.New(t)
	tracer := newTracer()
	root := tracer.StartSpan("web.request", Tag(ext.SamplingPriority, 2)).(*span)
	child := tracer.StartSpan("db.query", ChildOf(root.Context())).(*span)
	assert.EqualValues(2, root.Metrics[keySamplingPriority])
	assert.EqualValues(2, child.Metrics[keySamplingPriority])
	assert.EqualValues(2., *root.context.priority)
	assert.EqualValues(2., *child.context.priority)
}

func TestTracerBaggageImmutability(t *testing.T) {
	assert := assert.New(t)
	tracer := newTracer()
	root := tracer.StartSpan("web.request").(*span)
	root.SetBaggageItem("key", "value")
	child := tracer.StartSpan("db.query", ChildOf(root.Context())).(*span)
	child.SetBaggageItem("key", "changed!")
	parentContext := root.Context().(*spanContext)
	childContext := child.Context().(*spanContext)
	assert.Equal("value", parentContext.baggage["key"])
	assert.Equal("changed!", childContext.baggage["key"])
}

func TestTracerSpanTags(t *testing.T) {
	tracer := newTracer()
	tag := Tag("key", "value")
	span := tracer.StartSpan("web.request", tag).(*span)
	assert := assert.New(t)
	assert.Equal("value", span.Meta["key"])
}

func TestTracerSpanGlobalTags(t *testing.T) {
	assert := assert.New(t)
	tracer := newTracer(WithGlobalTag("key", "value"))
	s := tracer.StartSpan("web.request").(*span)
	assert.Equal("value", s.Meta["key"])
	child := tracer.StartSpan("db.query", ChildOf(s.Context())).(*span)
	assert.Equal("value", child.Meta["key"])
}

func TestNewSpan(t *testing.T) {
	assert := assert.New(t)

	// the tracer must create root spans
	tracer := newTracer(withTransport(newDefaultTransport()))
	span := tracer.newRootSpan("pylons.request", "pylons", "/")
	assert.Equal(uint64(0), span.ParentID)
	assert.Equal("pylons", span.Service)
	assert.Equal("pylons.request", span.Name)
	assert.Equal("/", span.Resource)
}

func TestNewSpanChild(t *testing.T) {
	assert := assert.New(t)

	// the tracer must create child spans
	tracer := newTracer(withTransport(newDefaultTransport()))
	parent := tracer.newRootSpan("pylons.request", "pylons", "/")
	child := tracer.newChildSpan("redis.command", parent)
	// ids and services are inherited
	assert.Equal(parent.SpanID, child.ParentID)
	assert.Equal(parent.TraceID, child.TraceID)
	assert.Equal(parent.Service, child.Service)
	// the resource is not inherited and defaults to the name
	assert.Equal("redis.command", child.Resource)
}

func TestNewRootSpanHasPid(t *testing.T) {
	assert := assert.New(t)

	tracer := newTracer(withTransport(newDefaultTransport()))
	root := tracer.newRootSpan("pylons.request", "pylons", "/")

	assert.Equal(strconv.Itoa(os.Getpid()), root.Meta[ext.Pid])
}

func TestNewChildHasNoPid(t *testing.T) {
	assert := assert.New(t)

	tracer := newTracer(withTransport(newDefaultTransport()))
	root := tracer.newRootSpan("pylons.request", "pylons", "/")
	child := tracer.newChildSpan("redis.command", root)

	assert.Equal("", child.Meta[ext.Pid])
}

func TestTracerSampler(t *testing.T) {
	assert := assert.New(t)

	sampler := NewRateSampler(0.9999) // high probability of sampling
	tracer := newTracer(
		withTransport(newDefaultTransport()),
		WithSampler(sampler),
	)

	span := tracer.newRootSpan("pylons.request", "pylons", "/")

	if !sampler.Sample(span) {
		t.Skip("wasn't sampled") // no flaky tests
	}
	// only run test if span was sampled to avoid flaky tests
	_, ok := span.Metrics[sampleRateMetricKey]
	assert.True(ok)
}

func TestTracerPrioritySampler(t *testing.T) {
	assert := assert.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"rate_by_service":{
				"service:,env:":0.1,
				"service:my-service,env:":0.2,
				"service:my-service,env:default":0.2,
				"service:my-service,env:other":0.3
			}
		}`))
	}))
	addr := srv.Listener.Addr().String()

	tr, _, flush, stop := startTestTracer(t,
		withTransport(newHTTPTransport(addr, defaultRoundTripper)),
	)
	defer stop()

	// default rates (1.0)
	s := tr.newEnvSpan("pylons", "")
	assert.Equal(1., s.Metrics[keySamplingPriorityRate])
	assert.Equal(1., s.Metrics[keySamplingPriority])
	p, ok := s.context.samplingPriority()
	assert.True(ok)
	assert.EqualValues(p, s.Metrics[keySamplingPriority])
	s.Finish()

	waitForPayloadCount(t, tr.payload, 1)
	flush(-1) // trigger a flush
	// wait for priority sampling to be set
	timeout := time.After(2 * time.Second)
loop:
	for {
		select {
		case <-timeout:
			t.Fatal("timed out waiting for rates")
		default:
			if tr.prioritySampling.getRate(&span{Service: "Unknown"}) == 0.1 {
				break loop
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	for i, tt := range []struct {
		service, env string
		rate         float64
	}{
		{
			service: "pylons",
			rate:    0.1,
		},
		{
			service: "my-service",
			rate:    0.2,
		},
		{
			service: "my-service",
			env:     "default",
			rate:    0.2,
		},
		{
			service: "my-service",
			env:     "other",
			rate:    0.3,
		},
	} {
		s := tr.newEnvSpan(tt.service, tt.env)
		assert.Equal(tt.rate, s.Metrics[keySamplingPriorityRate], strconv.Itoa(i))
		prio, ok := s.Metrics[keySamplingPriority]
		assert.True(ok)
		assert.Contains([]float64{0, 1}, prio)

		p, ok := s.context.samplingPriority()
		assert.True(ok)
		assert.EqualValues(p, prio)

		// injectable
		h := make(http.Header)
		tr.Inject(s.Context(), HTTPHeadersCarrier(h))
		assert.Equal(strconv.Itoa(int(prio)), h.Get(DefaultPriorityHeader))
	}
}

func TestTracerEdgeSampler(t *testing.T) {
	// a sample rate of 0 should sample nothing
	tracer0, _, _, stop := startTestTracer(t,
		withTransport(newDefaultTransport()),
		WithSampler(NewRateSampler(0)),
	)
	defer stop()
	// a sample rate of 1 should sample everything
	tracer1, _, _, stop := startTestTracer(t,
		withTransport(newDefaultTransport()),
		WithSampler(NewRateSampler(1)),
	)
	defer stop()

	count := payloadQueueSize / 3

	for i := 0; i < count; i++ {
		span0 := tracer0.newRootSpan("pylons.request", "pylons", "/")
		span0.Finish()
		span1 := tracer1.newRootSpan("pylons.request", "pylons", "/")
		span1.Finish()
	}

	waitForPayloadCount(t, tracer0.payload, 0)
	waitForPayloadCount(t, tracer1.payload, count)
}

func TestTracerConcurrent(t *testing.T) {
	assert := assert.New(t)
	tracer, transport, flush, stop := startTestTracer(t)
	defer stop()

	// Wait for three different goroutines that should create
	// three different traces with one child each
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		tracer.newRootSpan("pylons.request", "pylons", "/").Finish()
	}()
	go func() {
		defer wg.Done()
		tracer.newRootSpan("pylons.request", "pylons", "/home").Finish()
	}()
	go func() {
		defer wg.Done()
		tracer.newRootSpan("pylons.request", "pylons", "/trace").Finish()
	}()

	wg.Wait()
	flush(3)
	spans := transport.Spans()
	assert.Len(spans, 3)
}

func TestTracerParentFinishBeforeChild(t *testing.T) {
	assert := assert.New(t)
	tracer, transport, flush, stop := startTestTracer(t)
	defer stop()

	// Testing an edge case: a child refers to a parent that is already closed.

	parent := tracer.newRootSpan("pylons.request", "pylons", "/")
	parent.Finish()

	flush(1)

	spans := transport.Spans()
	assert.Len(spans, 1)
	comparePayloadSpans(t, parent, spans[0])

	child := tracer.newChildSpan("redis.command", parent)
	child.Finish()

	flush(1)

	spans = transport.Spans()
	assert.Len(spans, 1)
	comparePayloadSpans(t, child, spans[0])
	assert.Equal(parent.SpanID, spans[0].ParentID, "child should refer to parent, even if they have been flushed separately")
}

func TestTracerConcurrentMultipleSpans(t *testing.T) {
	assert := assert.New(t)
	tracer, transport, flush, stop := startTestTracer(t)
	defer stop()

	// Wait for two different goroutines that should create
	// two traces with two children each
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		parent := tracer.newRootSpan("pylons.request", "pylons", "/")
		child := tracer.newChildSpan("redis.command", parent)
		child.Finish()
		parent.Finish()
	}()
	go func() {
		defer wg.Done()
		parent := tracer.newRootSpan("pylons.request", "pylons", "/")
		child := tracer.newChildSpan("redis.command", parent)
		child.Finish()
		parent.Finish()
	}()

	wg.Wait()
	flush(4)
	traces := transport.Spans()
	assert.Len(traces, 4)
}

func TestTracerRace(t *testing.T) {
	tracer, _, _, stop := startTestTracer(t)
	defer stop()

	total := payloadQueueSize / 3
	var wg sync.WaitGroup
	wg.Add(total)

	// Trying to be quite brutal here, firing lots of concurrent things, finishing in
	// different orders, and modifying spans after creation.
	for n := 0; n < total; n++ {
		i := n // keep local copy
		odd := ((i % 2) != 0)
		go func() {
			if i%11 == 0 {
				time.Sleep(time.Microsecond)
			}

			parent := tracer.newRootSpan("pylons.request", "pylons", "/")

			tracer.newChildSpan("redis.command", parent).Finish()
			child := tracer.newChildSpan("async.service", parent)

			if i%13 == 0 {
				time.Sleep(time.Microsecond)
			}

			if odd {
				parent.SetTag("odd", "true")
				parent.SetTag("oddity", 1)
				parent.Finish()
			} else {
				child.SetTag("odd", "false")
				child.SetTag("oddity", 0)
				child.Finish()
			}

			if i%17 == 0 {
				time.Sleep(time.Microsecond)
			}

			if odd {
				child.Resource = "HGETALL"
				child.SetTag("odd", "false")
				child.SetTag("oddity", 0)
			} else {
				parent.Resource = "/" + strconv.Itoa(i) + ".html"
				parent.SetTag("odd", "true")
				parent.SetTag("oddity", 1)
			}

			if i%19 == 0 {
				time.Sleep(time.Microsecond)
			}

			if odd {
				child.Finish()
			} else {
				parent.Finish()
			}

			wg.Done()
		}()
	}

	wg.Wait()
}

// TestWorker is definitely a flaky test, as here we test that the worker
// background task actually does flush things. Most other tests are and should
// be using forceFlush() to make sure things are really sent to transport.
// Here, we just wait until things show up, as we would do with a real program.
func TestWorker(t *testing.T) {
	if testing.Short() {
		return
	}
	assert := assert.New(t)

	tracer, transport, _, stop := startTestTracer(t)
	defer stop()

	n := payloadQueueSize * 10 // put more traces than the chan size, on purpose
	for i := 0; i < n; i++ {
		root := tracer.newRootSpan("pylons.request", "pylons", "/")
		child := tracer.newChildSpan("redis.command", root)
		child.Finish()
		root.Finish()
	}

	now := time.Now()
	count := 0
	for time.Now().Before(now.Add(time.Minute)) && count < payloadQueueSize {
		nbTraces := len(transport.Spans())
		if nbTraces > 0 {
			t.Logf("popped %d traces", nbTraces)
		}
		count += nbTraces
		time.Sleep(time.Millisecond)
	}
	// here we just check that we have "enough traces". In practice, lots of them
	// are dropped, it's another interesting side-effect of this test: it does
	// trigger error messages (which are repeated, so it aggregates them etc.)
	if count < payloadQueueSize {
		assert.Fail(fmt.Sprintf("timeout, not enough traces in buffer (%d/%d)", count, n))
	}
}

func TestPushPayload(t *testing.T) {
	tracer, transport, _, stop := startTestTracer(t)
	defer stop()

	s := newBasicSpan("3MB")
	s.Meta["key"] = strings.Repeat("X", payloadSizeLimit/2+10)

	// half payload size reached, we have 1 item, no flush request
	tracer.addToPayload(s)
	assert.Equal(t, tracer.payload.itemCount(), 1)

	// payload size exceeded, we have no items and a flush
	tracer.addToPayload(s)
	assert.Equal(t, tracer.payload.itemCount(), 0)

	expectFlush(t, transport, 2)

	spans := transport.Spans()
	assert.Len(t, spans, 2)
	assert.Equal(t, spans[0].SpanID, s.SpanID)
	assert.Equal(t, spans[1].SpanID, s.SpanID)
}

func TestPushTrace(t *testing.T) {
	assert := assert.New(t)

	tp := new(testLogger)
	log.UseLogger(tp)
	tracer, _, _, stop := startTestTracer(t)
	defer stop()
	trace := []*span{
		&span{
			Name:     "pylons.request",
			Service:  "pylons",
			Resource: "/",
		},
		&span{
			Name:     "pylons.request",
			Service:  "pylons",
			Resource: "/foo",
		},
	}
	for _, span := range trace {
		tracer.pushSpan(span)
	}

	assert.Len(tracer.spanChan, 2)

	t0 := <-tracer.spanChan
	assert.True(reflect.DeepEqual(trace[0], t0) || reflect.DeepEqual(trace[1], t0))

	many := payloadQueueSize + 2
	for i := 0; i < many; i++ {
		for j := 0; j < many; j++ {
			tracer.pushSpan(&span{})
		}
	}
	log.Flush()
	assert.Contains(fmt.Sprint(tp.Lines()), "payload queue full")
	assert.Contains(fmt.Sprint(tp.Lines()), "200+ additional messages")
}

func TestTracerFlush(t *testing.T) {
	// https://github.com/DataDog/dd-trace-go/issues/377

	t.Run("direct", func(t *testing.T) {
		tracer, transport, flush, stop := startTestTracer(t)
		defer stop()
		defer transport.Reset()
		assert := assert.New(t)
		root := tracer.StartSpan("root")
		tracer.StartSpan("child.direct", ChildOf(root.Context())).Finish()
		root.Finish()

		flush(2)

		list := transport.Spans()
		assert.Len(list, 2)
		assert.Equal("child.direct", list[0].Name)
		assert.Equal("root", list[1].Name)
	})

	t.Run("extracted", func(t *testing.T) {
		tracer, transport, flush, stop := startTestTracer(t)
		defer stop()
		defer transport.Reset()
		assert := assert.New(t)
		root := tracer.StartSpan("root")
		h := HTTPHeadersCarrier(http.Header{})
		if err := tracer.Inject(root.Context(), h); err != nil {
			t.Fatal(err)
		}
		sctx, err := tracer.Extract(h)
		if err != nil {
			t.Fatal(err)
		}
		tracer.StartSpan("child.extracted", ChildOf(sctx)).Finish()

		flush(1)

		list := transport.Spans()
		assert.Len(list, 1)
		assert.Equal("child.extracted", list[0].Name)
	})
}

func TestTracerReportsHostname(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		os.Setenv("DD_TRACE_REPORT_HOSTNAME", "true")
		defer os.Unsetenv("DD_TRACE_REPORT_HOSTNAME")

		tracer, _, _, stop := startTestTracer(t)
		defer stop()

		root := tracer.StartSpan("root").(*span)
		child := tracer.StartSpan("child", ChildOf(root.Context())).(*span)
		child.Finish()
		root.Finish()

		assert := assert.New(t)

		name, ok := root.Meta[keyHostname]
		assert.True(ok)
		assert.Equal(name, tracer.hostname)

		_, ok = child.Meta[keyHostname]
		assert.False(ok)
	})

	t.Run("disabled", func(t *testing.T) {
		tracer, _, _, stop := startTestTracer(t)
		defer stop()

		root := tracer.StartSpan("root").(*span)
		child := tracer.StartSpan("child", ChildOf(root.Context())).(*span)
		child.Finish()
		root.Finish()

		assert := assert.New(t)

		_, ok := root.Meta[keyHostname]
		assert.False(ok)
		_, ok = child.Meta[keyHostname]
		assert.False(ok)
	})
}

// BenchmarkConcurrentTracing tests the performance of spawning a lot of
// goroutines where each one creates a trace with a parent and a child.
func BenchmarkConcurrentTracing(b *testing.B) {
	tracer, _, _, stop := startTestTracer(b, WithSampler(NewRateSampler(0)))
	defer stop()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func() {
			parent := tracer.StartSpan("pylons.request", ServiceName("pylons"), ResourceName("/"))
			defer parent.Finish()

			for i := 0; i < 10; i++ {
				tracer.StartSpan("redis.command", ChildOf(parent.Context())).Finish()
			}
		}()
	}
}

// BenchmarkTracerAddSpans tests the performance of creating and finishing a root
// span. It should include the encoding overhead.
func BenchmarkTracerAddSpans(b *testing.B) {
	tracer, _, _, stop := startTestTracer(b, WithSampler(NewRateSampler(0)))
	defer stop()

	for n := 0; n < b.N; n++ {
		span := tracer.StartSpan("pylons.request", ServiceName("pylons"), ResourceName("/"))
		span.Finish()
	}
}

func BenchmarkStartSpan(b *testing.B) {
	tracer, _, _, stop := startTestTracer(b, WithSampler(NewRateSampler(0)))
	defer stop()
	root := tracer.StartSpan("pylons.request", ServiceName("pylons"), ResourceName("/"))
	ctx := ContextWithSpan(context.TODO(), root)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s, ok := SpanFromContext(ctx)
		if !ok {
			b.Fatal("no span")
		}
		StartSpan("op", ChildOf(s.Context()))
	}
}

// startTestTracer returns a test tracer, a dummy transport which stores all sent spans, a flush function
// which flushes any active payload and monitors the transport until it receives n spans, and a stop function.
func startTestTracer(t interface {
	Fatalf(format string, args ...interface{})
}, opts ...StartOption) (
	testTracer *tracer,
	testTransport *dummyTransport,
	flushFunc func(n int),
	stopFunc func(),
) {
	transport := newDummyTransport()
	o := append([]StartOption{withTransport(transport)}, opts...)
	tracer := newUnstartedTracer(o...)
	tick := make(chan time.Time)
	go func() {
		defer close(tracer.stopped)
		tracer.worker(tick)
	}()
	internal.SetGlobalTracer(tracer)
	// flushFunc triggers a flush and waits until n spans are flushed or until it times out.
	// A negative value will trigger the flush without waiting for any spans.
	flushFunc = func(n int) {
		if n < 0 {
			tick <- time.Now()
			return
		}
		d := 200 * time.Millisecond
		expire := time.After(d)
	loop:
		for {
			select {
			case <-expire:
				t.Fatalf("timed out in %s waiting for %d spans", d, n)
			default:
				tick <- time.Now()
				if transport.Len() == n {
					break loop
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
	return tracer, transport, flushFunc, func() {
		internal.SetGlobalTracer(&internal.NoopTracer{})
		tracer.Stop()
	}
}

// Mock Transport with a real Encoder
type dummyTransport struct {
	sync.RWMutex
	spans spanList
}

func newDummyTransport() *dummyTransport {
	return &dummyTransport{spans: spanList{}}
}

func (t *dummyTransport) send(p *payload) (io.ReadCloser, error) {
	spans, err := decode(p)
	if err != nil {
		return nil, err
	}
	t.Lock()
	t.spans = append(t.spans, spans...)
	t.Unlock()
	ok := ioutil.NopCloser(strings.NewReader("OK"))
	return ok, nil
}

func decode(p *payload) (spanList, error) {
	var spans spanList
	err := msgp.Decode(p, &spans)
	return spans, err
}

func encode(spans []*span) (*payload, error) {
	p := newPayload()
	for _, t := range spans {
		if err := p.push(t); err != nil {
			return p, err
		}
	}
	return p, nil
}

func (t *dummyTransport) Reset() {
	t.Lock()
	t.spans = t.spans[:0]
	t.Unlock()
}

// Spans returns the spans that the transport receives and resets.
func (t *dummyTransport) Spans() spanList {
	t.Lock()
	defer t.Unlock()

	spans := t.spans
	t.spans = spanList{}
	return spans
}

func (t *dummyTransport) Len() int {
	t.RLock()
	defer t.RUnlock()
	return len(t.spans)
}

// comparePayloadSpans allows comparing two spans which might have been
// read from the msgpack payload. In that case the private fields will
// not be available and the maps (meta & metrics will be nil for lengths
// of 0). This function covers for those cases and correctly compares.
func comparePayloadSpans(t *testing.T, a, b *span) {
	assert.Equal(t, cpspan(a), cpspan(b))
}

func cpspan(s *span) *span {
	if len(s.Metrics) == 0 {
		s.Metrics = nil
	}
	if len(s.Meta) == 0 {
		s.Meta = nil
	}
	return &span{
		Name:     s.Name,
		Service:  s.Service,
		Resource: s.Resource,
		Type:     s.Type,
		Start:    s.Start,
		Duration: s.Duration,
		Meta:     s.Meta,
		Metrics:  s.Metrics,
		SpanID:   s.SpanID,
		TraceID:  s.TraceID,
		ParentID: s.ParentID,
		Error:    s.Error,
	}
}

func TestTakeStackTrace(t *testing.T) {
	t.Run("n=12", func(t *testing.T) {
		val := takeStacktrace(12, 0)
		// top frame should be runtime.main or runtime.goexit, in case of tests that's goexit
		assert.Contains(t, val, "runtime.goexit")
		assert.Contains(t, val, "testing.tRunner")
		assert.Contains(t, val, "tracer.TestTakeStackTrace")
	})

	t.Run("n=15,skip=2", func(t *testing.T) {
		val := takeStacktrace(3, 2)
		// top frame should be runtime.main or runtime.goexit, in case of tests that's goexit
		assert.Contains(t, val, "runtime.goexit")
		numFrames := strings.Count(val, "\n\t")
		assert.Equal(t, 1, numFrames)
	})

	t.Run("n=1", func(t *testing.T) {
		val := takeStacktrace(1, 0)
		assert.Contains(t, val, "tracer.TestTakeStackTrace", "should contain this function")
		// each frame consists of two strings separated by \n\t, thus number of frames == number of \n\t
		numFrames := strings.Count(val, "\n\t")
		assert.Equal(t, 1, numFrames)
	})

	t.Run("invalid", func(t *testing.T) {
		assert.Empty(t, takeStacktrace(100, 115))
	})
}

// BenchmarkTracerStackFrames tests the performance of taking stack trace.
func BenchmarkTracerStackFrames(b *testing.B) {
	tracer, _, _, stop := startTestTracer(b, WithSampler(NewRateSampler(0)))
	defer stop()

	for n := 0; n < b.N; n++ {
		span := tracer.StartSpan("test")
		span.Finish(StackFrames(64, 0))
	}
}
