// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.

package tracer

import (
	"sync"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
)

var _ ddtrace.SpanContext = (*spanContext)(nil)

// SpanContext represents a span state that can propagate to descendant spans
// and across process boundaries. It contains all the information needed to
// spawn a direct descendant of the span that it belongs to. It can be used
// to create distributed tracing by propagating it using the provided interfaces.
type spanContext struct {
	// the below group should propagate only locally

	span *span // reference to the span that hosts this context
	drop bool  // when true, the span will not be sent to the agent

	// the below group should propagate both locally and cross-process

	traceID uint64
	spanID  uint64

	mu       sync.RWMutex // guards below fields
	baggage  map[string]string
	origin   string // e.g. "synthetics"
	priority *float64
}

// newSpanContext creates a new SpanContext to serve as context for the given
// span. If the provided parent is not nil, the context will inherit the trace,
// baggage and other values from it. This method also pushes the span into the
// new context's trace and as a result, it should not be called multiple times
// for the same span.
func newSpanContext(span *span, parent *spanContext) *spanContext {
	context := &spanContext{
		traceID: span.TraceID,
		spanID:  span.SpanID,
		span:    span,
	}
	if v, ok := span.Metrics[keySamplingPriority]; ok {
		context.setSamplingPriority(int(v))
	}
	if parent != nil {
		context.drop = parent.drop
		context.origin = parent.origin
		context.priority = parent.priority
		parent.ForeachBaggageItem(func(k, v string) bool {
			context.setBaggageItem(k, v)
			return true
		})
	}
	return context
}

// SpanID implements ddtrace.SpanContext.
func (c *spanContext) SpanID() uint64 { return c.spanID }

// TraceID implements ddtrace.SpanContext.
func (c *spanContext) TraceID() uint64 { return c.traceID }

// ForeachBaggageItem implements ddtrace.SpanContext.
func (c *spanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for k, v := range c.baggage {
		if !handler(k, v) {
			break
		}
	}
}

func (c *spanContext) setSamplingPriority(p int) {
	if c.priority == nil {
		c.priority = new(float64)
	}
	*c.priority = float64(p)
}

func (c *spanContext) samplingPriority() int {
	if c.priority == nil {
		return 0
	}
	return int(*c.priority)
}

func (c *spanContext) hasSamplingPriority() bool {
	return c.priority != nil
}

func (c *spanContext) setBaggageItem(key, val string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.baggage == nil {
		c.baggage = make(map[string]string, 1)
	}
	c.baggage[key] = val
}

func (c *spanContext) baggageItem(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.baggage[key]
}
