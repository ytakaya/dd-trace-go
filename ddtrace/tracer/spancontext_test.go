// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.

package tracer

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
)

func TestAsyncSpanRace(t *testing.T) {
	// This tests a regression where asynchronously finishing spans would
	// modify a flushing root's sampling priority.
	_, _, _, stop := startTestTracer(t)
	defer stop()

	for i := 0; i < 100; i++ {
		// The test has 100 iterations because it is not easy to reproduce the race.
		t.Run("", func(t *testing.T) {
			root, ctx := StartSpanFromContext(context.Background(), "root", Tag(ext.SamplingPriority, ext.PriorityUserKeep))
			var wg sync.WaitGroup
			done := make(chan struct{})
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-done:
					root.Finish()
					for i := 0; i < 500; i++ {
						for range root.(*span).Metrics {
							// this range simulates iterating over the metrics map
							// as we do when encoding msgpack upon flushing.
						}
					}
					return
				}
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case <-done:
					for i := 0; i < 50; i++ {
						// to trigger the bug, the child should be created after the root was finished,
						// as its being flushed
						child, _ := StartSpanFromContext(ctx, "child", Tag(ext.SamplingPriority, ext.PriorityUserKeep))
						child.Finish()
					}
					return
				}
			}()
			// closing will attempt trigger the two goroutines at approximately the same time.
			close(done)
			wg.Wait()
		})
	}

	// Test passes if no panic occurs while running.
}

func TestNewSpanContext(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		span := &span{
			TraceID:  1,
			SpanID:   2,
			ParentID: 3,
		}
		ctx := newSpanContext(span, nil)
		assert := assert.New(t)
		assert.Equal(ctx.traceID, span.TraceID)
		assert.Equal(ctx.spanID, span.SpanID)
		assert.Nil(ctx.priority)
	})

	t.Run("priority", func(t *testing.T) {
		span := &span{
			TraceID:  1,
			SpanID:   2,
			ParentID: 3,
			Metrics:  map[string]float64{keySamplingPriority: 1},
		}
		ctx := newSpanContext(span, nil)
		assert := assert.New(t)
		assert.Equal(ctx.traceID, span.TraceID)
		assert.Equal(ctx.spanID, span.SpanID)
		assert.Equal(ctx.TraceID(), span.TraceID)
		assert.Equal(ctx.SpanID(), span.SpanID)
		assert.Equal(*ctx.priority, 1.)
	})

	t.Run("root", func(t *testing.T) {
		_, _, _, stop := startTestTracer(t)
		defer stop()
		assert := assert.New(t)
		ctx, err := NewPropagator(nil).Extract(TextMapCarrier(map[string]string{
			DefaultTraceIDHeader:  "1",
			DefaultParentIDHeader: "2",
			DefaultPriorityHeader: "3",
		}))
		assert.Nil(err)
		sctx, ok := ctx.(*spanContext)
		assert.True(ok)
		assert.EqualValues(sctx.traceID, 1)
		assert.EqualValues(sctx.spanID, 2)
		assert.EqualValues(*sctx.priority, 3)
	})
}

func TestSpanContextParent(t *testing.T) {
	s := &span{
		TraceID:  1,
		SpanID:   2,
		ParentID: 3,
	}
	for name, parentCtx := range map[string]*spanContext{
		"basic": &spanContext{
			baggage: map[string]string{"A": "A", "B": "B"},
			drop:    true,
		},
		"nil-trace": &spanContext{
			drop: true,
		},
		"priority": &spanContext{
			baggage: map[string]string{"A": "A", "B": "B"},
		},
		"origin": &spanContext{
			origin: "synthetics",
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := newSpanContext(s, parentCtx)
			assert := assert.New(t)
			assert.Equal(ctx.traceID, s.TraceID)
			assert.Equal(ctx.spanID, s.SpanID)
			assert.Equal(ctx.priority, parentCtx.priority)
			assert.Equal(ctx.drop, parentCtx.drop)
			assert.Equal(ctx.baggage, parentCtx.baggage)
			assert.Equal(ctx.origin, parentCtx.origin)
		})
	}
}

func TestSpanContextBaggage(t *testing.T) {
	assert := assert.New(t)

	var ctx spanContext
	ctx.setBaggageItem("key", "value")
	assert.Equal("value", ctx.baggage["key"])
}

func TestSpanContextIterator(t *testing.T) {
	assert := assert.New(t)

	got := make(map[string]string)
	ctx := spanContext{baggage: map[string]string{"key": "value"}}
	ctx.ForeachBaggageItem(func(k, v string) bool {
		got[k] = v
		return true
	})

	assert.Len(got, 1)
	assert.Equal("value", got["key"])
}

func TestSpanContextIteratorBreak(t *testing.T) {
	got := make(map[string]string)
	ctx := spanContext{baggage: map[string]string{"key": "value"}}
	ctx.ForeachBaggageItem(func(k, v string) bool {
		return false
	})

	assert.Len(t, got, 0)
}

// testLogger implements a mock Printer.
type testLogger struct {
	mu    sync.RWMutex
	lines []string
}

// Print implements log.Printer.
func (tp *testLogger) Log(msg string) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.lines = append(tp.lines, msg)
}

// Lines returns the lines that were printed using this printer.
func (tp *testLogger) Lines() []string {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return tp.lines
}

// Reset resets the printer's internal buffer.
func (tp *testLogger) Reset() {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.lines = tp.lines[:0]
}
