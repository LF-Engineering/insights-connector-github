package main

// NoopSpanContext is an implementation of ddtrace.SpanContext that is a no-op.
type NoopSpanContext struct {
	spanID  uint64
	traceID uint64
}

func NewSpanContext(traceID uint64, spanID uint64) NoopSpanContext {
	return NoopSpanContext{traceID: traceID, spanID: spanID}
}

// SpanID implements ddtrace.SpanContext.
func (n NoopSpanContext) SpanID() uint64 { return n.spanID }

// TraceID implements ddtrace.SpanContext.
func (n NoopSpanContext) TraceID() uint64 { return n.traceID }

// ForeachBaggageItem implements ddtrace.SpanContext.
func (n NoopSpanContext) ForeachBaggageItem(handler func(k, v string) bool) {}
