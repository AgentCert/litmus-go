package telemetry

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func TestGetTraceParentContext_NoEnv(t *testing.T) {
	os.Unsetenv(TraceParent)
	ctx := GetTraceParentContext()
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	// with no env set, it should be a plain background context (no active span)
	if ctx != context.Background() {
		t.Errorf("expected background context when %s unset", TraceParent)
	}
}

func TestGetTraceParentContext_WithEnv(t *testing.T) {
	carrier, _ := json.Marshal(map[string]string{"foo": "bar"})
	os.Setenv(TraceParent, string(carrier))
	defer os.Unsetenv(TraceParent)
	ctx := GetTraceParentContext()
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
}

func TestGetMarshalledSpanFromContext_Empty(t *testing.T) {
	// background context carries no span -> empty propagation carrier -> ""
	got := GetMarshalledSpanFromContext(context.Background())
	if got != "" {
		t.Errorf("expected empty string for span-less context, got %q", got)
	}
}

func TestTracingConstants(t *testing.T) {
	if TracerName == "" || TraceParent == "" {
		t.Error("tracing constants must be non-empty")
	}
}
