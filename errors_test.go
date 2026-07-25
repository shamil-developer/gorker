package gorker

import (
	"strings"
	"testing"
)

func TestPanicError(t *testing.T) {
	err := newPanicError("boom")

	if err.value != "boom" {
		t.Fatalf("Value = %v, want boom", err.value)
	}
	if len(err.stack) == 0 {
		t.Fatal("Stack is empty")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Error() = %q, want panic value", err.Error())
	}
}
