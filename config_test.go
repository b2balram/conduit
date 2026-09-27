package conduit

import (
	"context"
	"testing"
)

func TestNewValidation(t *testing.T) {
	_, err := New(Config{}, fakeAdapter{}, String{}, HandlerFunc[string](func(_ context.Context, _ Message[string]) error { return nil }))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
}
