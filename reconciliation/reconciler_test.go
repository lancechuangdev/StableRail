package reconciliation

import (
	"database/sql"
	"testing"
	"time"
)

func TestNewValidatesConfiguration(t *testing.T) {
	if _, err := New(nil, Config{}, nil); err == nil {
		t.Fatal("expected nil database error")
	}
	if _, err := New(&sql.DB{}, Config{Interval: -time.Second}, nil); err == nil {
		t.Fatal("expected negative interval error")
	}
	r, err := New(&sql.DB{}, Config{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.interval != time.Minute {
		t.Fatalf("default interval = %s", r.interval)
	}
}
