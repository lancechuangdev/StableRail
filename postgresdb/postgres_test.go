package postgresdb

import (
	"context"
	"testing"
)

func TestOpenRejectsEmptyDataSourceName(t *testing.T) {
	if _, err := Open(context.Background(), ""); err == nil {
		t.Fatal("expected empty data source name error")
	}
}
