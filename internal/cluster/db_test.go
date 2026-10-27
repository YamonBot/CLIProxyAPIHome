package cluster

import "testing"


func TestPostgresOpenBoundsConnectionPool(t *testing.T) {
	// Guards the 53300 fix: the sql pool must never be left at the
	// database/sql unlimited defaults.
	if defaultPostgresMaxOpenConns <= 0 || defaultPostgresMaxIdleConns <= 0 {
		t.Fatalf("pool bounds must be positive: open=%d idle=%d", defaultPostgresMaxOpenConns, defaultPostgresMaxIdleConns)
	}
	if defaultPostgresMaxIdleConns > defaultPostgresMaxOpenConns {
		t.Fatalf("idle (%d) must not exceed open (%d)", defaultPostgresMaxIdleConns, defaultPostgresMaxOpenConns)
	}
	if defaultPostgresConnMaxIdleTime >= defaultPostgresConnMaxLifetime {
		t.Fatalf("idle time (%s) must be shorter than lifetime (%s)", defaultPostgresConnMaxIdleTime, defaultPostgresConnMaxLifetime)
	}
}
