package cluster

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestStartupPostgresOpenHonorsCallerDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	received := make(chan net.Conn)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			received <- conn
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := Open(ctx, PGSQLConfig{
			PostgresURI: "postgresql://fixture@" + listener.Addr().String() + "/fixture?sslmode=disable",
		})
		result <- err
	}()

	select {
	case conn := <-received:
		defer conn.Close()
	case err := <-result:
		t.Fatalf("fixture never received a database connection: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the local fixture")
	}

	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected caller deadline error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("PostgreSQL startup handshake remained blocked past the caller deadline")
	}
}
