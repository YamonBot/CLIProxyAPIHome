package cluster

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPostgresPoolBoundsConnectionsAndReusesReleasedSlot(t *testing.T) {
	maxOpen, maxIdle := 12, 2
	for _, test := range []struct {
		name    string
		cfg     PGSQLConfig
		maxOpen int
		maxIdle int
	}{
		{name: "defaults", maxOpen: 24, maxIdle: 8},
		{name: "shared database budget", cfg: PGSQLConfig{MaxOpenConns: &maxOpen, MaxIdleConns: &maxIdle}, maxOpen: 12, maxIdle: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &postgresPoolTestDriver{}
			sqlDB := sql.OpenDB(state)
			t.Cleanup(func() { _ = sqlDB.Close() })
			if errInitialize := initializePostgresPool(t.Context(), sqlDB, test.cfg); errInitialize != nil {
				t.Fatal(errInitialize)
			}
			if got := sqlDB.Stats().MaxOpenConnections; got != test.maxOpen {
				t.Fatalf("MaxOpenConnections = %d, want %d", got, test.maxOpen)
			}

			connections := make([]*sql.Conn, test.maxOpen)
			for i := range connections {
				conn, errConn := sqlDB.Conn(t.Context())
				if errConn != nil {
					t.Fatal(errConn)
				}
				connections[i] = conn
				t.Cleanup(func() { _ = conn.Close() })
			}
			waitCtx, cancelWait := context.WithTimeout(t.Context(), 25*time.Millisecond)
			defer cancelWait()
			if conn, errConn := sqlDB.Conn(waitCtx); !errors.Is(errConn, context.DeadlineExceeded) {
				if conn != nil {
					_ = conn.Close()
				}
				t.Fatalf("additional connection error = %v, want deadline exceeded", errConn)
			}
			if got := state.opened.Load(); got != int32(test.maxOpen) {
				t.Fatalf("driver opened %d connections, want %d", got, test.maxOpen)
			}
			if stats := sqlDB.Stats(); stats.InUse != test.maxOpen || stats.WaitCount != 1 {
				t.Fatalf("saturated pool stats = %+v", stats)
			}

			if errClose := connections[0].Close(); errClose != nil {
				t.Fatal(errClose)
			}
			resumeCtx, cancelResume := context.WithTimeout(t.Context(), time.Second)
			defer cancelResume()
			conn, errConn := sqlDB.Conn(resumeCtx)
			if errConn != nil {
				t.Fatalf("released slot did not resume progress: %v", errConn)
			}
			connections[0] = conn
			t.Cleanup(func() { _ = conn.Close() })
			if got := state.opened.Load(); got != int32(test.maxOpen) {
				t.Fatalf("slot reuse opened %d connections, want %d", got, test.maxOpen)
			}
			for _, connection := range connections {
				if errClose := connection.Close(); errClose != nil {
					t.Fatal(errClose)
				}
			}
			if stats := sqlDB.Stats(); stats.Idle != test.maxIdle || stats.InUse != 0 {
				t.Fatalf("released pool stats = %+v, want %d idle and 0 in use", stats, test.maxIdle)
			}
			if errClose := sqlDB.Close(); errClose != nil {
				t.Fatal(errClose)
			}
			if stats := sqlDB.Stats(); stats.OpenConnections != 0 || state.closed.Load() != int32(test.maxOpen) {
				t.Fatalf("closed pool stats = %+v, driver closed = %d", stats, state.closed.Load())
			}
		})
	}
}

func TestPostgresPoolClosesAfterFailedPing(t *testing.T) {
	pingErr := errors.New("test ping failed")
	for _, test := range []struct {
		name     string
		closeErr error
	}{
		{name: "close success"},
		{name: "close error", closeErr: errors.New("test close failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &postgresPoolTestDriver{pingErr: pingErr, closeErr: test.closeErr}
			sqlDB := sql.OpenDB(state)
			t.Cleanup(func() { _ = sqlDB.Close() })
			errInitialize := initializePostgresPool(t.Context(), sqlDB, PGSQLConfig{})
			if !errors.Is(errInitialize, pingErr) {
				t.Fatalf("initialize error = %v, want ping error", errInitialize)
			}
			if test.closeErr != nil && !strings.Contains(errInitialize.Error(), test.closeErr.Error()) {
				t.Fatalf("initialize error = %v, want close error detail", errInitialize)
			}
			if state.opened.Load() != 1 || state.closed.Load() != 1 || sqlDB.Stats().OpenConnections != 0 {
				t.Fatalf("failed ping leaked connections: stats=%+v opened=%d closed=%d", sqlDB.Stats(), state.opened.Load(), state.closed.Load())
			}
			if errPing := sqlDB.PingContext(t.Context()); errPing == nil {
				t.Fatal("closed failed pool accepted another ping")
			}
		})
	}
}

func TestPostgresOpenHonorsCanceledStartupContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	db, errOpen := Open(ctx, PGSQLConfig{
		Host: "127.0.0.1", Port: 1, User: "pool-test", Database: "pool-test", SSLMode: "disable",
	})
	if db != nil || !errors.Is(errOpen, context.Canceled) {
		t.Fatalf("Open() = %v, %v, want nil and context canceled", db, errOpen)
	}
}

type postgresPoolTestDriver struct {
	opened   atomic.Int32
	closed   atomic.Int32
	pingErr  error
	closeErr error
}

func (d *postgresPoolTestDriver) Open(string) (driver.Conn, error) {
	d.opened.Add(1)
	return &postgresPoolTestConn{state: d}, nil
}

func (d *postgresPoolTestDriver) Connect(ctx context.Context) (driver.Conn, error) {
	if errContext := ctx.Err(); errContext != nil {
		return nil, errContext
	}
	return d.Open("")
}

func (d *postgresPoolTestDriver) Driver() driver.Driver { return d }

type postgresPoolTestConn struct{ state *postgresPoolTestDriver }

func (c *postgresPoolTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}

func (c *postgresPoolTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}

func (c *postgresPoolTestConn) Close() error {
	c.state.closed.Add(1)
	return c.state.closeErr
}

func (c *postgresPoolTestConn) Ping(context.Context) error { return c.state.pingErr }
