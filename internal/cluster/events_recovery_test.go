package cluster

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

func TestEventWatcherRecoversDatabaseConnectionLoss(t *testing.T) {
	for _, failureAt := range []string{"query", "callback"} {
		t.Run(failureAt, func(t *testing.T) {
			db, errOpen := OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "events.db"))
			if errOpen != nil {
				t.Fatal(errOpen)
			}
			sqlDB, errDB := db.DB()
			if errDB != nil {
				t.Fatal(errDB)
			}
			t.Cleanup(func() { _ = sqlDB.Close() })
			if errMigrate := db.AutoMigrate(&ClusterEventRecord{}); errMigrate != nil {
				t.Fatal(errMigrate)
			}
			event := ClusterEventRecord{Scope: "config", Op: "update", EntityUUID: "test", Version: 1}
			if errCreate := db.Create(&event).Error; errCreate != nil {
				t.Fatal(errCreate)
			}
			connectionLost := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
			queries := 0
			if errRegister := db.Callback().Query().Before("gorm:query").Register("test:connection-loss", func(tx *gorm.DB) {
				queries++
				if failureAt == "query" && queries == 1 {
					tx.AddError(connectionLost)
				}
			}); errRegister != nil {
				t.Fatal(errRegister)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			attempts, applied := 0, false
			watcher := NewEventWatcherFrom(NewRepository(db), time.Millisecond, 0, func(_ context.Context, got ClusterEventRecord) error {
				attempts++
				if got.ID != event.ID {
					return fmt.Errorf("event changed: %d", got.ID)
				}
				if failureAt == "callback" && attempts == 1 {
					return connectionLost
				}
				applied = true
				cancel()
				return nil
			})
			if errStart := watcher.Start(ctx); errStart != nil {
				t.Fatalf("watcher exited during recoverable outage: %v", errStart)
			}
			if !applied || watcher.lastSeenID != int64(event.ID) {
				t.Fatalf("event not replayed after recovery: applied=%v cursor=%d", applied, watcher.lastSeenID)
			}
			if failureAt == "callback" && attempts != 2 {
				t.Fatalf("callback attempts=%d, want 2", attempts)
			}
		})
	}
}

func TestRetryableEventDatabaseErrorPreservesFatalErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		{"bad connection", driver.ErrBadConn, true},
		{"closed connection", io.EOF, true},
		{"truncated connection", io.ErrUnexpectedEOF, true},
		{"connection sqlstate", &pgconn.PgError{Code: "08006"}, true},
		{"shutdown", &pgconn.PgError{Code: "57P01"}, true},
		{"recovery", &pgconn.PgError{Code: "57P03"}, true},
		{"duplicate key", &pgconn.PgError{Code: "23505"}, false},
		{"fenced", ErrHomeIncarnationFenced, false},
		{"inactive", ErrHomeIncarnationInactive, false},
		{"missing", ErrHomeIncarnationNotFound, false},
		{"fence with connection error", errors.Join(ErrHomeIncarnationFenced, io.EOF), false},
		{"invalid event", errors.New("invalid CPA fence event"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryableDatabaseConnectionError(fmt.Errorf("wrapped: %w", tc.err)); got != tc.retry {
				t.Fatalf("retry=%v, want %v", got, tc.retry)
			}
		})
	}
}
