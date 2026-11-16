package cluster

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
)

type EventWatcher struct {
	repo         *Repository
	lastSeenID   int64
	hasLastSeen  bool
	pollInterval time.Duration
	onEvent      func(context.Context, ClusterEventRecord) error
}

// NewEventWatcher creates a new event watcher.
func NewEventWatcher(repo *Repository, pollInterval time.Duration, onEvent func(context.Context, ClusterEventRecord) error) *EventWatcher {
	return &EventWatcher{
		repo:         repo,
		pollInterval: pollInterval,
		onEvent:      onEvent,
	}
}

// NewEventWatcherFrom creates a new event watcher from.
func NewEventWatcherFrom(repo *Repository, pollInterval time.Duration, lastSeenID int64, onEvent func(context.Context, ClusterEventRecord) error) *EventWatcher {
	watcher := NewEventWatcher(repo, pollInterval, onEvent)
	watcher.lastSeenID = lastSeenID
	watcher.hasLastSeen = true
	return watcher
}

// Start starts the process.
func (w *EventWatcher) Start(ctx context.Context) error {
	// Keep validation before state changes so failures leave existing data intact.
	if w == nil {
		return fmt.Errorf("cluster event watcher is nil")
	}
	if w.repo == nil {
		return fmt.Errorf("cluster event watcher repository is nil")
	}
	if w.onEvent == nil {
		return fmt.Errorf("cluster event watcher callback is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	interval := w.pollInterval
	if interval <= 0 {
		interval = time.Second
	}

	if !w.hasLastSeen {
		maxID, errMaxID := w.maxEventID(ctx)
		if errMaxID != nil {
			return errMaxID
		}
		w.lastSeenID = maxID
		w.hasLastSeen = true
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if errPoll := w.poll(ctx); errPoll != nil {
				if ctx.Err() != nil {
					return nil
				}
				if retryableDatabaseConnectionError(errPoll) {
					log.Warnf("cluster event database unavailable; retrying on next poll: %v", errPoll)
					continue
				}
				return errPoll
			}
		}
	}
}

// poll handles a poll.
func (w *EventWatcher) poll(ctx context.Context) error {
	events, errEvents := w.eventsAfter(ctx, w.lastSeenID)
	if errEvents != nil {
		return errEvents
	}
	for _, event := range events {
		if errEvent := w.onEvent(ctx, event); errEvent != nil {
			return errEvent
		}
		w.lastSeenID = int64(event.ID)
	}
	return nil
}

// maxEventID handles a max event id.
func (w *EventWatcher) maxEventID(ctx context.Context) (int64, error) {
	return w.repo.MaxEventID(ctx)
}

// eventsAfter handles an events after.
func (w *EventWatcher) eventsAfter(ctx context.Context, id int64) ([]ClusterEventRecord, error) {
	db, errDB := w.repo.database()
	if errDB != nil {
		return nil, errDB
	}
	var events []ClusterEventRecord
	if errFind := db.WithContext(contextOrBackground(ctx)).Where("id > ?", id).Order("id").Find(&events).Error; errFind != nil {
		return nil, errFind
	}
	return events, nil
}

// retryableDatabaseConnectionError recognizes connection loss and bounded database waits.
// Semantic errors, including incarnation fencing, must still stop the watcher.
func retryableDatabaseConnectionError(err error) bool {
	if errors.Is(err, ErrHomeIncarnationFenced) || errors.Is(err, ErrHomeIncarnationInactive) || errors.Is(err, ErrHomeIncarnationNotFound) {
		return false
	}
	if IsLifecycleTimeout(err) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03"
	}
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
