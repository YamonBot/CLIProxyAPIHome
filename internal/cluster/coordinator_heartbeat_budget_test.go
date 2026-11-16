package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

// A delayed election must not consume the next incarnation heartbeat's lifetime.
func TestCoordinatorRetriesElectionDeadlineWithoutLosingIncarnation(t *testing.T) {
	repo := newCredentialFoundationTestRepository(t)
	coordinator := newSnapshotTestCoordinator(t, repo)
	coordinator.heartbeatInterval = 20 * time.Millisecond
	original, _ := coordinator.HomeIncarnation()
	blocked, recovered := make(chan struct{}), make(chan struct{})
	var electionCalls atomic.Int64
	if errRegister := repo.db.Callback().Create().Before("gorm:create").Register("test:block-first-election", func(tx *gorm.DB) {
		if tx.Statement.Table != (ClusterNodeRecord{}).TableName() {
			return
		}
		switch electionCalls.Add(1) {
		case 1:
			close(blocked)
			<-tx.Statement.Context.Done()
			tx.AddError(tx.Statement.Context.Err())
		case 2:
			close(recovered)
		}
	}); errRegister != nil {
		t.Fatal(errRegister)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- coordinator.Start(ctx) }()
	defer func() {
		cancel()
		select {
		case errStart := <-done:
			if errStart != nil {
				t.Error(errStart)
			}
		case <-time.After(5 * time.Second):
			t.Error("coordinator did not stop")
		}
	}()
	waitSnapshotSignal(t, blocked)
	select {
	case <-recovered:
	case errStart := <-done:
		t.Fatalf("coordinator exited instead of retrying election: %v", errStart)
	case <-time.After(2 * time.Second):
		t.Fatal("election prevented the next incarnation heartbeat")
	}
	var record HomeProcessIncarnationRecord
	if errFind := repo.db.First(&record, "home_ip = ? AND home_port = ? AND started_at = ?", original.IP, original.Port, original.StartedAt).Error; errFind != nil {
		t.Fatal(errFind)
	}
	if record.State != HomeIncarnationActive {
		t.Fatalf("incarnation state = %s", record.State)
	}
	current, _ := coordinator.HomeIncarnation()
	if current != original {
		t.Fatal("incarnation changed during transient election failure")
	}
}

func TestCoordinatorHeartbeatBudgetPreservesFencing(t *testing.T) {
	repo := newCredentialFoundationTestRepository(t)
	coordinator := newSnapshotTestCoordinator(t, repo)
	incarnation, _ := coordinator.HomeIncarnation()
	if errFence := repo.FenceHomeIncarnation(context.Background(), incarnation, "test operator fence"); errFence != nil {
		t.Fatal(errFence)
	}
	if errHeartbeat := coordinator.refreshHeartbeat(context.Background()); !errors.Is(errHeartbeat, ErrHomeIncarnationFenced) {
		t.Fatalf("fenced heartbeat = %v", errHeartbeat)
	}
}

func TestPostgresCoordinatorElectionLockIsBoundedAndRecovers(t *testing.T) {
	repo := newPostgresQuiescenceRepository(t)
	coordinator := newSnapshotTestCoordinator(t, repo)
	coordinator.heartbeatInterval = 2 * time.Second
	original, _ := coordinator.HomeIncarnation()
	locker := repo.db.Begin()
	if locker.Error != nil {
		t.Fatal(locker.Error)
	}
	defer func() { _ = locker.Rollback().Error }()
	if errLock := locker.Exec("SELECT id FROM cluster_master_gate WHERE id = 1 FOR UPDATE").Error; errLock != nil {
		t.Fatal(errLock)
	}
	if errHeartbeat := coordinator.refreshHeartbeat(t.Context()); !errors.Is(errHeartbeat, context.DeadlineExceeded) {
		t.Fatalf("locked election = %v, want bounded deadline", errHeartbeat)
	}
	if errRollback := locker.Rollback().Error; errRollback != nil {
		t.Fatal(errRollback)
	}
	if errHeartbeat := coordinator.refreshHeartbeat(t.Context()); errHeartbeat != nil {
		t.Fatalf("heartbeat did not recover after database lock release: %v", errHeartbeat)
	}
	current, _ := coordinator.HomeIncarnation()
	if current != original {
		t.Fatal("recovery replaced the original incarnation")
	}
	var record HomeProcessIncarnationRecord
	if errFind := repo.db.First(&record, "home_ip = ? AND home_port = ? AND started_at = ?", original.IP, original.Port, original.StartedAt).Error; errFind != nil {
		t.Fatal(errFind)
	}
	if record.State != HomeIncarnationActive {
		t.Fatalf("recovered incarnation state = %s", record.State)
	}
}
