package cluster

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

// A blocked reconnect must not hold an exclusive singleton lock over other CPAs.
func TestPostgresBlockedReconnectAllowsIndependentMembership(t *testing.T) {
	repo, ctx, lifetime := newPostgresLifecycleFixture(t)
	locker := repo.db.Begin()
	if locker.Error != nil {
		t.Fatal(locker.Error)
	}
	defer func() { _ = locker.Rollback().Error }()
	if errLock := locker.Exec("SELECT certificate_fingerprint FROM cpa_node_membership WHERE certificate_fingerprint = ? FOR UPDATE", lifetime.Fingerprint).Error; errLock != nil {
		t.Fatal(errLock)
	}
	type blockedReconnectKey struct{}
	reachedMembership := make(chan struct{}, 1)
	if errRegister := repo.db.Callback().Query().Before("gorm:query").Register("test:blocked-reconnect", func(tx *gorm.DB) {
		if tx.Statement.Table == "cpa_node_membership" && tx.Statement.Context.Value(blockedReconnectKey{}) != nil {
			reachedMembership <- struct{}{}
		}
	}); errRegister != nil {
		t.Fatal(errRegister)
	}
	cfg, errConfig := repo.EnsureLifecycleConfig(ctx, DefaultHeartbeatTimeout())
	if errConfig != nil {
		t.Fatal(errConfig)
	}
	done := make(chan error, 1)
	go func() {
		_, errSubscribe := repo.SubscribeMembership(context.WithValue(ctx, blockedReconnectKey{}, true), SubscribeMembershipRequest{
			Fingerprint: lifetime.Fingerprint, NodeID: "node-a", Home: lifetime.Home,
			ProtocolVersion: 1, LifecycleConfigRevision: cfg.Revision, Takeover: true,
		})
		done <- errSubscribe
	}()
	defer func() {
		_ = locker.Rollback().Error
		<-done
	}()
	select {
	case <-reachedMembership:
	case <-ctx.Done():
		t.Fatal("reconnect did not reach membership lock", ctx.Err())
	}
	independentCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if _, errSubscribe := repo.SubscribeMembership(independentCtx, SubscribeMembershipRequest{
		Fingerprint: "independent", NodeID: "node-b", Home: lifetime.Home,
		ProtocolVersion: 1, LifecycleConfigRevision: cfg.Revision,
	}); errSubscribe != nil {
		t.Fatal("unrelated CPA was blocked behind reconnect", errSubscribe)
	}
}
