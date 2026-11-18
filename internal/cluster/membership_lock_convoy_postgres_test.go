package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Cancellation must release its membership lock before waiting for an unrelated
// event publisher, then revalidate the complete lifetime before committing.
func TestPostgresMembershipCancellationEventWaitAllowsLiveness(t *testing.T) {
	for _, action := range []string{"fingerprint", "lifetime", "cleanup", "takeover", "canceled"} {
		t.Run(action, func(t *testing.T) {
			repo, ctx, lifetime := newPostgresLifecycleFixture(t)
			if action == "cleanup" {
				if errStale := repo.db.Model(&CPANodeMembershipRecord{}).Where("certificate_fingerprint = ?", lifetime.Fingerprint).
					Update("last_seen_at", time.Unix(0, 0).UTC()).Error; errStale != nil {
					t.Fatal(errStale)
				}
			}
			holder := repo.db.WithContext(ctx).Begin()
			if holder.Error != nil {
				t.Fatal(holder.Error)
			}
			t.Cleanup(func() { _ = holder.Rollback().Error })
			if errLock := lockClusterEventTransaction(holder); errLock != nil {
				t.Fatal(errLock)
			}
			holderPID := make(chan postgresBackendPIDResult, 1)
			if errPID := postgresContenderBackendPIDHook(holderPID)(holder); errPID != nil {
				t.Fatal(errPID)
			}
			contenderPID := make(chan postgresBackendPIDResult, 1)
			cancelCtx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() {
				done <- repo.db.WithContext(cancelCtx).Connection(func(conn *gorm.DB) error {
					if errPID := postgresContenderBackendPIDHook(contenderPID)(conn); errPID != nil {
						return errPID
					}
					contender := NewRepository(conn)
					switch action {
					case "fingerprint":
						_, errBegin := contender.BeginFingerprintCancellation(cancelCtx, lifetime.Fingerprint)
						return errBegin
					case "cleanup":
						return contender.cleanupStaleMembership(cancelCtx, lifetime.Fingerprint, DefaultHeartbeatTimeout())
					default:
						_, errBegin := contender.BeginFingerprintCancellationForLifetime(cancelCtx, lifetime)
						return errBegin
					}
				})
			}()
			finished := false
			t.Cleanup(func() {
				cancel()
				if !finished {
					<-done
				}
			})
			waitForPostgresContendersBlockedByHolder(t, ctx, repo, holderPID, contenderPID)
			select {
			case errBegin := <-done:
				finished = true
				t.Fatalf("cancellation acknowledged before event commit: %v", errBegin)
			default:
			}
			assertUnpublishedMembershipCancellation(t, repo, lifetime)
			heartbeatCtx, cancelHeartbeat := context.WithTimeout(ctx, 250*time.Millisecond)
			errHeartbeat := repo.RefreshCPALiveness(heartbeatCtx, lifetime)
			cancelHeartbeat()
			if errHeartbeat != nil {
				t.Fatalf("event publication wait retained membership lock: %v", errHeartbeat)
			}
			if action == "takeover" {
				cfg, errConfig := repo.LifecycleConfig(ctx)
				if errConfig != nil {
					t.Fatal(errConfig)
				}
				if _, errSubscribe := repo.SubscribeMembership(ctx, SubscribeMembershipRequest{
					Fingerprint: lifetime.Fingerprint, NodeID: "node-a", Home: lifetime.Home,
					ProtocolVersion: 1, LifecycleConfigRevision: cfg.LifecycleConfigRevision, Takeover: true,
				}); errSubscribe != nil {
					t.Fatal(errSubscribe)
				}
			}
			if action == "canceled" {
				cancel()
			} else if errRollback := holder.Rollback().Error; errRollback != nil {
				t.Fatal(errRollback)
			}
			var errBegin error
			select {
			case errBegin = <-done:
				finished = true
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			switch action {
			case "canceled":
				if !errors.Is(errBegin, context.Canceled) {
					t.Fatalf("cancellation wait error = %v, want context cancellation", errBegin)
				}
				assertUnpublishedMembershipCancellation(t, repo, lifetime)
			case "takeover":
				if !errors.Is(errBegin, ErrMembershipNotActive) {
					t.Fatalf("stale lifetime retry error = %v, want fenced lifetime", errBegin)
				}
				assertUnpublishedMembershipCancellation(t, repo, lifetime)
			case "cleanup":
				if errBegin != nil {
					t.Fatal(errBegin)
				}
				// The heartbeat committed during the wait, so retry must skip expiry.
				assertUnpublishedMembershipCancellation(t, repo, lifetime)
			default:
				if errBegin != nil {
					t.Fatal(errBegin)
				}
				var member CPANodeMembershipRecord
				if errFind := repo.db.First(&member, "certificate_fingerprint = ?", lifetime.Fingerprint).Error; errFind != nil {
					t.Fatal(errFind)
				}
				var events []ClusterEventRecord
				if errFind := repo.db.Where("scope = ?", "cpa-fence").Find(&events).Error; errFind != nil {
					t.Fatal(errFind)
				}
				if member.State != MembershipStateCanceling || member.CancelRevision != 1 || len(events) != 1 || events[0].Version != member.CancelRevision || events[0].Op != lifetime.ConnectedAt.UTC().Format(time.RFC3339Nano) {
					t.Fatalf("membership and exact fence event did not commit atomically: member=%+v events=%+v", member, events)
				}
			}
		})
	}
}

func assertUnpublishedMembershipCancellation(t *testing.T, repo *Repository, lifetime ConnectionLifetime) {
	t.Helper()
	var member CPANodeMembershipRecord
	if errFind := repo.db.First(&member, "certificate_fingerprint = ?", lifetime.Fingerprint).Error; errFind != nil {
		t.Fatal(errFind)
	}
	var events, rows int64
	if errCount := repo.db.Model(&ClusterEventRecord{}).Where("scope = ?", "cpa-fence").Count(&events).Error; errCount != nil {
		t.Fatal(errCount)
	}
	if errCount := repo.db.Model(&CPANodeQuiescenceRecord{}).Where("certificate_fingerprint = ?", lifetime.Fingerprint).Count(&rows).Error; errCount != nil {
		t.Fatal(errCount)
	}
	if member.State != MembershipStateActive || member.CancelRevision != 0 || events != 0 || rows != 0 {
		t.Fatalf("uncommitted cancellation leaked state: member=%+v events=%d quiescence=%d", member, events, rows)
	}
}
