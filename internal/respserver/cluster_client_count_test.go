package respserver

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/router-for-me/CLIProxyAPIHome/internal/cluster"
)

func TestBlockedClientCountDoesNotExpireSubscriptionLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := NewFingerprintRegistry()
		lifetime := cluster.ConnectionLifetime{Fingerprint: "count-delay", ConnectedAt: time.Unix(90, 0), HeartbeatTimeout: 3 * time.Second}
		defer func() {
			if err := registry.Fence(context.Background(), lifetime, 0); err != nil {
				t.Error(err)
			}
		}()
		liveness, err := registry.watchSubscriptionLiveness(lifetime)
		if err != nil {
			t.Fatal(err)
		}
		server := &Server{cluster: &initializingLivenessHandler{updateCount: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}}}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		started := time.Now()
		server.syncClusterClientCount(ctx)
		if elapsed := time.Since(started); elapsed >= time.Second {
			t.Errorf("nonessential client count blocked heartbeat initialization for %v", elapsed)
		}
		if liveness.remaining() <= 0 {
			t.Fatal("statistics delay expired a live subscription")
		}
	})
}
