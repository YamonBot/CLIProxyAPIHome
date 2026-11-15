package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSessionAffinityRollingUpgradePreservesProviderBinding(t *testing.T) {
	for _, scenario := range []string{"live", "expired", "no-longer-authorized", "canonical-wins", "new-child"} {
		t.Run(scenario, func(t *testing.T) {
			store := NewSessionCache(time.Hour)
			t.Cleanup(store.Stop)
			previousKey := sessionBindingKey("codex", "existing-session", "gpt")
			canonicalKey := sessionBindingKey("mixed", "existing-session", "gpt")
			ttl := time.Hour
			if scenario == "expired" {
				ttl = -time.Hour
			}
			if ok, err := store.CompareAndSwapSessionBinding(t.Context(), SessionAffinityCommit{Key: previousKey, AuthID: "b", TTL: ttl}); err != nil || !ok {
				t.Fatalf("seed binding: ok=%v err=%v", ok, err)
			}
			if scenario == "canonical-wins" {
				if ok, err := store.CompareAndSwapSessionBinding(t.Context(), SessionAffinityCommit{Key: canonicalKey, AuthID: "a", TTL: time.Hour}); err != nil || !ok {
					t.Fatalf("seed canonical binding: ok=%v err=%v", ok, err)
				}
			}
			previous, _ := store.LoadSessionBinding(t.Context(), previousKey)
			auths := []*Auth{{ID: "a", Status: StatusActive}, {ID: "b", Status: StatusActive}}
			if scenario == "no-longer-authorized" {
				auths = auths[:1]
			}
			selection := &SessionAffinitySelection{}
			opts := Options{Headers: http.Header{"X-Session-ID": {"existing-session"}}, SessionAffinityScope: "mixed", SessionAffinity: selection}
			if scenario == "new-child" {
				opts.Headers.Set("X-Session-ID", "new-child")
				opts.Headers.Set("X-Parent-Session-ID", "existing-session")
			}
			selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Store: store, Fallback: &FillFirstSelector{}, SubagentAffinity: true})
			picked, err := selector.Pick(t.Context(), "codex", "gpt", opts, auths)
			want, wantKey := "a", canonicalKey
			if scenario == "live" {
				want, wantKey = "b", previousKey
			} else if scenario == "new-child" {
				want, wantKey = "b", sessionBindingKey("mixed", "new-child", "gpt")
			} else if scenario == "no-longer-authorized" {
				wantKey = previousKey
			}
			if err != nil || picked == nil || picked.ID != want || selection.Commit == nil || selection.Commit.Key != wantKey {
				t.Fatalf("pick=%v commit=%+v err=%v; want auth=%s key=%s", picked, selection.Commit, err, want, wantKey)
			}
			if ok, err := store.CompareAndSwapSessionBinding(t.Context(), *selection.Commit); err != nil || !ok {
				t.Fatalf("native CAS: ok=%v err=%v", ok, err)
			}
			if scenario == "live" {
				after, _ := store.LoadSessionBinding(t.Context(), previousKey)
				if after.AuthID != previous.AuthID || after.Revision != previous.Revision {
					t.Fatalf("rolling upgrade changed bound credential: before=%+v after=%+v", previous, after)
				}
			}
		})
	}
}

type failedPriorBindingStore struct {
	SessionAffinityStore
	key string
}

func (s failedPriorBindingStore) LoadSessionBinding(ctx context.Context, key string) (SessionBinding, error) {
	if key == s.key {
		return SessionBinding{}, errors.New("database unavailable")
	}
	return s.SessionAffinityStore.LoadSessionBinding(ctx, key)
}

func TestSessionAffinityUpgradeDatabaseFailureDoesNotSelectAnotherAccount(t *testing.T) {
	cache := NewSessionCache(time.Hour)
	t.Cleanup(cache.Stop)
	store := failedPriorBindingStore{SessionAffinityStore: cache, key: sessionBindingKey("codex", "existing-session", "gpt")}
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Store: store})
	opts := Options{Headers: http.Header{"X-Session-ID": {"existing-session"}}, SessionAffinityScope: "mixed"}
	picked, err := selector.Pick(t.Context(), "codex", "gpt", opts, []*Auth{{ID: "a", Status: StatusActive}})
	if picked != nil || err == nil || !strings.Contains(err.Error(), "load prior session affinity binding") {
		t.Fatalf("database outage must preserve affinity authority: pick=%v err=%v", picked, err)
	}
}
