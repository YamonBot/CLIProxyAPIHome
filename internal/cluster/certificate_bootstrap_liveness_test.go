package cluster

import (
	"testing"
	"time"
)

func TestCurrentMasterNodeBootstrapLiveness(t *testing.T) {
	for _, test := range []struct {
		name    string
		timeout time.Duration
		age     time.Duration
		master  bool
		want    bool
	}{
		{name: "expired persisted master", timeout: time.Minute, age: 2 * time.Minute, master: true},
		{name: "healthy master", timeout: time.Minute, master: true, want: true},
		{name: "configured longer timeout", timeout: 2 * time.Minute, age: time.Minute, master: true, want: true},
		{name: "configured shorter timeout", timeout: 5 * time.Second, age: 10 * time.Second, master: true},
		{name: "healthy follower", timeout: time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := newCredentialFoundationTestRepository(t)
			ctx := t.Context()
			if _, errEnsure := repo.EnsureLifecycleConfig(ctx, test.timeout); errEnsure != nil {
				t.Fatal(errEnsure)
			}
			now, errNow := DatabaseNow(ctx, repo.db)
			if errNow != nil {
				t.Fatal(errNow)
			}
			record := ClusterNodeRecord{IP: "10.52.0.100", Port: 8327, IsMaster: test.master, StartedAt: now.Add(-time.Hour), LastSeenAt: now.Add(-test.age)}
			if errCreate := repo.db.Create(&record).Error; errCreate != nil {
				t.Fatal(errCreate)
			}
			master, errMaster := repo.CurrentMasterNode(ctx)
			if errMaster != nil {
				t.Fatal(errMaster)
			}
			if (master != nil) != test.want {
				t.Fatalf("CurrentMasterNode() = %#v, want master present = %t", master, test.want)
			}
			if master != nil && (master.IP != record.IP || master.Port != record.Port) {
				t.Fatalf("CurrentMasterNode() = %#v, want recorded healthy master", master)
			}
		})
	}
}
