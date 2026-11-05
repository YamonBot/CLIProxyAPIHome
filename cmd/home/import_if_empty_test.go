package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPIHome/internal/cluster"
)

func TestRunImportIfEmptyInitializesAndPreserves(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "home.db")
	configPath := filepath.Join(dir, "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("port: 8317\napi-keys: [existing-client]\n"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	oldArgs, oldCommandLine := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldCommandLine })
	runImport := func(seed string) int {
		os.Args = []string{"home", "-import", "-if-empty", "-sqlite-path", dbPath, "-config", seed, "-auth-dir", dir}
		flag.CommandLine = flag.NewFlagSet("home", flag.ContinueOnError)
		return run()
	}
	if exit := runImport(configPath); exit != 0 {
		t.Fatalf("initial import exit = %d", exit)
	}
	db, errOpen := cluster.OpenSQLite(context.Background(), dbPath)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	sqlDB, errDB := db.DB()
	if errDB != nil {
		t.Fatal(errDB)
	}
	t.Cleanup(func() {
		if errClose := sqlDB.Close(); errClose != nil {
			t.Errorf("close sqlite: %v", errClose)
		}
	})
	repo := cluster.NewRepository(db)
	before, errBefore := repo.LoadConfigSnapshot(context.Background())
	if errBefore != nil || len(before) == 0 {
		t.Fatalf("initial config = %v, %v", before, errBefore)
	}
	var lifecycleBefore cluster.LifecycleConfigRecord
	if errFind := db.First(&lifecycleBefore).Error; errFind != nil {
		t.Fatal(errFind)
	}
	// A skipped seed must not attempt to replace the existing lifecycle policy.
	clusterConfig := "sqlite:\n  path: home.db\nnode:\n  external-ip: 127.0.0.1\n  port: 8327\n  heartbeat-timeout: " + (cluster.DefaultHeartbeatTimeout() * 2).String() + "\n"
	if errWrite := os.WriteFile("cluster.yaml", []byte(clusterConfig), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if exit := runImport(filepath.Join(dir, "missing.yaml")); exit != 0 {
		t.Fatalf("initialized import exit = %d", exit)
	}
	after, errAfter := repo.LoadConfigSnapshot(context.Background())
	if errAfter != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("initialized config changed")
	}
	var lifecycleAfter cluster.LifecycleConfigRecord
	if errFind := db.First(&lifecycleAfter).Error; errFind != nil || !reflect.DeepEqual(lifecycleBefore, lifecycleAfter) {
		t.Fatalf("initialized lifecycle changed: %v", errFind)
	}
}

func TestRunIfEmptyRequiresImport(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	oldArgs, oldCommandLine := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldCommandLine })
	os.Args = []string{"home", "-if-empty"}
	flag.CommandLine = flag.NewFlagSet("home", flag.ContinueOnError)
	if exit := run(); exit != 1 {
		t.Fatalf("invalid mode exit = %d", exit)
	}
	if _, errStat := os.Stat("home.db"); !os.IsNotExist(errStat) {
		t.Fatalf("invalid mode opened a database: %v", errStat)
	}
}

func TestRunImportIfEmptyPreservesLegacyConfigBeforeMigration(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	dbPath := filepath.Join(dir, "legacy.db")
	db, errOpen := cluster.OpenSQLite(context.Background(), dbPath)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	sqlDB, errDB := db.DB()
	if errDB != nil {
		t.Fatal(errDB)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if errMigrate := cluster.AutoMigrateContext(context.Background(), db); errMigrate != nil {
		t.Fatal(errMigrate)
	}
	legacy := cluster.ConfigRecord{Key: "api-keys", Value: cluster.JSONB(`["legacy-client"]`), Version: 1}
	if errCreate := db.Create(&legacy).Error; errCreate != nil {
		t.Fatal(errCreate)
	}
	var before cluster.ConfigRecord
	if errRead := db.First(&before, "key = ?", legacy.Key).Error; errRead != nil {
		t.Fatal(errRead)
	}
	oldArgs, oldCommandLine := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldCommandLine })
	os.Args = []string{"home", "-import", "-if-empty", "-sqlite-path", dbPath, "-config", filepath.Join(dir, "missing.yaml")}
	flag.CommandLine = flag.NewFlagSet("home", flag.ContinueOnError)
	if exit := run(); exit != 0 {
		t.Fatalf("legacy initialized import exit = %d", exit)
	}
	var after cluster.ConfigRecord
	if errRead := db.First(&after, "key = ?", legacy.Key).Error; errRead != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("legacy config changed: %v", errRead)
	}
	var keyCount int64
	if errCount := db.Model(&cluster.APIKeyRecord{}).Count(&keyCount).Error; errCount != nil || keyCount != 0 {
		t.Fatalf("skipped import migrated legacy keys: count=%d err=%v", keyCount, errCount)
	}
}
