package cluster

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestImportLocalStateOnlyIfEmptyPreservesInitializedDatabase(t *testing.T) {
	testImportOnlyIfEmptyPreserves(t, openImportTestSQLite(t))
}

func TestImportLocalStateOnlyIfEmptyPreservesInitializedDatabasePostgres(t *testing.T) {
	repo := newPostgresQuiescenceRepository(t)
	db, errDB := repo.database()
	if errDB != nil {
		t.Fatal(errDB)
	}
	testImportOnlyIfEmptyPreserves(t, db)
}

func testImportOnlyIfEmptyPreserves(t *testing.T, db *gorm.DB) {
	t.Helper()
	repo := NewRepository(db)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	writeFile(t, configPath, "port: 8317\napi-keys: [existing-client]\ngemini-api-key:\n  - api-key: existing-provider\n")
	opts := ImportOptions{ConfigPath: configPath, AuthDir: dir, Repository: repo, OnlyIfEmpty: true}
	stats, errImport := ImportLocalState(context.Background(), opts)
	if errImport != nil || stats.Created == 0 {
		t.Fatalf("empty database import = %+v, %v", stats, errImport)
	}
	before := importDatabaseRows(t, db)
	// An initialized database must not depend on or read a replacement seed.
	opts.ConfigPath = filepath.Join(dir, "missing-seed.yaml")
	stats, errImport = ImportLocalState(context.Background(), opts)
	if errImport != nil || stats != (ImportStats{}) {
		t.Fatalf("initialized database import = %+v, %v", stats, errImport)
	}
	if after := importDatabaseRows(t, db); !reflect.DeepEqual(before, after) {
		t.Fatal("initialized database rows changed")
	}
}

func TestImportLocalStateOnlyIfEmptyRechecksAfterSourcePreparation(t *testing.T) {
	testImportOnlyIfEmptyRechecks(t, openImportTestSQLite(t))
}

func TestImportLocalStateOnlyIfEmptyRechecksAfterSourcePreparationPostgres(t *testing.T) {
	repo := newPostgresQuiescenceRepository(t)
	db, errDB := repo.database()
	if errDB != nil {
		t.Fatal(errDB)
	}
	testImportOnlyIfEmptyRechecks(t, db)
}

func testImportOnlyIfEmptyRechecks(t *testing.T, db *gorm.DB) {
	t.Helper()
	repo := NewRepository(db)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	writeFile(t, configPath, "port: 8317\napi-keys: [replacement-client]\n")
	inserted := false
	// Deterministically model another writer committing after the initial read.
	errRegister := db.Callback().Query().After("gorm:query").Register("test:initialize-config", func(tx *gorm.DB) {
		if tx.Statement.Table != "config" || inserted {
			return
		}
		inserted = true
		if errCreate := db.Create(&ConfigRecord{Key: "port", Value: JSONB("9000")}).Error; errCreate != nil {
			t.Errorf("initialize config: %v", errCreate)
		}
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() {
		if errRemove := db.Callback().Query().Remove("test:initialize-config"); errRemove != nil {
			t.Errorf("remove callback: %v", errRemove)
		}
	})
	stats, errImport := ImportLocalState(context.Background(), ImportOptions{
		ConfigPath: configPath, AuthDir: dir, Repository: repo, OnlyIfEmpty: true,
	})
	if errImport != nil || stats != (ImportStats{}) || !inserted {
		t.Fatalf("interleaved initialization = %+v, %v; inserted=%v", stats, errImport, inserted)
	}
	var rows []ConfigRecord
	if errFind := db.Find(&rows).Error; errFind != nil {
		t.Fatal(errFind)
	}
	if len(rows) != 1 || rows[0].Key != "port" || string(rows[0].Value) != "9000" {
		t.Fatalf("existing config was replaced: %+v", rows)
	}
	assertTableCount(t, db, &APIKeyRecord{}, 0)
	assertTableCount(t, db, &AuthRecord{}, 0)
}

func importDatabaseRows(t *testing.T, db *gorm.DB) map[string][]string {
	t.Helper()
	tables, errTables := db.Migrator().GetTables()
	if errTables != nil {
		t.Fatal(errTables)
	}
	snapshot := make(map[string][]string, len(tables))
	for _, table := range tables {
		var rows []map[string]any
		if errFind := db.Table(table).Find(&rows).Error; errFind != nil {
			t.Fatal(errFind)
		}
		for _, row := range rows {
			encoded, errEncode := json.Marshal(row)
			if errEncode != nil {
				t.Fatal(errEncode)
			}
			snapshot[table] = append(snapshot[table], string(encoded))
		}
		sort.Strings(snapshot[table])
	}
	return snapshot
}

func TestImportLocalStateOnlyIfEmptySerializesInitializersPostgres(t *testing.T) {
	repo := newPostgresQuiescenceRepository(t)
	db, errDB := repo.database()
	if errDB != nil {
		t.Fatal(errDB)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var initialReads atomic.Int32
	barrier := make(chan struct{})
	errRegister := db.Callback().Query().After("gorm:query").Register("test:two-initializers", func(tx *gorm.DB) {
		if tx.Statement.Table != "config" {
			return
		}
		read := initialReads.Add(1)
		if read == 2 {
			close(barrier)
		}
		if read <= 2 {
			select {
			case <-barrier:
			case <-ctx.Done():
				t.Error("initializers did not reach the empty-config read")
			}
		}
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() {
		if errRemove := db.Callback().Query().Remove("test:two-initializers"); errRemove != nil {
			t.Errorf("remove callback: %v", errRemove)
		}
	})
	type outcome struct {
		stats ImportStats
		err   error
	}
	results := make(chan outcome, 2)
	for _, port := range []string{"8317", "9000"} {
		dir := t.TempDir()
		seed := filepath.Join(dir, "config.yaml")
		writeFile(t, seed, "port: "+port+"\n")
		go func() {
			stats, errImport := ImportLocalState(ctx, ImportOptions{
				ConfigPath: seed, AuthDir: dir, Repository: repo, OnlyIfEmpty: true,
			})
			results <- outcome{stats, errImport}
		}()
	}
	skipped := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent initializer: %v", result.err)
		}
		if result.stats == (ImportStats{}) {
			skipped++
		}
	}
	if skipped != 1 {
		t.Fatalf("skipped initializers = %d, want exactly one", skipped)
	}
}
