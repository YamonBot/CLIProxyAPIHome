package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigOptionalParsesExternalPort(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "cluster.yaml")
	content := strings.TrimSpace(`
pgsql:
  host: "127.0.0.1"
  port: 5432
  user: "cliproxy"
  password: "secret"
  database: "cliproxy_home"
node:
  external-ip: "203.0.113.10"
  external-port: 443
  port: 8327
`) + "\n"
	if errWrite := os.WriteFile(configPath, []byte(content), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}

	cfg, exists, errLoad := LoadConfigOptional(configPath)
	if errLoad != nil {
		t.Fatalf("LoadConfigOptional failed: %v", errLoad)
	}
	if !exists {
		t.Fatalf("expected config to exist")
	}
	if cfg.Node.ExternalIP != "203.0.113.10" {
		t.Fatalf("external ip = %q, want %q", cfg.Node.ExternalIP, "203.0.113.10")
	}
	if cfg.Node.ExternalPort != 443 {
		t.Fatalf("external port = %d, want 443", cfg.Node.ExternalPort)
	}
}

func TestLoadConfigOptionalParsesPostgresURI(t *testing.T) {
	t.Parallel()

	const postgresURI = "postgresql://cliproxy:secret@db.example.com:5432/cliproxy_home?sslmode=require"
	configPath := filepath.Join(t.TempDir(), "cluster.yaml")
	content := strings.TrimSpace(`
pgsql:
  postgres_uri: "postgresql://cliproxy:secret@db.example.com:5432/cliproxy_home?sslmode=require"
node:
  port: 8327
`) + "\n"
	if errWrite := os.WriteFile(configPath, []byte(content), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}

	cfg, exists, errLoad := LoadConfigOptional(configPath)
	if errLoad != nil {
		t.Fatalf("LoadConfigOptional failed: %v", errLoad)
	}
	if !exists {
		t.Fatal("expected config to exist")
	}
	if cfg.PGSQL.PostgresURI != postgresURI {
		t.Fatalf("pgsql postgres_uri = %q, want %q", cfg.PGSQL.PostgresURI, postgresURI)
	}
	dsn, errDSN := cfg.PGSQL.DSN()
	if errDSN != nil {
		t.Fatalf("build PostgreSQL DSN: %v", errDSN)
	}
	if dsn != postgresURI {
		t.Fatalf("PostgreSQL DSN = %q, want %q", dsn, postgresURI)
	}
}

func TestLoadConfigOptionalDatabaseSlowQueryThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		backend DatabaseBackend
		want    time.Duration
	}{
		{
			name: "postgres default",
			content: `
pgsql:
  postgres_uri: "postgresql://cliproxy:secret@db.example.com/cliproxy_home"
node:
  port: 8327
`,
			backend: DatabaseBackendPostgres,
			want:    defaultDatabaseSlowQueryThreshold,
		},
		{
			name: "postgres configured",
			content: `
pgsql:
  postgres_uri: "postgresql://cliproxy:secret@db.example.com/cliproxy_home"
  slow-query-threshold: "450ms"
node:
  port: 8327
`,
			backend: DatabaseBackendPostgres,
			want:    450 * time.Millisecond,
		},
		{
			name: "sqlite default",
			content: `
sqlite:
  path: "home.db"
node:
  port: 8327
`,
			backend: DatabaseBackendSQLite,
			want:    defaultDatabaseSlowQueryThreshold,
		},
		{
			name: "sqlite configured",
			content: `
sqlite:
  path: "home.db"
  slow-query-threshold: "650ms"
node:
  port: 8327
`,
			backend: DatabaseBackendSQLite,
			want:    650 * time.Millisecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			configPath := filepath.Join(t.TempDir(), "cluster.yaml")
			content := strings.TrimSpace(test.content) + "\n"
			if errWrite := os.WriteFile(configPath, []byte(content), 0o600); errWrite != nil {
				t.Fatalf("write config: %v", errWrite)
			}

			cfg, exists, errLoad := LoadConfigOptional(configPath)
			if errLoad != nil {
				t.Fatalf("LoadConfigOptional failed: %v", errLoad)
			}
			if !exists {
				t.Fatal("expected config to exist")
			}
			if gotBackend := cfg.DatabaseBackend(); gotBackend != test.backend {
				t.Fatalf("database backend = %q, want %q", gotBackend, test.backend)
			}

			gotThreshold := cfg.PGSQL.SlowQueryThreshold
			if test.backend == DatabaseBackendSQLite {
				gotThreshold = cfg.SQLite.SlowQueryThreshold
			}
			if gotThreshold != test.want {
				t.Fatalf("slow query threshold = %s, want %s", gotThreshold, test.want)
			}
		})
	}
}

func TestLoadConfigOptionalRejectsNegativeDatabaseSlowQueryThreshold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		content   string
		wantError string
	}{
		{
			name: "postgres",
			content: `
pgsql:
  postgres_uri: "postgresql://cliproxy:secret@db.example.com/cliproxy_home"
  slow-query-threshold: "-1ms"
node:
  port: 8327
`,
			wantError: "pgsql.slow-query-threshold",
		},
		{
			name: "sqlite",
			content: `
sqlite:
  path: "home.db"
  slow-query-threshold: "-1ms"
node:
  port: 8327
`,
			wantError: "sqlite.slow-query-threshold",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			configPath := filepath.Join(t.TempDir(), "cluster.yaml")
			content := strings.TrimSpace(test.content) + "\n"
			if errWrite := os.WriteFile(configPath, []byte(content), 0o600); errWrite != nil {
				t.Fatalf("write config: %v", errWrite)
			}

			_, exists, errLoad := LoadConfigOptional(configPath)
			if errLoad == nil {
				t.Fatal("expected validation error")
			}
			if !exists {
				t.Fatal("expected config to exist")
			}
			if !strings.Contains(errLoad.Error(), test.wantError) {
				t.Fatalf("unexpected validation error: %v", errLoad)
			}
		})
	}
}

func TestPGSQLConfigRejectsMixedPostgresURIAndIndividualFields(t *testing.T) {
	t.Parallel()

	cfg := PGSQLConfig{
		PostgresURI: "postgresql://cliproxy:secret@db.example.com/cliproxy_home",
		Host:        "db.example.com",
	}
	_, errDSN := cfg.DSN()
	if errDSN == nil {
		t.Fatal("expected mixed PostgreSQL configuration to fail")
	}
	if !strings.Contains(errDSN.Error(), "cannot be combined") {
		t.Fatalf("unexpected validation error: %v", errDSN)
	}
}

func TestLoadConfigOptional_RejectsBothPGSQLAndSQLite(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "cluster.yaml")
	content := strings.TrimSpace(`
pgsql:
  host: "127.0.0.1"
  port: 5432
  user: "cliproxy"
  password: "secret"
  database: "cliproxy_home"
sqlite:
  path: "home.db"
node:
  port: 8327
`) + "\n"
	if errWrite := os.WriteFile(configPath, []byte(content), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}

	_, exists, errLoad := LoadConfigOptional(configPath)
	if errLoad == nil {
		t.Fatalf("expected validation error")
	}
	if !exists {
		t.Fatalf("expected config to exist")
	}
	if !strings.Contains(errLoad.Error(), "exactly one database backend") {
		t.Fatalf("unexpected validation error: %v", errLoad)
	}
}

func TestConfigValidateRejectsNegativeExternalPort(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		PGSQL: PGSQLConfig{
			Host:     "127.0.0.1",
			Port:     5432,
			User:     "cliproxy",
			Database: "cliproxy_home",
		},
		Node: NodeConfig{
			ExternalPort:      -1,
			Port:              8327,
			HeartbeatInterval: defaultHeartbeatInterval,
			HeartbeatTimeout:  defaultHeartbeatTimeout,
			EventPollInterval: defaultHeartbeatInterval,
		},
	}

	errValidate := cfg.Validate()
	if errValidate == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(errValidate.Error(), "node.external-port") {
		t.Fatalf("unexpected validation error: %v", errValidate)
	}
}

func TestPostgresPoolSettingsDoNotSelectDatabaseBackend(t *testing.T) {
	maxOpen, maxIdle := 12, 2
	cfg := PGSQLConfig{MaxOpenConns: &maxOpen, MaxIdleConns: &maxIdle}
	if cfg.Configured() {
		t.Fatal("pool settings alone selected PostgreSQL")
	}
	if _, errDSN := cfg.DSN(); errDSN == nil || !strings.Contains(errDSN.Error(), "pgsql.host is required") {
		t.Fatalf("pool-only DSN error = %v, want missing connection settings", errDSN)
	}
}

func TestLoadConfigOptionalPostgresPoolLimits(t *testing.T) {
	for _, test := range []struct {
		name      string
		settings  string
		maxOpen   int
		maxIdle   int
		wantError string
	}{
		{name: "defaults", maxOpen: 24, maxIdle: 8},
		{name: "configured", settings: "  max-open-conns: 12\n  max-idle-conns: 2\n", maxOpen: 12, maxIdle: 2},
		{name: "smaller open limit", settings: "  max-open-conns: 4\n", maxOpen: 4, maxIdle: 4},
		{name: "no idle connections", settings: "  max-idle-conns: 0\n", maxOpen: 24, maxIdle: 0},
		{name: "zero open", settings: "  max-open-conns: 0\n", wantError: "max-open-conns must be greater than 0"},
		{name: "negative open", settings: "  max-open-conns: -1\n", wantError: "max-open-conns must be greater than 0"},
		{name: "negative idle", settings: "  max-idle-conns: -1\n", wantError: "max-idle-conns must not be negative"},
		{name: "idle above open", settings: "  max-open-conns: 12\n  max-idle-conns: 13\n", wantError: "max-idle-conns must not exceed max-open-conns"},
	} {
		t.Run(test.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "cluster.yaml")
			content := "pgsql:\n  postgres_uri: postgresql://pool-test@localhost/home\n" + test.settings + "node:\n  port: 8327\n"
			if errWrite := os.WriteFile(configPath, []byte(content), 0o600); errWrite != nil {
				t.Fatal(errWrite)
			}
			cfg, exists, errLoad := LoadConfigOptional(configPath)
			if !exists {
				t.Fatal("configuration missing")
			}
			if test.wantError != "" {
				if errLoad == nil || !strings.Contains(errLoad.Error(), test.wantError) {
					t.Fatalf("configuration error = %v, want %q", errLoad, test.wantError)
				}
				return
			}
			if errLoad != nil {
				t.Fatal(errLoad)
			}
			maxOpen, maxIdle := cfg.PGSQL.poolLimits()
			if maxOpen != test.maxOpen || maxIdle != test.maxIdle {
				t.Fatalf("pool limits = %d/%d, want %d/%d", maxOpen, maxIdle, test.maxOpen, test.maxIdle)
			}
		})
	}
}
