package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/router-for-me/CLIProxyAPIHome/internal/cluster"
)

func TestCertificateBootstrapUsesLiveTarget(t *testing.T) {
	for _, test := range []struct {
		name          string
		masterAge     time.Duration
		master        bool
		masterRetired bool
		issuerIP      string
		issuerPort    int
		wantIP        string
	}{
		{name: "rolling update stale master uses issuer", master: true, masterAge: 2 * time.Minute, issuerIP: "10.52.0.101", issuerPort: 8327, wantIP: "10.52.0.101"},
		{name: "recently live retired master uses responding issuer", master: true, masterRetired: true, issuerIP: "10.52.0.101", issuerPort: 8327, wantIP: "10.52.0.101"},
		{name: "healthy master does not override issuer", master: true, issuerIP: " 10.52.0.101 ", issuerPort: 8327, wantIP: "10.52.0.101"},
		{name: "no elected master uses issuer", issuerIP: "10.52.0.101", issuerPort: 8327, wantIP: "10.52.0.101"},
		{name: "missing issuer IP falls back to live master", master: true, issuerIP: " ", issuerPort: 8327, wantIP: "10.52.0.100"},
		{name: "invalid issuer port falls back to live master", master: true, issuerIP: "10.52.0.101", wantIP: "10.52.0.100"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, cleanup := openManagementLogTestDB(t)
			defer cleanup()
			repo := cluster.NewRepository(db)
			ctx := t.Context()
			if _, errEnsure := repo.EnsureLifecycleConfig(ctx, time.Minute); errEnsure != nil {
				t.Fatal(errEnsure)
			}
			now, errNow := cluster.DatabaseNow(ctx, db)
			if errNow != nil {
				t.Fatal(errNow)
			}
			for _, record := range []cluster.ClusterNodeRecord{
				{IP: "10.52.0.100", Port: 8327, IsMaster: test.master, StartedAt: now.Add(-time.Hour), LastSeenAt: now.Add(-test.masterAge)},
				{IP: "10.52.0.101", Port: 8327, StartedAt: now.Add(-time.Minute), LastSeenAt: now},
				{IP: "10.52.0.102", Port: 8327, StartedAt: now.Add(-time.Minute), LastSeenAt: now},
			} {
				if errCreate := db.Create(&record).Error; errCreate != nil {
					t.Fatal(errCreate)
				}
			}
			if test.masterRetired {
				retired := cluster.HomeProcessIncarnationRecord{
					HomeIP: "10.52.0.100", HomePort: 8327, StartedAt: now.Add(-time.Hour),
					LastSeenAt: now, State: cluster.HomeIncarnationRetired, Capabilities: cluster.JSONB(`[]`),
				}
				if errCreate := db.Create(&retired).Error; errCreate != nil {
					t.Fatal(errCreate)
				}
			}
			handler := NewHandler(repo, nil, test.issuerIP, test.issuerPort)
			engine := gin.New()
			engine.POST("/certificates/clients", handler.CreateClientCertificate)
			response := httptest.NewRecorder()
			engine.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/certificates/clients", nil))
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", response.Code)
			}
			var body struct {
				ID      string `json:"id"`
				HomeJWT string `json:"home_jwt"`
			}
			if errDecode := json.Unmarshal(response.Body.Bytes(), &body); errDecode != nil {
				t.Fatal(errDecode)
			}
			ca, _, errCA := repo.ClusterCAKeyPair(ctx)
			if errCA != nil {
				t.Fatal(errCA)
			}
			token, errParse := jwt.Parse(body.HomeJWT, func(_ *jwt.Token) (any, error) {
				return ca.PublicKey, nil
			}, jwt.WithValidMethods([]string{"RS256"}))
			if errParse != nil || !token.Valid {
				t.Fatalf("CA-signed Home JWT verification failed: %v", errParse)
			}
			claims := token.Claims.(jwt.MapClaims)
			if claims["ip"] != test.wantIP || claims["port"] != float64(8327) {
				t.Fatalf("signed target = %v:%v, want %s:8327", claims["ip"], claims["port"], test.wantIP)
			}
			if body.ID == "" || claims["certificate_id"] != body.ID {
				t.Fatal("signed enrollment certificate identity mismatch")
			}
			for _, name := range []string{"cluster_id", "ca_fingerprint", "enrollment_secret"} {
				if value, okValue := claims[name].(string); !okValue || value == "" {
					t.Fatalf("signed enrollment claim %s missing", name)
				}
			}
		})
	}
}
