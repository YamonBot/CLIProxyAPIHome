package synthesizer

import (
	"context"
	"encoding/json"
	coreauth "github.com/router-for-me/CLIProxyAPIHome/internal/cliproxy/auth"
	appconfig "github.com/router-for-me/CLIProxyAPIHome/internal/config"
	"github.com/router-for-me/CLIProxyAPIHome/internal/registry"
	"testing"
	"time"
)

func TestOpenAICompatibilityMetadataPreservesUpstreamName(t *testing.T) {
	models := buildOpenAICompatibilityModels([]appconfig.OpenAICompatibilityModel{
		{Name: " glm-5.3 ", Alias: "fractalops-coding"},
		{Name: "glm-5.3", Alias: "symphony-glm"},
	}, "zai-coding-plan-glm-5-3", time.Unix(100, 0))
	raw, err := json.Marshal(modelInfoMetadataPayload(models))
	if err != nil {
		t.Fatal(err)
	}
	var persisted []map[string]any
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	for i, alias := range []string{"fractalops-coding", "symphony-glm"} {
		if persisted[i]["name"] != "glm-5.3" || persisted[i]["id"] != alias {
			t.Fatalf("persisted model %d = %v; upstream and alias must remain distinct", i, persisted[i])
		}
	}
}

// Home hydrates credentials independently of runtime config; dispatch must use
// the same persisted model metadata that management exports.
func TestOpenAICompatibilityPersistedMetadataDispatchesUpstream(t *testing.T) {
	models := buildOpenAICompatibilityModels([]appconfig.OpenAICompatibilityModel{
		{Name: "glm-5.3", Alias: "fractalops-coding"},
		{Name: "glm-5.3", Alias: "symphony-glm"},
	}, "zai-coding-plan-glm-5-3", time.Unix(100, 0))
	original := &coreauth.Auth{ID: "openai-persisted-dispatch", Provider: "zai-coding-plan-glm-5-3", Status: coreauth.StatusActive,
		Attributes: map[string]string{"api_key": "synthetic-key", "compat_name": "zai-coding-plan-glm-5-3"},
		Metadata:   map[string]any{homeConfigModelsMetadataKey: modelInfoMetadataPayload(models)}}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var persisted coreauth.Auth
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	registry.GetGlobalRegistry().RegisterClient(persisted.ID, persisted.Provider, models)
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(persisted.ID) })
	if _, err := manager.Register(context.Background(), &persisted); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"fractalops-coding", "symphony-glm"} {
		decision, err := manager.Dispatch(context.Background(), []string{persisted.Provider}, alias, coreauth.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if decision.UpstreamModel != "glm-5.3" {
			t.Fatalf("Dispatch(%s) upstream = %q, want glm-5.3", alias, decision.UpstreamModel)
		}
	}
}
