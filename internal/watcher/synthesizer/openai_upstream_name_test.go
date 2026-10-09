package synthesizer

import (
	"encoding/json"
	appconfig "github.com/router-for-me/CLIProxyAPIHome/internal/config"
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
