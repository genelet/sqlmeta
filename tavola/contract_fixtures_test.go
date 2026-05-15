package tavola

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestManualPKFKProjectFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/contracts/manual_pk_fk.project.json")
	if err != nil {
		t.Fatal(err)
	}
	spec := &Spec{}
	if err := json.Unmarshal(data, spec); err != nil {
		t.Fatal(err)
	}
	if spec.Project.Name != "SqlmetaApp" || spec.Introspection == nil || spec.Introspection.Source != "sqlmeta" {
		t.Fatalf("unexpected project fixture metadata: %#v", spec.Project)
	}
	tables := map[string]Table{}
	for _, table := range spec.Schema.Tables {
		tables[table.Name] = table
	}
	if tables["users"].PrimaryKey != "public_id" || tables["users"].AutoKey != "id" {
		t.Fatalf("users key = %q auto = %q", tables["users"].PrimaryKey, tables["users"].AutoKey)
	}
	components := map[string]Component{}
	for _, component := range spec.Components {
		components[component.Name] = component
	}
	if len(components["posts"].Roles["u"]) == 0 || len(components["users"].Roles["u"]) == 0 {
		t.Fatalf("role grants missing from generated fixture: %#v", components)
	}
	if len(components["audit_log"].Roles["u"]) != 0 {
		t.Fatalf("unrelated audit_log should remain public-only: %#v", components["audit_log"].Roles)
	}

	warnings, err := os.ReadFile("testdata/contracts/manual_pk_fk.warnings.txt")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(warnings)) != strings.Join(spec.Introspection.Warnings, "\n") {
		t.Fatalf("warning snapshot does not match project JSON")
	}
}
