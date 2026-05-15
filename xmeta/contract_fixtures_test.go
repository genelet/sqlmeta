package xmeta

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestContractFixtures(t *testing.T) {
	names := ContractFixtureNames()
	if got := strings.Join(names, ","); got != "auth_descendants.expanded_app_spec.json,basic_crud.app_spec.json,invalid_overrides.expanded_app_spec.json,manual_fk.expanded_app_spec.json,manual_pk.expanded_app_spec.json,manual_pk_fk.expanded_app_spec.json" {
		t.Fatalf("fixture names = %s", got)
	}

	basic := mustContractAppSpec(t, "basic_crud.app_spec.json")
	if basic.GetName() != "Basic CRUD" || len(basic.GetComponents()) != 2 || len(basic.GetRoles()) != 0 {
		t.Fatalf("basic fixture shape changed: %#v", basic)
	}

	auth := mustContractExpandedAppSpec(t, "auth_descendants.expanded_app_spec.json")
	if got := strings.Join(tableGrantKeys(auth.GetTableGrants()), ","); got != "comments,posts,users" {
		t.Fatalf("auth descendant grants = %s", got)
	}

	manualPK := mustContractExpandedAppSpec(t, "manual_pk.expanded_app_spec.json")
	if got := manualPK.GetSpec().GetSchemaOverrides().GetPrimaryKeys(); len(got) != 1 || got[0].GetColumns()[0] != "public_id" {
		t.Fatalf("manual PK fixture override changed: %#v", got)
	}
	if got := strings.Join(tableGrantKeys(manualPK.GetTableGrants()), ","); got != "orders,users" {
		t.Fatalf("manual PK grants = %s", got)
	}

	manualFK := mustContractExpandedAppSpec(t, "manual_fk.expanded_app_spec.json")
	if got := manualFK.GetSpec().GetSchemaOverrides().GetForeignKeys(); len(got) != 1 || got[0].GetChildColumns()[0] != "user_id" {
		t.Fatalf("manual FK fixture override changed: %#v", got)
	}
	if got := strings.Join(tableGrantKeys(manualFK.GetTableGrants()), ","); got != "orders,users" {
		t.Fatalf("manual FK grants = %s", got)
	}

	manualPKFK := mustContractExpandedAppSpec(t, "manual_pk_fk.expanded_app_spec.json")
	if got := strings.Join(tableGrantKeys(manualPKFK.GetTableGrants()), ","); got != "posts,users" {
		t.Fatalf("manual PK/FK grants = %s", got)
	}
	if got := strings.Join(componentNames(manualPKFK.GetSpec().GetComponents()), ","); got != "audit_log,posts,users" {
		t.Fatalf("manual PK/FK components = %s", got)
	}
	if grant := findTableGrant(manualPKFK.GetTableGrants(), "posts"); grant == nil {
		t.Fatal("posts grant missing")
	} else if join := grant.GetTraversalJoins()[0]; join.GetChildColumn() != "user_public_id" || join.GetParentColumn() != "public_id" {
		t.Fatalf("manual PK/FK traversal = %#v", join)
	}

	invalid := mustContractExpandedAppSpec(t, "invalid_overrides.expanded_app_spec.json")
	if got := strings.Join(tableGrantKeys(invalid.GetTableGrants()), ","); got != "posts,users" {
		t.Fatalf("invalid override grants = %s", got)
	}
	if findTableGrant(invalid.GetTableGrants(), "audit_log") != nil || findTableGrant(invalid.GetTableGrants(), "memberships") != nil {
		t.Fatalf("invalid override unrelated tables were granted: %#v", invalid.GetTableGrants())
	}
	warnings := strings.Join(invalid.GetWarnings(), "\n")
	for _, want := range []string{
		"missing_public_id",
		"ambiguous table name matched archive.teams, public.teams",
		"composite columns; skipped role scope edge",
		"missing_user_id",
	} {
		if !strings.Contains(warnings, want) {
			t.Fatalf("expected invalid override warning containing %q, got:\n%s", want, warnings)
		}
	}
}

func mustContractAppSpec(t *testing.T, name string) *AppSpec {
	t.Helper()
	data, err := ContractFixture(name)
	if err != nil {
		t.Fatal(err)
	}
	spec := &AppSpec{}
	if err := protojson.Unmarshal(data, spec); err != nil {
		t.Fatalf("unmarshal %s: %v", name, err)
	}
	return spec
}

func mustContractExpandedAppSpec(t *testing.T, name string) *ExpandedAppSpec {
	t.Helper()
	data, err := ContractFixture(name)
	if err != nil {
		t.Fatal(err)
	}
	spec := &ExpandedAppSpec{}
	if err := protojson.Unmarshal(data, spec); err != nil {
		t.Fatalf("unmarshal %s: %v", name, err)
	}
	return spec
}
