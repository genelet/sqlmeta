package xmeta

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestContractFixtures(t *testing.T) {
	names := ContractFixtureNames()
	if got := strings.Join(names, ","); got != "auth_descendants.expanded_app_spec.json,basic_crud.app_spec.json,manual_fk.expanded_app_spec.json,manual_pk.expanded_app_spec.json" {
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
