package xmeta

import "testing"

func TestLoadManualPKFKContractScenario(t *testing.T) {
	scenario, err := LoadContractScenario(ContractScenarioManualPKFK)
	if err != nil {
		t.Fatal(err)
	}
	if scenario.Name != ContractScenarioManualPKFK || scenario.AppName != "Manual PK/FK" {
		t.Fatalf("unexpected scenario metadata: %#v", scenario)
	}
	if scenario.Meta == nil || len(scenario.Meta.GetTables()) != 3 {
		t.Fatalf("scenario tables = %#v", scenario.Meta.GetTables())
	}
	if scenario.Auth.GetUserTable().GetIdents()[0] != "users" || scenario.Auth.GetUserIDColumn() != "public_id" {
		t.Fatalf("scenario auth = %#v", scenario.Auth)
	}
	if len(scenario.SchemaOverrides.GetPrimaryKeys()) != 1 || len(scenario.SchemaOverrides.GetForeignKeys()) != 1 {
		t.Fatalf("scenario overrides = %#v", scenario.SchemaOverrides)
	}
}

func TestLoadContractScenarioRejectsUnknownName(t *testing.T) {
	if _, err := LoadContractScenario("missing"); err == nil {
		t.Fatal("expected unknown scenario error")
	}
}
