package xmeta

import (
	"os"
	"sort"
	"strings"
	"testing"
)

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

func TestLoadInvalidOverridesContractScenario(t *testing.T) {
	scenario, err := LoadContractScenario(ContractScenarioInvalidOverrides)
	if err != nil {
		t.Fatal(err)
	}
	if scenario.Name != ContractScenarioInvalidOverrides || scenario.AppName != "Invalid Overrides" {
		t.Fatalf("unexpected scenario metadata: %#v", scenario)
	}
	if scenario.Meta == nil || len(scenario.Meta.GetTables()) != 6 {
		t.Fatalf("scenario tables = %#v", scenario.Meta.GetTables())
	}
	if scenario.Auth.GetUserTable().GetIdents()[0] != "users" || scenario.Auth.GetUserIDColumn() != "id" {
		t.Fatalf("scenario auth = %#v", scenario.Auth)
	}
	if len(scenario.SchemaOverrides.GetPrimaryKeys()) != 1 || len(scenario.SchemaOverrides.GetForeignKeys()) != 3 {
		t.Fatalf("scenario overrides = %#v", scenario.SchemaOverrides)
	}
}

func TestInvalidOverridesContractScenarioExpandsWithWarnings(t *testing.T) {
	scenario, err := LoadContractScenario(ContractScenarioInvalidOverrides)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := BuildDefaultAppSpec(scenario.Meta, AppSpecOptions{
		Name:            scenario.AppName,
		Auth:            scenario.Auth,
		RoleName:        scenario.RoleName,
		SchemaOverrides: scenario.SchemaOverrides,
	})
	if err != nil {
		t.Fatal(err)
	}
	expanded, err := ExpandRoleScopes(scenario.Meta, spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(tableGrantKeys(expanded.GetTableGrants()), ","); got != "posts,users" {
		t.Fatalf("invalid override grants = %s", got)
	}
	warnings := strings.Join(expanded.GetWarnings(), "\n")
	for _, want := range []string{
		"manual primary key users_missing_public_id_pk on users references missing column missing_public_id",
		"manual foreign key posts_ambiguous_team_fk parent table \"teams\": ambiguous table name matched archive.teams, public.teams",
		"manual foreign key memberships_user_composite_fk on memberships has composite columns; skipped role scope edge",
		"manual foreign key posts_missing_child_fk on posts references missing child column missing_user_id",
	} {
		if !strings.Contains(warnings, want) {
			t.Fatalf("expected warning containing %q, got:\n%s", want, warnings)
		}
	}
}

func TestMissingAuthTableContractScenarioErrors(t *testing.T) {
	scenario, err := LoadContractScenario(ContractScenarioMissingAuthTable)
	if err != nil {
		t.Fatal(err)
	}
	if got := scenario.Auth.GetUserTable().GetIdents()[0]; got != "missing_users" {
		t.Fatalf("auth table = %s", got)
	}
	spec, err := BuildDefaultAppSpec(scenario.Meta, AppSpecOptions{
		Name:            scenario.AppName,
		Auth:            scenario.Auth,
		RoleName:        scenario.RoleName,
		SchemaOverrides: scenario.SchemaOverrides,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ExpandRoleScopes(scenario.Meta, spec)
	if err == nil {
		t.Fatal("expected missing auth table error")
	}
	data, readErr := os.ReadFile("testdata/contracts/missing_auth_table.expand_error.txt")
	if readErr != nil {
		t.Fatal(readErr)
	}
	if got := strings.TrimSpace(string(data)); got != err.Error() {
		t.Fatalf("error snapshot = %q, want %q", got, err.Error())
	}
}

func TestCurrentContractScenarioDiagnosticsHaveKnownCodes(t *testing.T) {
	for _, name := range []string{ContractScenarioManualPKFK, ContractScenarioInvalidOverrides} {
		t.Run(name, func(t *testing.T) {
			scenario, err := LoadContractScenario(name)
			if err != nil {
				t.Fatal(err)
			}
			spec, err := BuildDefaultAppSpec(scenario.Meta, AppSpecOptions{
				Name:            scenario.AppName,
				Auth:            scenario.Auth,
				RoleName:        scenario.RoleName,
				SchemaOverrides: scenario.SchemaOverrides,
			})
			if err != nil {
				t.Fatal(err)
			}
			expanded, diagnostics, err := ExpandRoleScopesWithDiagnostics(scenario.Meta, spec)
			if err != nil {
				t.Fatal(err)
			}
			messages := DiagnosticMessages(diagnostics)
			sort.Strings(messages)
			if got, want := strings.Join(messages, "\n"), strings.Join(expanded.GetWarnings(), "\n"); got != want {
				t.Fatalf("diagnostic messages =\n%s\nwant warnings =\n%s", got, want)
			}
			for _, diagnostic := range diagnostics {
				if diagnostic.Code == DiagnosticUnknown {
					t.Fatalf("unknown diagnostic code for warning %q", diagnostic.Message)
				}
			}
		})
	}

	t.Run(ContractScenarioMissingAuthTable, func(t *testing.T) {
		scenario, err := LoadContractScenario(ContractScenarioMissingAuthTable)
		if err != nil {
			t.Fatal(err)
		}
		spec, err := BuildDefaultAppSpec(scenario.Meta, AppSpecOptions{
			Name:            scenario.AppName,
			Auth:            scenario.Auth,
			RoleName:        scenario.RoleName,
			SchemaOverrides: scenario.SchemaOverrides,
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = ExpandRoleScopes(scenario.Meta, spec)
		if err == nil {
			t.Fatal("expected missing auth table error")
		}
		if diagnostic := ErrorDiagnostic(err); diagnostic.Code == DiagnosticUnknown {
			t.Fatalf("unknown diagnostic code for error %q", diagnostic.Message)
		}
	})
}

func TestLoadContractScenarioRejectsUnknownName(t *testing.T) {
	if _, err := LoadContractScenario("missing"); err == nil {
		t.Fatal("expected unknown scenario error")
	}
}
