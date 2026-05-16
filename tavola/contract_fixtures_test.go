package tavola

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/genelet/sqlmeta/xmeta"
)

func TestManualPKFKProjectFixture(t *testing.T) {
	spec := readContractProject(t, "manual_pk_fk")
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

	assertWarningSnapshot(t, "manual_pk_fk", spec)
}

func TestInvalidOverridesProjectFixture(t *testing.T) {
	spec := readContractProject(t, "invalid_overrides")
	if spec.Project.Name != "SqlmetaApp" || spec.Introspection == nil || spec.Introspection.Source != "sqlmeta" {
		t.Fatalf("unexpected project fixture metadata: %#v", spec.Project)
	}
	components := map[string]Component{}
	for _, component := range spec.Components {
		components[component.Table] = component
	}
	if len(components["users"].Roles["u"]) == 0 || len(components["posts"].Roles["u"]) == 0 {
		t.Fatalf("expected valid auth-scope grants, components = %#v", components)
	}
	for _, table := range []string{"audit_log", "memberships", "archive.teams", "teams"} {
		if len(components[table].Roles["u"]) != 0 {
			t.Fatalf("unrelated table %s should not receive protected role grants: %#v", table, components[table].Roles)
		}
	}
	warnings := strings.Join(spec.Introspection.Warnings, "\n")
	for _, want := range []string{
		"manual primary key users_missing_public_id_pk on users references missing column missing_public_id",
		"manual foreign key posts_ambiguous_team_fk parent table \"teams\": ambiguous table name matched archive.teams, public.teams",
		"manual foreign key memberships_user_composite_fk on memberships has composite columns; skipped role scope edge",
		"manual foreign key memberships_user_composite_fk on memberships has composite columns; skipped virtual schema edge",
		"manual foreign key posts_missing_child_fk on posts references missing child column missing_user_id",
		"auth role was generated without a login procedure",
	} {
		if !strings.Contains(warnings, want) {
			t.Fatalf("expected warning containing %q, got:\n%s", want, warnings)
		}
	}
	assertWarningSnapshot(t, "invalid_overrides", spec)
}

func TestMissingAuthTableErrorFixture(t *testing.T) {
	scenario, err := xmeta.LoadContractScenario(xmeta.ContractScenarioMissingAuthTable)
	if err != nil {
		t.Fatal(err)
	}
	_, err = BuildSpec(scenario.Meta, Options{
		Project:            "SqlmetaApp",
		Script:             "/sqlmeta/app.php",
		PublicRole:         "p",
		OwnerLogin:         "local",
		OwnerEmail:         "local@example.test",
		OwnerTypeID:        1,
		DatasourceType:     "SQLite",
		DatasourceNickname: "sqlmeta",
		DatasourceDatabase: "app.sqlite",
		DatasourcePath:     "data/sqlmeta.sqlite",
		Auth: AuthOptions{
			Role:      scenario.RoleName,
			Table:     strings.Join(scenario.Auth.GetUserTable().GetIdents(), "."),
			ID:        scenario.Auth.GetUserIDColumn(),
			Login:     scenario.Auth.GetLoginColumn(),
			Password:  scenario.Auth.GetPasswordColumn(),
			FirstName: scenario.Auth.GetFirstNameColumn(),
			LastName:  scenario.Auth.GetLastNameColumn(),
		},
		SchemaOverrides: scenario.SchemaOverrides,
	})
	if err == nil {
		t.Fatal("expected missing auth table error")
	}
	data, readErr := os.ReadFile("testdata/contracts/missing_auth_table.tavola_error.txt")
	if readErr != nil {
		t.Fatal(readErr)
	}
	if got := strings.TrimSpace(string(data)); got != err.Error() {
		t.Fatalf("error snapshot = %q, want %q", got, err.Error())
	}
}

func readContractProject(t *testing.T, scenario string) *Spec {
	t.Helper()
	data, err := os.ReadFile("testdata/contracts/" + scenario + ".project.json")
	if err != nil {
		t.Fatal(err)
	}
	spec := &Spec{}
	if err := json.Unmarshal(data, spec); err != nil {
		t.Fatal(err)
	}
	return spec
}

func assertWarningSnapshot(t *testing.T, scenario string, spec *Spec) {
	t.Helper()
	warnings, err := os.ReadFile("testdata/contracts/" + scenario + ".warnings.txt")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(warnings)) != strings.Join(spec.Introspection.Warnings, "\n") {
		t.Fatalf("%s warning snapshot does not match project JSON", scenario)
	}
	if spec.Introspection == nil {
		t.Fatalf("%s introspection is nil", scenario)
	}
	if got := len(spec.Introspection.WarningDetails); got != len(spec.Introspection.Warnings) {
		t.Fatalf("%s warning detail count = %d, want %d", scenario, got, len(spec.Introspection.Warnings))
	}
	for i, detail := range spec.Introspection.WarningDetails {
		if detail.Message != spec.Introspection.Warnings[i] {
			t.Fatalf("%s warningDetails[%d].message = %q, want %q", scenario, i, detail.Message, spec.Introspection.Warnings[i])
		}
		if detail.Code == "" || detail.Code == xmeta.DiagnosticUnknown {
			t.Fatalf("%s warningDetails[%d] has unknown code for %q", scenario, i, detail.Message)
		}
	}
}
