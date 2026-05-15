package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/genelet/sqlmeta/tavola"
	"github.com/genelet/sqlmeta/xmeta"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	var root, tavolaOut string
	flag.StringVar(&root, "root", ".", "sqlmeta repository root")
	flag.StringVar(&tavolaOut, "tavola-out", "", "optional downstream Tavola project JSON path")
	flag.Parse()

	if err := run(root, tavolaOut); err != nil {
		fmt.Fprintln(os.Stderr, "refresh-contract-fixtures:", err)
		os.Exit(1)
	}
}

func run(root, tavolaOut string) error {
	scenario, err := xmeta.LoadContractScenario(xmeta.ContractScenarioManualPKFK)
	if err != nil {
		return err
	}
	app, err := xmeta.BuildDefaultAppSpec(scenario.Meta, xmeta.AppSpecOptions{
		Name:            scenario.AppName,
		Auth:            scenario.Auth,
		RoleName:        scenario.RoleName,
		SchemaOverrides: scenario.SchemaOverrides,
	})
	if err != nil {
		return err
	}
	expanded, err := xmeta.ExpandRoleScopes(scenario.Meta, app)
	if err != nil {
		return err
	}
	spec, err := tavola.BuildTavolaSpec(scenario.Meta, expanded, tavola.Options{
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
	})
	if err != nil {
		return err
	}

	if err := writeProtoJSON(filepath.Join(root, "xmeta/testdata/contracts/"+xmeta.ManualPKFKExpandedFixture), expanded); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(root, "tavola/testdata/contracts/manual_pk_fk.project.json"), spec); err != nil {
		return err
	}
	if err := writeWarnings(filepath.Join(root, "tavola/testdata/contracts/manual_pk_fk.warnings.txt"), specWarnings(spec)); err != nil {
		return err
	}
	if tavolaOut != "" {
		if err := writeJSON(tavolaOut, spec); err != nil {
			return err
		}
	}
	return nil
}

func writeProtoJSON(path string, msg proto.Message) error {
	data, err := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		EmitUnpopulated: false,
	}.Marshal(msg)
	if err != nil {
		return err
	}
	return writeFile(path, append(data, '\n'))
}

func specWarnings(spec *tavola.Spec) []string {
	if spec == nil || spec.Introspection == nil {
		return nil
	}
	return spec.Introspection.Warnings
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(path, append(data, '\n'))
}

func writeWarnings(path string, warnings []string) error {
	return writeFile(path, []byte(strings.Join(warnings, "\n")+"\n"))
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
