package main

import (
	"bytes"
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
	if err := writeSuccessScenario(root, xmeta.ContractScenarioManualPKFK, tavolaOut); err != nil {
		return err
	}
	if err := writeSuccessScenario(root, xmeta.ContractScenarioInvalidOverrides, ""); err != nil {
		return err
	}
	if err := writeMissingAuthScenario(root); err != nil {
		return err
	}
	return nil
}

func writeSuccessScenario(root, name, tavolaOut string) error {
	scenario, err := xmeta.LoadContractScenario(name)
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
	spec, err := tavola.BuildTavolaSpec(scenario.Meta, expanded, tavolaOptions(scenario))
	if err != nil {
		return err
	}

	if err := writeProtoJSON(filepath.Join(root, "xmeta/testdata/contracts/"+name+".expanded_app_spec.json"), expanded); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(root, "tavola/testdata/contracts/"+name+".project.json"), spec); err != nil {
		return err
	}
	if err := writeWarnings(filepath.Join(root, "tavola/testdata/contracts/"+name+".warnings.txt"), specWarnings(spec)); err != nil {
		return err
	}
	if tavolaOut != "" {
		if err := writeJSON(tavolaOut, spec); err != nil {
			return err
		}
	}
	return nil
}

func writeMissingAuthScenario(root string) error {
	scenario, err := xmeta.LoadContractScenario(xmeta.ContractScenarioMissingAuthTable)
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
	if _, err := xmeta.ExpandRoleScopes(scenario.Meta, app); err == nil {
		return fmt.Errorf("%s expansion succeeded unexpectedly", scenario.Name)
	} else if err := writeText(filepath.Join(root, "xmeta/testdata/contracts/"+scenario.Name+".expand_error.txt"), err.Error()); err != nil {
		return err
	}

	if _, err := tavola.BuildSpec(scenario.Meta, tavolaOptions(scenario)); err == nil {
		return fmt.Errorf("%s Tavola build succeeded unexpectedly", scenario.Name)
	} else if err := writeText(filepath.Join(root, "tavola/testdata/contracts/"+scenario.Name+".tavola_error.txt"), err.Error()); err != nil {
		return err
	}
	for _, stale := range []string{
		"xmeta/testdata/contracts/" + scenario.Name + ".expanded_app_spec.json",
		"tavola/testdata/contracts/" + scenario.Name + ".project.json",
		"tavola/testdata/contracts/" + scenario.Name + ".warnings.txt",
	} {
		if err := os.Remove(filepath.Join(root, stale)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func tavolaOptions(scenario *xmeta.ContractScenario) tavola.Options {
	opts := tavola.Options{
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
		SchemaOverrides:    scenario.SchemaOverrides,
	}
	if auth := scenario.Auth; auth != nil {
		opts.Auth = tavola.AuthOptions{
			Role:      scenario.RoleName,
			Table:     strings.Join(auth.GetUserTable().GetIdents(), "."),
			ID:        auth.GetUserIDColumn(),
			Login:     auth.GetLoginColumn(),
			Password:  auth.GetPasswordColumn(),
			FirstName: auth.GetFirstNameColumn(),
			LastName:  auth.GetLastNameColumn(),
		}
	}
	return opts
}

func writeProtoJSON(path string, msg proto.Message) error {
	data, err := protojson.MarshalOptions{
		Multiline:       false,
		EmitUnpopulated: false,
	}.Marshal(msg)
	if err != nil {
		return err
	}
	var formatted bytes.Buffer
	if err := json.Indent(&formatted, data, "", "  "); err != nil {
		return err
	}
	formatted.WriteByte('\n')
	return writeFile(path, formatted.Bytes())
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

func writeText(path string, text string) error {
	return writeFile(path, []byte(text+"\n"))
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
