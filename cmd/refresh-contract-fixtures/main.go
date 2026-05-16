package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/genelet/sqlmeta/xmeta"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	var root string
	flag.StringVar(&root, "root", ".", "sqlmeta repository root")
	flag.Parse()

	if err := run(root); err != nil {
		fmt.Fprintln(os.Stderr, "refresh-contract-fixtures:", err)
		os.Exit(1)
	}
}

func run(root string) error {
	if err := writeSuccessScenario(root, xmeta.ContractScenarioManualPKFK); err != nil {
		return err
	}
	if err := writeSuccessScenario(root, xmeta.ContractScenarioInvalidOverrides); err != nil {
		return err
	}
	if err := writeMissingAuthScenario(root); err != nil {
		return err
	}
	return nil
}

func writeSuccessScenario(root, name string) error {
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

	if err := writeProtoJSON(filepath.Join(root, "xmeta/testdata/contracts/"+name+".expanded_app_spec.json"), expanded); err != nil {
		return err
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

	for _, stale := range []string{
		"xmeta/testdata/contracts/" + scenario.Name + ".expanded_app_spec.json",
	} {
		if err := os.Remove(filepath.Join(root, stale)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
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

func writeText(path string, text string) error {
	return writeFile(path, []byte(text+"\n"))
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
