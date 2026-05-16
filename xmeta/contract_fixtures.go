package xmeta

import (
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed testdata/contracts/*.json testdata/contracts/*.txt
var contractFixtures embed.FS

type ContractArtifactKind string

const (
	ContractArtifactExpandedAppSpec ContractArtifactKind = "expanded_app_spec"
	ContractArtifactExpandError     ContractArtifactKind = "expand_error"
)

type ContractArtifact struct {
	Scenario string               `json:"scenario"`
	Kind     ContractArtifactKind `json:"kind"`
	Path     string               `json:"path"`
	Primary  bool                 `json:"primary,omitempty"`
}

type ContractArtifacts struct {
	Scenario        string               `json:"scenario"`
	PrimaryJSONKind ContractArtifactKind `json:"primaryJsonKind,omitempty"`
	Artifacts       []ContractArtifact   `json:"artifacts"`
}

// ContractFixture returns a copy of a canonical sqlmeta contract fixture.
func ContractFixture(name string) ([]byte, error) {
	data, err := contractFixtures.ReadFile("testdata/contracts/" + name)
	if err != nil {
		return nil, fmt.Errorf("reading contract fixture %q: %w", name, err)
	}
	return append([]byte(nil), data...), nil
}

// ContractFixtureNames lists the canonical sqlmeta contract fixture names.
func ContractFixtureNames() []string {
	entries, err := fs.ReadDir(contractFixtures, "testdata/contracts")
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}

func ContractArtifactBytes(path string) ([]byte, error) {
	path = strings.TrimPrefix(path, "xmeta/testdata/contracts/")
	data, err := contractFixtures.ReadFile("testdata/contracts/" + path)
	if err != nil {
		return nil, fmt.Errorf("reading contract artifact %q: %w", path, err)
	}
	return append([]byte(nil), data...), nil
}

func ContractScenarioArtifacts(name string) (ContractArtifacts, error) {
	if _, err := LoadContractScenario(name); err != nil {
		return ContractArtifacts{}, err
	}
	artifacts := ContractArtifacts{Scenario: name}
	switch name {
	case ContractScenarioManualPKFK, ContractScenarioInvalidOverrides:
		artifacts.PrimaryJSONKind = ContractArtifactExpandedAppSpec
		artifacts.Artifacts = append(artifacts.Artifacts,
			ContractArtifact{
				Scenario: name,
				Kind:     ContractArtifactExpandedAppSpec,
				Path:     "xmeta/testdata/contracts/" + name + ".expanded_app_spec.json",
				Primary:  true,
			},
		)
	case ContractScenarioMissingAuthTable:
		artifacts.Artifacts = append(artifacts.Artifacts,
			ContractArtifact{
				Scenario: name,
				Kind:     ContractArtifactExpandError,
				Path:     "xmeta/testdata/contracts/" + name + ".expand_error.txt",
			},
		)
	}
	return artifacts, nil
}
