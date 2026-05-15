package xmeta

import (
	"embed"
	"fmt"
	"io/fs"
	"sort"
)

//go:embed testdata/contracts/*.json
var contractFixtures embed.FS

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
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}
