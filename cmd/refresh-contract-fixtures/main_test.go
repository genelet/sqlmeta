package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/genelet/sqlmeta/xmeta"
)

func TestRunRefreshesManualPKFKFixtures(t *testing.T) {
	dir := t.TempDir()
	if err := run(dir, ""); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"xmeta/testdata/contracts/" + xmeta.ManualPKFKExpandedFixture,
		"xmeta/testdata/contracts/" + xmeta.InvalidOverridesExpandedFixture,
		"xmeta/testdata/contracts/missing_auth_table.expand_error.txt",
		"tavola/testdata/contracts/manual_pk_fk.project.json",
		"tavola/testdata/contracts/manual_pk_fk.warnings.txt",
		"tavola/testdata/contracts/invalid_overrides.project.json",
		"tavola/testdata/contracts/invalid_overrides.warnings.txt",
		"tavola/testdata/contracts/missing_auth_table.tavola_error.txt",
	} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("missing generated %s: %v", name, err)
		}
		if len(data) == 0 {
			t.Fatalf("%s is empty", name)
		}
	}
	for _, name := range []string{
		"xmeta/testdata/contracts/missing_auth_table.expanded_app_spec.json",
		"tavola/testdata/contracts/missing_auth_table.project.json",
		"tavola/testdata/contracts/missing_auth_table.warnings.txt",
	} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("unexpected generated %s", name)
		}
	}
}
