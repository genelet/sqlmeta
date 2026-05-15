package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRunRefreshesManualPKFKFixtures(t *testing.T) {
	dir := t.TempDir()
	if err := run(dir, ""); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"xmeta/testdata/contracts/manual_pk_fk.expanded_app_spec.json",
		"tavola/testdata/contracts/manual_pk_fk.project.json",
		"tavola/testdata/contracts/manual_pk_fk.warnings.txt",
	} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("missing generated %s: %v", name, err)
		}
		if len(data) == 0 {
			t.Fatalf("%s is empty", name)
		}
	}
}
