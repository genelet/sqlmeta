package xmeta

import (
	"fmt"
	"strings"
)

type manualPrimaryKeyOverride struct {
	key         string
	columns     []string
	warnings    []string
	diagnostics []Diagnostic
	ok          bool
}

type manualForeignKeyOverride struct {
	childKey    string
	parentKey   string
	childCols   []string
	parentCols  []string
	warnings    []string
	diagnostics []Diagnostic
	ok          bool
}

func resolveManualPrimaryKeyOverride(index appTableIndex, pk *ManualPrimaryKey) manualPrimaryKeyOverride {
	out := manualPrimaryKeyOverride{}
	key, warning := index.resolve(pk.GetTableName())
	if warning != "" {
		out.add(manualPKTableDiagnosticCode(warning), fmt.Sprintf("manual primary key %s target %q: %s", pk.GetName(), objectNameKey(pk.GetTableName()), warning))
	}
	if key == "" {
		return out
	}
	cols := cleanColumns(pk.GetColumns())
	if len(cols) == 0 {
		out.add(DiagnosticManualPKEmptyColumns, fmt.Sprintf("manual primary key %s on %s has no columns", pk.GetName(), key))
		return out
	}
	if missing := missingColumns(index.byKey[key], cols); len(missing) > 0 {
		for _, col := range missing {
			out.add(DiagnosticManualPKMissingColumn, fmt.Sprintf("manual primary key %s on %s references missing column %s", pk.GetName(), key, col))
		}
		return out
	}
	out.key = key
	out.columns = cols
	out.ok = true
	return out
}

func resolveManualForeignKeyOverride(index appTableIndex, primaryKeys map[string][]string, fk *ManualForeignKey, compositeContext string) manualForeignKeyOverride {
	out := manualForeignKeyOverride{}
	childKey, childWarning := index.resolve(fk.GetChildTable())
	parentKey, parentWarning := index.resolve(fk.GetParentTable())
	if childWarning != "" {
		out.add(manualFKTableDiagnosticCode(childWarning), fmt.Sprintf("manual foreign key %s child table %q: %s", fk.GetName(), objectNameKey(fk.GetChildTable()), childWarning))
	}
	if parentWarning != "" {
		out.add(manualFKTableDiagnosticCode(parentWarning), fmt.Sprintf("manual foreign key %s parent table %q: %s", fk.GetName(), objectNameKey(fk.GetParentTable()), parentWarning))
	}
	if childKey == "" || parentKey == "" {
		return out
	}

	childCols := cleanColumns(fk.GetChildColumns())
	parentCols := cleanColumns(fk.GetParentColumns())
	if len(parentCols) == 0 {
		parentCols = primaryKeys[parentKey]
	}
	if len(childCols) == 0 {
		out.add(DiagnosticManualFKEmptyChildColumns, fmt.Sprintf("manual foreign key %s on %s has no child columns", fk.GetName(), childKey))
		return out
	}
	if len(parentCols) == 0 {
		out.add(DiagnosticManualFKMissingParentPK, fmt.Sprintf("manual foreign key %s on %s has no parent columns and %s has no primary key", fk.GetName(), childKey, parentKey))
		return out
	}
	if len(childCols) != 1 || len(parentCols) != 1 {
		out.add(DiagnosticManualFKComposite, fmt.Sprintf("manual foreign key %s on %s has composite columns; skipped %s edge", fk.GetName(), childKey, compositeContext))
		return out
	}
	if missing := missingColumns(index.byKey[childKey], childCols); len(missing) > 0 {
		for _, col := range missing {
			out.add(DiagnosticManualFKMissingChildColumn, fmt.Sprintf("manual foreign key %s on %s references missing child column %s", fk.GetName(), childKey, col))
		}
		return out
	}
	if missing := missingColumns(index.byKey[parentKey], parentCols); len(missing) > 0 {
		for _, col := range missing {
			out.add(DiagnosticManualFKMissingParentColumn, fmt.Sprintf("manual foreign key %s on %s references missing parent column %s.%s", fk.GetName(), childKey, parentKey, col))
		}
		return out
	}

	out.childKey = childKey
	out.parentKey = parentKey
	out.childCols = childCols
	out.parentCols = parentCols
	out.ok = true
	return out
}

func (o *manualPrimaryKeyOverride) add(code, message string) {
	o.warnings = append(o.warnings, message)
	o.diagnostics = append(o.diagnostics, WarningDiagnostic(code, message))
}

func (o *manualForeignKeyOverride) add(code, message string) {
	o.warnings = append(o.warnings, message)
	o.diagnostics = append(o.diagnostics, WarningDiagnostic(code, message))
}

func manualPKTableDiagnosticCode(warning string) string {
	if strings.Contains(warning, "ambiguous table name") {
		return DiagnosticManualPKAmbiguousTable
	}
	return DiagnosticManualPKMissingTable
}

func manualFKTableDiagnosticCode(warning string) string {
	if strings.Contains(warning, "ambiguous table name") {
		return DiagnosticManualFKAmbiguousTable
	}
	return DiagnosticManualFKMissingTable
}
