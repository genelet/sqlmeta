package xmeta

import "strings"

const (
	DiagnosticSeverityWarning = "warning"
	DiagnosticSeverityError   = "error"
)

const (
	DiagnosticUnknown                     = "unknown"
	DiagnosticAuthMissingLoginProcedure   = "auth_missing_login_procedure"
	DiagnosticAuthTableMissing            = "auth_table_missing"
	DiagnosticAuthTableAmbiguous          = "auth_table_ambiguous"
	DiagnosticAuthUserIDMissing           = "auth_user_id_missing"
	DiagnosticManualPKMissingTable        = "manual_pk_missing_table"
	DiagnosticManualPKAmbiguousTable      = "manual_pk_ambiguous_table"
	DiagnosticManualPKMissingColumn       = "manual_pk_missing_column"
	DiagnosticManualPKEmptyColumns        = "manual_pk_empty_columns"
	DiagnosticManualFKMissingTable        = "manual_fk_missing_table"
	DiagnosticManualFKAmbiguousTable      = "manual_fk_ambiguous_table"
	DiagnosticManualFKMissingChildColumn  = "manual_fk_missing_child_column"
	DiagnosticManualFKMissingParentColumn = "manual_fk_missing_parent_column"
	DiagnosticManualFKEmptyChildColumns   = "manual_fk_empty_child_columns"
	DiagnosticManualFKMissingParentPK     = "manual_fk_missing_parent_pk"
	DiagnosticManualFKComposite           = "manual_fk_composite"
	DiagnosticRoleFKComposite             = "role_fk_composite"
	DiagnosticRoleFKAmbiguousTable        = "role_fk_ambiguous_table"
	DiagnosticRoleFKMissingTarget         = "role_fk_missing_target"
	DiagnosticRoleFKCycle                 = "role_fk_cycle"
	DiagnosticRoleFKNoChildren            = "role_fk_no_children"
	DiagnosticRoleTableMissing            = "role_table_missing"
	DiagnosticRoleTableAmbiguous          = "role_table_ambiguous"
	DiagnosticTableSynthesizedDDL         = "table_synthesized_ddl"
	DiagnosticTableManualPK               = "table_manual_primary_key"
	DiagnosticTableMissingPK              = "table_missing_primary_key"
	DiagnosticTableCompositePK            = "table_composite_primary_key"
	DiagnosticComponentSkipped            = "component_skipped"
	DiagnosticUnsupportedFKTraversal      = "unsupported_fk_traversal"
)

// Diagnostic is the stable machine-readable counterpart to a human warning or
// error message. Existing JSON surfaces keep the original message strings.
type Diagnostic struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
}

func WarningDiagnostic(code, message string) Diagnostic {
	message = strings.TrimSpace(message)
	if code == "" {
		code = DiagnosticCode(message)
	}
	return Diagnostic{
		Code:     code,
		Severity: DiagnosticSeverityWarning,
		Message:  message,
	}
}

func WarningDiagnostics(messages []string) []Diagnostic {
	out := make([]Diagnostic, 0, len(messages))
	for _, message := range messages {
		message = strings.TrimSpace(message)
		if message == "" {
			continue
		}
		out = append(out, WarningDiagnostic("", message))
	}
	return out
}

func DiagnosticMessages(diagnostics []Diagnostic) []string {
	out := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		message := strings.TrimSpace(diagnostic.Message)
		if message == "" {
			continue
		}
		out = append(out, message)
	}
	return uniqueAppStrings(out)
}

func MergeWarningDiagnostics(primary []Diagnostic, fallbackMessages []string) []Diagnostic {
	seen := map[string]bool{}
	out := make([]Diagnostic, 0, len(primary)+len(fallbackMessages))
	for _, diagnostic := range primary {
		message := strings.TrimSpace(diagnostic.Message)
		if message == "" || seen[message] {
			continue
		}
		if diagnostic.Code == "" {
			diagnostic.Code = DiagnosticCode(message)
		}
		if diagnostic.Severity == "" {
			diagnostic.Severity = DiagnosticSeverityWarning
		}
		diagnostic.Message = message
		out = append(out, diagnostic)
		seen[message] = true
	}
	for _, diagnostic := range WarningDiagnostics(fallbackMessages) {
		if seen[diagnostic.Message] {
			continue
		}
		out = append(out, diagnostic)
		seen[diagnostic.Message] = true
	}
	return out
}

func uniqueDiagnostics(diagnostics []Diagnostic) []Diagnostic {
	return MergeWarningDiagnostics(diagnostics, nil)
}

func ErrorDiagnostic(err error) Diagnostic {
	message := ""
	if err != nil {
		message = err.Error()
	}
	return Diagnostic{
		Code:     DiagnosticCode(message),
		Severity: DiagnosticSeverityError,
		Message:  message,
	}
}

func DiagnosticCode(message string) string {
	m := strings.ToLower(message)
	switch {
	case strings.Contains(m, "without a login procedure"):
		return DiagnosticAuthMissingLoginProcedure
	case (strings.Contains(m, "auth user table") || strings.Contains(m, "auth table")) && strings.Contains(m, "required but was not found"):
		return DiagnosticAuthTableMissing
	case strings.Contains(m, "auth table") && strings.Contains(m, "ambiguous table name"):
		return DiagnosticAuthTableAmbiguous
	case strings.Contains(m, "has no user id column") || strings.Contains(m, "auth binding has no user id column"):
		return DiagnosticAuthUserIDMissing
	case strings.Contains(m, "manual primary key") && strings.Contains(m, "ambiguous table name"):
		return DiagnosticManualPKAmbiguousTable
	case strings.Contains(m, "manual primary key") && strings.Contains(m, "table was not found"):
		return DiagnosticManualPKMissingTable
	case strings.Contains(m, "manual primary key") && strings.Contains(m, "references missing column"):
		return DiagnosticManualPKMissingColumn
	case strings.Contains(m, "manual primary key") && strings.Contains(m, "has no columns"):
		return DiagnosticManualPKEmptyColumns
	case strings.Contains(m, "manual foreign key") && strings.Contains(m, "ambiguous table name"):
		return DiagnosticManualFKAmbiguousTable
	case strings.Contains(m, "manual foreign key") && strings.Contains(m, "table was not found"):
		return DiagnosticManualFKMissingTable
	case strings.Contains(m, "manual foreign key") && strings.Contains(m, "references missing child column"):
		return DiagnosticManualFKMissingChildColumn
	case strings.Contains(m, "manual foreign key") && strings.Contains(m, "references missing parent column"):
		return DiagnosticManualFKMissingParentColumn
	case strings.Contains(m, "manual foreign key") && strings.Contains(m, "has no child columns"):
		return DiagnosticManualFKEmptyChildColumns
	case strings.Contains(m, "manual foreign key") && strings.Contains(m, "has no parent columns"):
		return DiagnosticManualFKMissingParentPK
	case strings.Contains(m, "manual foreign key") && strings.Contains(m, "composite columns"):
		return DiagnosticManualFKComposite
	case strings.Contains(m, "has composite") && strings.Contains(m, "fk") && strings.Contains(m, "skipped role scope edge"):
		return DiagnosticRoleFKComposite
	case strings.Contains(m, "fk target") && strings.Contains(m, "ambiguous table name"):
		return DiagnosticRoleFKAmbiguousTable
	case strings.Contains(m, "fk target") && strings.Contains(m, "table was not found"):
		return DiagnosticRoleFKMissingTarget
	case strings.Contains(m, "skipped cycle"):
		return DiagnosticRoleFKCycle
	case strings.Contains(m, "found no child foreign keys"):
		return DiagnosticRoleFKNoChildren
	case (strings.Contains(m, "include table") || strings.Contains(m, "exclude table")) && strings.Contains(m, "ambiguous table name"):
		return DiagnosticRoleTableAmbiguous
	case (strings.Contains(m, "include table") || strings.Contains(m, "exclude table")) && strings.Contains(m, "table was not found"):
		return DiagnosticRoleTableMissing
	case strings.Contains(m, "uses synthesized create table statement"):
		return DiagnosticTableSynthesizedDDL
	case strings.Contains(m, "uses manual primary key override"):
		return DiagnosticTableManualPK
	case strings.Contains(m, "has no primary key"):
		return DiagnosticTableMissingPK
	case strings.Contains(m, "has composite primary key"):
		return DiagnosticTableCompositePK
	case strings.Contains(m, "component") && strings.Contains(m, "skipped"):
		return DiagnosticComponentSkipped
	case strings.Contains(m, "unsupported fk traversal"):
		return DiagnosticUnsupportedFKTraversal
	default:
		return DiagnosticUnknown
	}
}
