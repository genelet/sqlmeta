package tavola

import (
	"fmt"
	"strings"
)

func ValidateSpec(spec *Spec) error {
	if spec == nil {
		return fmt.Errorf("spec is nil")
	}
	if spec.Version != 1 {
		return fmt.Errorf("spec version must be 1")
	}
	if strings.TrimSpace(spec.Owner.Login) == "" {
		return fmt.Errorf("owner.login is required")
	}
	if strings.TrimSpace(spec.Owner.Email) == "" {
		return fmt.Errorf("owner.email is required")
	}
	if spec.Owner.TypeID == 0 {
		return fmt.Errorf("owner.typeid is required")
	}
	if strings.TrimSpace(spec.Project.Name) == "" {
		return fmt.Errorf("project.name is required")
	}
	if strings.TrimSpace(spec.Project.Script) == "" {
		return fmt.Errorf("project.script is required")
	}
	if strings.TrimSpace(spec.Project.PublicRole) == "" {
		return fmt.Errorf("project.publicRole is required")
	}
	if strings.TrimSpace(spec.Project.Default.Component) == "" {
		return fmt.Errorf("project.default.component is required")
	}
	if strings.TrimSpace(spec.Project.Default.Action) == "" {
		return fmt.Errorf("project.default.action is required")
	}
	if err := validateDatasource(spec.Datasource); err != nil {
		return err
	}
	if spec.Overlays == nil {
		return fmt.Errorf("overlays is required")
	}

	tables := map[string]bool{}
	for _, table := range spec.Schema.Tables {
		if strings.TrimSpace(table.Name) == "" {
			return fmt.Errorf("schema table name is required")
		}
		if strings.TrimSpace(table.PrimaryKey) == "" {
			return fmt.Errorf("table %s primaryKey is required", table.Name)
		}
		if strings.TrimSpace(table.Statement) == "" {
			return fmt.Errorf("table %s statement is required", table.Name)
		}
		tables[table.Name] = true
	}
	if len(tables) == 0 {
		return fmt.Errorf("schema.tables must contain at least one table")
	}

	for _, procedure := range spec.Schema.Procedures {
		if strings.TrimSpace(procedure.Name) == "" {
			return fmt.Errorf("schema procedure name is required")
		}
		if strings.TrimSpace(procedure.Statement) == "" {
			return fmt.Errorf("procedure %s statement is required", procedure.Name)
		}
		if procedure.Table != "" && !tables[procedure.Table] {
			return fmt.Errorf("procedure %s references unknown table %q", procedure.Name, procedure.Table)
		}
	}

	roles := map[string]bool{}
	for _, role := range spec.Roles {
		if strings.TrimSpace(role.Name) == "" {
			return fmt.Errorf("role name is required")
		}
		if strings.TrimSpace(role.Description) == "" {
			return fmt.Errorf("role %s description is required", role.Name)
		}
		if strings.TrimSpace(role.Authen) == "" {
			return fmt.Errorf("role %s authen is required", role.Name)
		}
		if strings.TrimSpace(role.Restriction) == "" {
			return fmt.Errorf("role %s restriction is required", role.Name)
		}
		if strings.TrimSpace(role.Fields.ID) == "" || strings.TrimSpace(role.Fields.Login) == "" || strings.TrimSpace(role.Fields.Password) == "" {
			return fmt.Errorf("role %s fields.id, fields.login, and fields.password are required", role.Name)
		}
		if role.Table != "" && !tables[role.Table] {
			return fmt.Errorf("role %s references unknown table %q", role.Name, role.Table)
		}
		roles[role.Name] = true
	}

	for _, component := range spec.Components {
		if strings.TrimSpace(component.Name) == "" {
			return fmt.Errorf("component name is required")
		}
		if strings.TrimSpace(component.Description) == "" {
			return fmt.Errorf("component %s description is required", component.Name)
		}
		if !tables[component.Table] {
			return fmt.Errorf("component %s references unknown table %q", component.Name, component.Table)
		}
		for role := range component.Roles {
			if !roles[role] {
				return fmt.Errorf("component %s references unknown role %q", component.Name, role)
			}
		}
	}
	if len(spec.Components) == 0 {
		return fmt.Errorf("components must contain at least one component")
	}
	return nil
}

func validateDatasource(ds Datasource) error {
	if strings.TrimSpace(ds.Type) == "" {
		return fmt.Errorf("datasource.type is required")
	}
	if strings.TrimSpace(ds.Nickname) == "" {
		return fmt.Errorf("datasource.nickname is required")
	}
	family := datasourceFamily(ds.Type)
	if family == "unknown" {
		return fmt.Errorf("unsupported datasource type %q", ds.Type)
	}
	if family == "sqlite" {
		if strings.TrimSpace(ds.Database) == "" && strings.TrimSpace(ds.Path) == "" {
			return fmt.Errorf("sqlite datasource needs database or path")
		}
		return nil
	}
	if strings.TrimSpace(ds.Database) == "" || strings.TrimSpace(ds.Host) == "" || strings.TrimSpace(ds.Port) == "" || strings.TrimSpace(ds.User) == "" || strings.TrimSpace(ds.Password) == "" {
		return fmt.Errorf("%s datasource needs database, host, port, user, and password", ds.Type)
	}
	return nil
}

func datasourceFamily(value string) string {
	normalized := strings.ToLower(value)
	normalized = strings.NewReplacer("-", "", "_", "", " ", "").Replace(normalized)
	switch normalized {
	case "mysql", "mariadb":
		return "mysql"
	case "postgresql", "postgres", "pgsql":
		return "postgresql"
	case "sqlite", "sqlite3":
		return "sqlite"
	default:
		return "unknown"
	}
}
