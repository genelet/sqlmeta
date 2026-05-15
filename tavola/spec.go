package tavola

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/genelet/sqlmeta/xmeta"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type Options struct {
	Project            string
	Script             string
	PublicRole         string
	OwnerLogin         string
	OwnerEmail         string
	OwnerTypeID        int
	DatasourceType     string
	DatasourceNickname string
	DatasourceDatabase string
	DatasourceHost     string
	DatasourcePort     string
	DatasourceUser     string
	DatasourcePassword string
	DatasourcePath     string
	Auth               AuthOptions
	SchemaOverrides    *xmeta.SchemaRelationshipOverrides
}

type AuthOptions struct {
	Role              string
	Table             string
	ID                string
	Login             string
	Password          string
	FirstName         string
	LastName          string
	Description       string
	Restriction       string
	FallbackAllTables bool
}

type Spec struct {
	Version       int            `json:"version"`
	Owner         Owner          `json:"owner"`
	Project       Project        `json:"project"`
	Datasource    Datasource     `json:"datasource"`
	Schema        Schema         `json:"schema"`
	Roles         []Role         `json:"roles"`
	Components    []Component    `json:"components"`
	Overlays      map[string]any `json:"overlays"`
	Introspection *Introspection `json:"introspection,omitempty"`
}

type Owner struct {
	Login  string `json:"login"`
	Email  string `json:"email"`
	TypeID int    `json:"typeid"`
}

type Project struct {
	Name       string         `json:"name"`
	Script     string         `json:"script"`
	PublicRole string         `json:"publicRole"`
	Default    ProjectDefault `json:"default"`
}

type ProjectDefault struct {
	Component string `json:"component"`
	Action    string `json:"action"`
}

type Datasource struct {
	Type     string `json:"type"`
	Nickname string `json:"nickname"`
	Database string `json:"database,omitempty"`
	Path     string `json:"path,omitempty"`
	Host     string `json:"host,omitempty"`
	Port     string `json:"port,omitempty"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
}

type Schema struct {
	Tables     []Table     `json:"tables"`
	Procedures []Procedure `json:"procedures"`
}

type Table struct {
	Name       string   `json:"name"`
	PrimaryKey string   `json:"primaryKey"`
	AutoKey    string   `json:"autoKey,omitempty"`
	Statement  string   `json:"statement"`
	Insert     []string `json:"insert"`
	Edit       []string `json:"edit"`
	Update     []string `json:"update"`
	Topics     []string `json:"topics"`
	Comment    string   `json:"comment,omitempty"`
}

type Procedure struct {
	Name      string `json:"name"`
	Table     string `json:"table,omitempty"`
	Statement string `json:"statement"`
}

type Role struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Authen      string         `json:"authen"`
	IsAdmin     int            `json:"isAdmin"`
	IsAuto      int            `json:"isAuto"`
	Table       string         `json:"table,omitempty"`
	Default     ProjectDefault `json:"default"`
	Fields      RoleFields     `json:"fields"`
	Restriction string         `json:"restriction"`
}

type RoleFields struct {
	ID        string `json:"id"`
	Login     string `json:"login"`
	Password  string `json:"password"`
	FirstName string `json:"firstname,omitempty"`
	LastName  string `json:"lastname,omitempty"`
}

type Component struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	Table       string              `json:"table"`
	Public      []string            `json:"public,omitempty"`
	Roles       map[string][]string `json:"roles,omitempty"`
}

type Introspection struct {
	Source   string   `json:"source"`
	Warnings []string `json:"warnings,omitempty"`
}

func BuildSpec(meta *xmeta.MetaDatabase, opts Options) (*Spec, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata database is nil")
	}
	if strings.TrimSpace(opts.Project) == "" {
		return nil, fmt.Errorf("project name is required")
	}
	appOpts := xmeta.AppSpecOptions{
		Name:               opts.Project,
		DatasourceName:     opts.DatasourceNickname,
		DatasourceDriver:   normalizeDriver(meta.GetOptions()["Driver"], opts.DatasourceType),
		DatasourceDatabase: defaultString(opts.DatasourceDatabase, meta.GetName()),
	}
	if hasAuth(opts.Auth) {
		if err := validateAuth(opts.Auth); err != nil {
			return nil, err
		}
		appOpts.Auth = &xmeta.AuthBinding{
			UserTable:       xmeta.ObjectNameFromString(opts.Auth.Table),
			UserIDColumn:    opts.Auth.ID,
			LoginColumn:     opts.Auth.Login,
			PasswordColumn:  opts.Auth.Password,
			FirstNameColumn: opts.Auth.FirstName,
			LastNameColumn:  opts.Auth.LastName,
			Options:         map[string]string{"Restriction": opts.Auth.Restriction},
		}
		appOpts.RoleName = opts.Auth.Role
		appOpts.RoleDescription = opts.Auth.Description
		appOpts.FallbackAllTables = opts.Auth.FallbackAllTables
	}
	appOpts.SchemaOverrides = opts.SchemaOverrides
	app, err := xmeta.BuildDefaultAppSpec(meta, appOpts)
	if err != nil {
		return nil, err
	}
	expanded, err := xmeta.ExpandRoleScopes(meta, app)
	if err != nil {
		return nil, err
	}
	return BuildTavolaSpec(meta, expanded, opts)
}

func BuildTavolaSpec(meta *xmeta.MetaDatabase, expanded *xmeta.ExpandedAppSpec, opts Options) (*Spec, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata database is nil")
	}
	if expanded == nil || expanded.GetSpec() == nil {
		return nil, fmt.Errorf("expanded app spec is required")
	}
	app := expanded.GetSpec()
	projectName := defaultString(opts.Project, app.GetName())
	if strings.TrimSpace(projectName) == "" {
		return nil, fmt.Errorf("project name is required")
	}

	warnings := []string{}
	driver := normalizeDriver(meta.GetOptions()["Driver"], opts.DatasourceType)
	publicRole := defaultString(opts.PublicRole, "p")

	spec := &Spec{
		Version: 1,
		Owner: Owner{
			Login:  defaultString(opts.OwnerLogin, "local"),
			Email:  defaultString(opts.OwnerEmail, "local@example.test"),
			TypeID: defaultInt(opts.OwnerTypeID, 1),
		},
		Project: Project{
			Name:       projectName,
			Script:     defaultString(opts.Script, "/"+safeName(projectName)+"/app.php"),
			PublicRole: publicRole,
			Default:    ProjectDefault{Action: "topics"},
		},
		Datasource: datasource(meta, driver, opts),
		Schema:     Schema{Procedures: []Procedure{}},
		Roles:      []Role{},
		Components: []Component{},
		Overlays:   map[string]any{},
		Introspection: &Introspection{
			Source: "sqlmeta",
		},
	}

	pkOverrides, pkWarnings := manualPrimaryKeyOverrides(meta, app.GetSchemaOverrides())
	warnings = append(warnings, pkWarnings...)
	tablesByName := map[string]Table{}
	tableAvailable := map[string]bool{}
	for _, mt := range meta.GetTables() {
		table, warningList, ok := buildTable(mt, driver, pkOverrides[tableName(mt.GetName())])
		warnings = append(warnings, warningList...)
		if !ok {
			continue
		}
		spec.Schema.Tables = append(spec.Schema.Tables, table)
		tablesByName[table.Name] = table
		tableAvailable[table.Name] = true
	}

	if len(spec.Schema.Tables) == 0 {
		return nil, fmt.Errorf("no introspectable tables found")
	}

	grantsByComponent := map[string]map[string][]string{}
	for _, grant := range expanded.GetComponentGrants() {
		if grantsByComponent[grant.GetComponentName()] == nil {
			grantsByComponent[grant.GetComponentName()] = map[string][]string{}
		}
		grantsByComponent[grant.GetComponentName()][grant.GetRoleName()] = tavolaActions(grant.GetOperations(), false)
	}

	for _, appComponent := range app.GetComponents() {
		table := tableName(appComponent.GetTableName())
		if !tableAvailable[table] {
			warnings = append(warnings, fmt.Sprintf("component %s skipped because table %s is not introspectable", appComponent.GetName(), table))
			continue
		}
		component := Component{
			Name:        appComponent.GetName(),
			Description: appComponent.GetDescription(),
			Table:       table,
			Public:      tavolaActions(appComponent.GetPublicOperations(), true),
		}
		if len(grantsByComponent[component.Name]) > 0 {
			component.Roles = grantsByComponent[component.Name]
		}
		spec.Components = append(spec.Components, component)
		if spec.Project.Default.Component == "" {
			spec.Project.Default.Component = component.Name
		}
	}

	for _, appRole := range app.GetRoles() {
		role, warningList, err := buildRoleFromApp(appRole, spec.Project.Default, tablesByName)
		if err != nil {
			return nil, err
		}
		warnings = append(warnings, warningList...)
		spec.Roles = append(spec.Roles, role)
	}

	sort.Slice(spec.Schema.Tables, func(i, j int) bool { return spec.Schema.Tables[i].Name < spec.Schema.Tables[j].Name })
	sort.Slice(spec.Components, func(i, j int) bool { return spec.Components[i].Name < spec.Components[j].Name })
	sort.Slice(spec.Roles, func(i, j int) bool { return spec.Roles[i].Name < spec.Roles[j].Name })
	warnings = append(warnings, expanded.GetWarnings()...)
	spec.Introspection.Warnings = unique(warnings)
	if len(warnings) == 0 {
		spec.Introspection = &Introspection{Source: "sqlmeta"}
	}
	return spec, nil
}

func buildTable(mt *xmeta.MetaTable, driver string, primaryKeyOverride []string) (Table, []string, bool) {
	var warnings []string
	name := tableName(mt.GetName())
	cols := columns(mt)
	if len(cols) == 0 {
		return Table{}, []string{fmt.Sprintf("skipped %s: table has no columns", name)}, false
	}

	pks := primaryKeys(mt)
	if len(primaryKeyOverride) > 0 {
		pks = primaryKeyOverride
		warnings = append(warnings, fmt.Sprintf("%s uses manual primary key override (%s)", name, strings.Join(primaryKeyOverride, ", ")))
	}
	primaryKey := ""
	if len(pks) == 1 {
		primaryKey = pks[0]
	} else if len(pks) > 1 {
		primaryKey = pks[0]
		warnings = append(warnings, fmt.Sprintf("%s has composite primary key (%s); using %s as Tavola primaryKey", name, strings.Join(pks, ", "), primaryKey))
	} else {
		primaryKey = cols[0].Name
		warnings = append(warnings, fmt.Sprintf("%s has no primary key; using first column %s as Tavola primaryKey", name, primaryKey))
	}

	autoKey := ""
	insert := []string{}
	edit := []string{}
	update := []string{primaryKey}
	topics := []string{}
	for _, col := range cols {
		edit = append(edit, col.Name)
		topics = append(topics, col.Name)
		if isAutoColumn(col) {
			if autoKey == "" {
				autoKey = col.Name
			}
			continue
		}
		if isGeneratedColumn(col) {
			continue
		}
		insert = append(insert, col.Name)
		if col.Name != primaryKey {
			update = append(update, col.Name)
		}
	}

	statement := strings.TrimSpace(mt.GetOptions()["CreateStatement"])
	if statement == "" {
		statement = renderCreateTable(mt, driver)
		warnings = append(warnings, fmt.Sprintf("%s uses synthesized CREATE TABLE statement", name))
	}

	return Table{
		Name:       name,
		PrimaryKey: primaryKey,
		AutoKey:    autoKey,
		Statement:  strings.TrimSuffix(statement, ";"),
		Insert:     insert,
		Edit:       edit,
		Update:     unique(update),
		Topics:     topics,
		Comment:    mt.GetComment(),
	}, warnings, true
}

func manualPrimaryKeyOverrides(meta *xmeta.MetaDatabase, overrides *xmeta.SchemaRelationshipOverrides) (map[string][]string, []string) {
	out := map[string][]string{}
	var warnings []string
	tableNames := map[string]bool{}
	tableColumns := map[string]map[string]bool{}
	for _, mt := range meta.GetTables() {
		table := tableName(mt.GetName())
		tableNames[table] = true
		tableColumns[table] = map[string]bool{}
		for _, col := range columns(mt) {
			tableColumns[table][strings.ToLower(col.GetName())] = true
		}
	}
	for _, pk := range overrides.GetPrimaryKeys() {
		table := tableName(pk.GetTableName())
		if !tableNames[table] {
			table = resolveTableNameOnly(table, tableNames)
		}
		if !tableNames[table] {
			warnings = append(warnings, fmt.Sprintf("manual primary key %s target %q was not found in introspected tables", pk.GetName(), tableName(pk.GetTableName())))
			continue
		}
		var cols []string
		for _, col := range pk.GetColumns() {
			col = strings.TrimSpace(col)
			if col != "" {
				cols = append(cols, col)
			}
		}
		if len(cols) == 0 {
			warnings = append(warnings, fmt.Sprintf("manual primary key %s on %s has no columns", pk.GetName(), table))
			continue
		}
		missingColumn := false
		for _, col := range cols {
			if !tableColumns[table][strings.ToLower(col)] {
				warnings = append(warnings, fmt.Sprintf("manual primary key %s on %s references missing column %s", pk.GetName(), table, col))
				missingColumn = true
			}
		}
		if missingColumn {
			continue
		}
		out[table] = cols
	}
	return out, warnings
}

func buildRole(auth AuthOptions, def ProjectDefault, tables map[string]Table) (Role, []string, error) {
	var warnings []string
	roleName := defaultString(auth.Role, "u")
	table := resolveTable(auth.Table, tables)
	if _, ok := tables[table]; !ok {
		return Role{}, warnings, fmt.Errorf("auth table %q is required but was not found in introspected tables", table)
	}
	warnings = append(warnings, "auth role was generated without a login procedure; review schema.procedures before relying on login")
	return Role{
		Name:        roleName,
		Description: defaultString(auth.Description, "Signed-in user"),
		Authen:      "db",
		IsAdmin:     0,
		IsAuto:      1,
		Table:       table,
		Default:     def,
		Fields: RoleFields{
			ID:        auth.ID,
			Login:     auth.Login,
			Password:  auth.Password,
			FirstName: auth.FirstName,
			LastName:  auth.LastName,
		},
		Restriction: defaultString(auth.Restriction, "1=1"),
	}, warnings, nil
}

func buildRoleFromApp(appRole *xmeta.AppRole, def ProjectDefault, tables map[string]Table) (Role, []string, error) {
	auth := appRole.GetAuth()
	if auth == nil || len(auth.GetUserTable().GetIdents()) == 0 {
		return Role{}, nil, fmt.Errorf("role %s has no auth user table", appRole.GetName())
	}
	opts := auth.GetOptions()
	return buildRole(AuthOptions{
		Role:        appRole.GetName(),
		Table:       tableName(auth.GetUserTable()),
		ID:          auth.GetUserIDColumn(),
		Login:       auth.GetLoginColumn(),
		Password:    auth.GetPasswordColumn(),
		FirstName:   auth.GetFirstNameColumn(),
		LastName:    auth.GetLastNameColumn(),
		Description: appRole.GetDescription(),
		Restriction: opts["Restriction"],
	}, def, tables)
}

func tavolaActions(ops []xmeta.CRUDOperation, public bool) []string {
	has := map[xmeta.CRUDOperation]bool{}
	for _, op := range ops {
		has[op] = true
	}
	if len(has) == 0 {
		if public {
			has[xmeta.CRUDOperation_CRUDOperationList] = true
		} else {
			has[xmeta.CRUDOperation_CRUDOperationList] = true
			has[xmeta.CRUDOperation_CRUDOperationRead] = true
			has[xmeta.CRUDOperation_CRUDOperationCreate] = true
			has[xmeta.CRUDOperation_CRUDOperationUpdate] = true
			has[xmeta.CRUDOperation_CRUDOperationDelete] = true
		}
	}
	var actions []string
	if has[xmeta.CRUDOperation_CRUDOperationCreate] {
		actions = append(actions, "startnew", "insert")
	}
	if has[xmeta.CRUDOperation_CRUDOperationUpdate] {
		actions = append(actions, "edit", "update")
	}
	if has[xmeta.CRUDOperation_CRUDOperationDelete] {
		actions = append(actions, "delete")
	}
	if has[xmeta.CRUDOperation_CRUDOperationList] || has[xmeta.CRUDOperation_CRUDOperationRead] {
		actions = append(actions, "topics")
	}
	return unique(actions)
}

func renderCreateTable(mt *xmeta.MetaTable, driver string) string {
	var lines []string
	for _, col := range columns(mt) {
		line := quoteIdent(col.Name, driver) + " " + sqlType(col.GetDataType(), driver)
		if defaultExpr := anyToString(col.GetDefault()); defaultExpr != "" {
			line += " DEFAULT " + defaultExpr
		}
		if hasNotNull(col) {
			line += " NOT NULL"
		}
		if isAutoColumn(col) {
			switch driver {
			case "mysql":
				line += " AUTO_INCREMENT"
			}
		}
		lines = append(lines, line)
	}
	for _, constraint := range constraints(mt) {
		if unique := constraint.GetSpec().GetUniqueItem(); unique != nil && len(unique.Columns) > 0 {
			kind := "UNIQUE"
			if unique.IsPrimary {
				kind = "PRIMARY KEY"
			}
			lines = append(lines, kind+" ("+quoteIdents(unique.Columns, driver)+")")
		}
		if ref := constraint.GetSpec().GetReferenceItem(); ref != nil && ref.KeyExpr != nil && len(ref.Columns) > 0 {
			line := "FOREIGN KEY (" + quoteIdents(ref.Columns, driver) + ") REFERENCES " + quoteTable(referenceKeyTableName(ref.KeyExpr), driver)
			if len(ref.KeyExpr.Columns) > 0 {
				line += " (" + quoteIdents(ref.KeyExpr.Columns, driver) + ")"
			}
			if action := referentialAction(ref.OnDelete); action != "" {
				line += " ON DELETE " + action
			}
			if action := referentialAction(ref.OnUpdate); action != "" {
				line += " ON UPDATE " + action
			}
			lines = append(lines, line)
		}
	}
	return "CREATE TABLE " + quoteTable(tableName(mt.GetName()), driver) + " (\n  " + strings.Join(lines, ",\n  ") + "\n)"
}

func datasource(meta *xmeta.MetaDatabase, driver string, opts Options) Datasource {
	ds := Datasource{
		Type:     tavolaDriverName(driver),
		Nickname: defaultString(opts.DatasourceNickname, defaultString(meta.GetName(), "default")),
	}
	if opts.DatasourceType != "" {
		ds.Type = opts.DatasourceType
	}
	if driver == "sqlite" {
		ds.Path = opts.DatasourcePath
		ds.Database = defaultString(opts.DatasourceDatabase, meta.GetName())
		return ds
	}
	ds.Database = defaultString(opts.DatasourceDatabase, meta.GetName())
	ds.Host = defaultString(opts.DatasourceHost, "127.0.0.1")
	ds.Port = defaultString(opts.DatasourcePort, defaultPort(driver))
	ds.User = defaultString(opts.DatasourceUser, "${APP_DB_USER}")
	ds.Password = defaultString(opts.DatasourcePassword, "${APP_DB_PASSWORD}")
	return ds
}

func columns(mt *xmeta.MetaTable) []*xmeta.ColumnDef {
	var cols []*xmeta.ColumnDef
	for _, elem := range mt.GetElements() {
		if col := elem.GetColumnDefElement(); col != nil {
			cols = append(cols, col)
		}
	}
	return cols
}

func constraints(mt *xmeta.MetaTable) []*xmeta.TableConstraint {
	var out []*xmeta.TableConstraint
	for _, elem := range mt.GetElements() {
		if constraint := elem.GetTableConstraintElement(); constraint != nil && constraint.GetSpec() != nil {
			out = append(out, constraint)
		}
	}
	return out
}

func primaryKeys(mt *xmeta.MetaTable) []string {
	var pks []string
	for _, col := range columns(mt) {
		for _, constraint := range col.GetConstraints() {
			if constraint.GetSpec().GetUniqueItem().GetIsPrimaryKey() {
				pks = append(pks, col.Name)
			}
		}
	}
	for _, constraint := range constraints(mt) {
		if unique := constraint.GetSpec().GetUniqueItem(); unique != nil && unique.IsPrimary {
			pks = append(pks, unique.Columns...)
		}
	}
	return unique(pks)
}

func isAutoColumn(col *xmeta.ColumnDef) bool {
	for _, deco := range col.GetMyDecos() {
		if deco == xmeta.AutoIncrement_AutoIncrementConfirm {
			return true
		}
	}
	if strings.EqualFold(col.GetOptions()["IsIdentity"], "true") {
		return true
	}
	defaultExpr := strings.ToLower(anyToString(col.GetDefault()))
	return strings.Contains(defaultExpr, "nextval(")
}

func isGeneratedColumn(col *xmeta.ColumnDef) bool {
	return strings.EqualFold(col.GetOptions()["IsGenerated"], "true")
}

func hasNotNull(col *xmeta.ColumnDef) bool {
	for _, constraint := range col.GetConstraints() {
		if constraint.GetSpec().GetNotNullItem() != xmeta.NotNullColumnSpec_NotNullColumnSpecUnknown {
			return true
		}
	}
	return false
}

func sqlType(dt *xmeta.DataType, driver string) string {
	if dt == nil {
		return "text"
	}
	switch t := dt.GetTypeClause().(type) {
	case *xmeta.DataType_IntData:
		return intType("int", t.IntData.GetIsUnsigned(), driver)
	case *xmeta.DataType_SmallIntData:
		return intType("smallint", t.SmallIntData.GetIsUnsigned(), driver)
	case *xmeta.DataType_BigIntData:
		return intType("bigint", t.BigIntData.GetIsUnsigned(), driver)
	case *xmeta.DataType_TinyIntData:
		return intType("tinyint", t.TinyIntData.GetIsUnsigned(), driver)
	case *xmeta.DataType_MediumIntData:
		return intType("mediumint", t.MediumIntData.GetIsUnsigned(), driver)
	case *xmeta.DataType_DecimalData:
		if t.DecimalData.GetPrecision() > 0 {
			return fmt.Sprintf("decimal(%d,%d)", t.DecimalData.GetPrecision(), t.DecimalData.GetScale())
		}
		return "decimal"
	case *xmeta.DataType_CharData:
		return sizedType("char", t.CharData.GetSize())
	case *xmeta.DataType_VarcharData:
		return sizedType("varchar", t.VarcharData.GetSize())
	case *xmeta.DataType_TextData:
		return "text"
	case *xmeta.DataType_BooleanData:
		if driver == "mysql" {
			return "tinyint(1)"
		}
		return "boolean"
	case *xmeta.DataType_TimestampData:
		if t.TimestampData.GetWithTimeZone() && driver == "postgres" {
			return "timestamp with time zone"
		}
		return "timestamp"
	case *xmeta.DataType_DateData:
		return "date"
	case *xmeta.DataType_TimeData:
		return "time"
	case *xmeta.DataType_RealData:
		return "real"
	case *xmeta.DataType_FloatData:
		return "float"
	case *xmeta.DataType_DoubleData:
		return "double precision"
	case *xmeta.DataType_ByteaData:
		if driver == "mysql" {
			return "blob"
		}
		return "bytea"
	case *xmeta.DataType_UUIDData:
		return "uuid"
	case *xmeta.DataType_JSONData:
		return "json"
	case *xmeta.DataType_XMLData:
		return "xml"
	case *xmeta.DataType_EnumData:
		return "enum('" + strings.Join(t.EnumData.GetValues(), "','") + "')"
	case *xmeta.DataType_SetData:
		return "set('" + strings.Join(t.SetData.GetValues(), "','") + "')"
	case *xmeta.DataType_CustomData:
		return strings.Join(t.CustomData.GetIdents(), ".")
	default:
		return "text"
	}
}

func intType(base string, unsigned bool, driver string) string {
	if unsigned && driver == "mysql" {
		return base + " unsigned"
	}
	return base
}

func sizedType(base string, size uint32) string {
	if size == 0 {
		return base
	}
	return fmt.Sprintf("%s(%d)", base, size)
}

func anyToString(a *anypb.Any) string {
	if a == nil {
		return ""
	}
	var sv wrapperspb.StringValue
	if err := a.UnmarshalTo(&sv); err == nil {
		return sv.Value
	}
	return ""
}

func referentialAction(action xmeta.ReferentialAction) string {
	switch action {
	case xmeta.ReferentialAction_ReferentialAction_Cascade:
		return "CASCADE"
	case xmeta.ReferentialAction_ReferentialAction_SetNull:
		return "SET NULL"
	case xmeta.ReferentialAction_ReferentialAction_SetDefault:
		return "SET DEFAULT"
	case xmeta.ReferentialAction_ReferentialAction_Restrict:
		return "RESTRICT"
	case xmeta.ReferentialAction_ReferentialAction_NoAction:
		return "NO ACTION"
	default:
		return ""
	}
}

func normalizeDriver(values ...string) string {
	for _, value := range values {
		v := strings.ToLower(strings.TrimSpace(value))
		switch v {
		case "postgres", "postgresql", "pgsql":
			return "postgres"
		case "mysql", "mariadb":
			return "mysql"
		case "sqlite", "sqlite3":
			return "sqlite"
		}
	}
	return "mysql"
}

func tavolaDriverName(driver string) string {
	switch driver {
	case "postgres":
		return "PostgreSQL"
	case "sqlite":
		return "SQLite"
	default:
		return "MySQL"
	}
}

func defaultPort(driver string) string {
	switch driver {
	case "postgres":
		return "5432"
	case "mysql":
		return "3306"
	default:
		return ""
	}
}

func hasAuth(auth AuthOptions) bool {
	return auth.Table != "" || auth.ID != "" || auth.Login != "" || auth.Password != ""
}

func validateAuth(auth AuthOptions) error {
	missing := []string{}
	if auth.Table == "" {
		missing = append(missing, "--auth-table")
	}
	if auth.ID == "" {
		missing = append(missing, "--auth-id")
	}
	if auth.Login == "" {
		missing = append(missing, "--auth-login")
	}
	if auth.Password == "" {
		missing = append(missing, "--auth-password")
	}
	if len(missing) > 0 {
		return fmt.Errorf("partial auth config; missing %s", strings.Join(missing, ", "))
	}
	return nil
}

func resolveTable(name string, tables map[string]Table) string {
	if _, ok := tables[name]; ok {
		return name
	}
	var matches []string
	for table := range tables {
		if lastSegment(table) == name {
			matches = append(matches, table)
		}
	}
	sort.Strings(matches)
	if len(matches) == 1 {
		return matches[0]
	}
	return name
}

func resolveTableNameOnly(name string, tables map[string]bool) string {
	if tables[name] {
		return name
	}
	var matches []string
	for table := range tables {
		if lastSegment(table) == name {
			matches = append(matches, table)
		}
	}
	sort.Strings(matches)
	if len(matches) == 1 {
		return matches[0]
	}
	return name
}

func lastSegment(value string) string {
	parts := strings.Split(value, ".")
	return parts[len(parts)-1]
}

func tableName(name *xmeta.ObjectName) string {
	idents := name.GetIdents()
	if len(idents) == 0 {
		return ""
	}
	if len(idents) == 2 && idents[0] == "public" {
		return idents[1]
	}
	return strings.Join(idents, ".")
}

func referenceKeyTableName(expr *xmeta.ReferenceKeyExpr) string {
	if name := tableName(expr.GetTableObjectName()); name != "" {
		return name
	}
	return expr.GetTableName()
}

func lastIdent(name *xmeta.ObjectName) string {
	idents := name.GetIdents()
	if len(idents) == 0 {
		return "table"
	}
	return idents[len(idents)-1]
}

func safeName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "app"
	}
	value = regexp.MustCompile(`[^A-Za-z0-9_]+`).ReplaceAllString(value, "_")
	value = strings.Trim(value, "_")
	if value == "" {
		return "app"
	}
	if value[0] >= '0' && value[0] <= '9' {
		value = "t_" + value
	}
	return value
}

func uniqueName(base string, seen map[string]int) string {
	if base == "" {
		base = "component"
	}
	seen[base]++
	if seen[base] == 1 {
		return base
	}
	return fmt.Sprintf("%s_%d", base, seen[base])
}

func unique(values []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func defaultInt(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func quoteIdents(idents []string, driver string) string {
	quoted := make([]string, 0, len(idents))
	for _, ident := range idents {
		quoted = append(quoted, quoteIdent(ident, driver))
	}
	return strings.Join(quoted, ", ")
}

func quoteTable(table, driver string) string {
	parts := strings.Split(table, ".")
	for i := range parts {
		parts[i] = quoteIdent(parts[i], driver)
	}
	return strings.Join(parts, ".")
}

func quoteIdent(ident, driver string) string {
	if driver == "mysql" {
		return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
