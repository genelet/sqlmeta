package xmeta

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
)

type AppSpecOptions struct {
	Name               string
	DatasourceName     string
	DatasourceDriver   string
	DatasourceDatabase string
	Auth               *AuthBinding
	RoleName           string
	RoleDescription    string
	PublicOperations   []CRUDOperation
	RoleOperations     []CRUDOperation
	Scope              *RoleScope
	SchemaOverrides    *SchemaRelationshipOverrides
	FallbackAllTables  bool
}

func BuildDefaultAppSpec(meta *MetaDatabase, opts AppSpecOptions) (*AppSpec, error) {
	if meta == nil {
		return nil, fmt.Errorf("metadata database is nil")
	}
	name := strings.TrimSpace(opts.Name)
	if name == "" {
		name = meta.GetName()
	}
	if name == "" {
		return nil, fmt.Errorf("app name is required")
	}

	driver := defaultAppString(opts.DatasourceDriver, meta.GetOptions()["Driver"])
	spec := &AppSpec{
		Name: name,
		Datasource: &DatasourceMetadata{
			Name:     defaultAppString(opts.DatasourceName, meta.GetName()),
			Driver:   driver,
			Database: defaultAppString(opts.DatasourceDatabase, meta.GetName()),
			Options:  map[string]string{},
		},
		Options:         map[string]string{},
		SchemaOverrides: cloneSchemaRelationshipOverrides(opts.SchemaOverrides),
	}

	publicOps := cloneOperations(opts.PublicOperations)
	if len(publicOps) == 0 {
		publicOps = []CRUDOperation{CRUDOperation_CRUDOperationList}
	}

	seen := map[string]int{}
	tables := sortedMetaTables(meta.GetTables())
	for _, table := range tables {
		componentName := uniqueAppName(safeAppName(lastAppIdent(table.GetName())), seen)
		spec.Components = append(spec.Components, &AppComponent{
			Name:             componentName,
			TableName:        cloneObjectName(table.GetName()),
			Description:      fmt.Sprintf("%s records", lastAppIdent(table.GetName())),
			PublicOperations: cloneOperations(publicOps),
			Options:          map[string]string{},
		})
	}

	if opts.Auth != nil {
		scope := cloneRoleScope(opts.Scope)
		if scope == nil {
			scope = &RoleScope{
				Mode:              FKTraversalMode_FKTraversalModeAuthUserDescendants,
				Direction:         FKTraversalDirection_FKTraversalDirectionChildren,
				MaxDepth:          32,
				IncludeStartTable: true,
			}
		}
		if opts.FallbackAllTables {
			scope.Mode = FKTraversalMode_FKTraversalModeAllTables
		}
		ops := cloneOperations(opts.RoleOperations)
		if len(ops) == 0 {
			ops = []CRUDOperation{
				CRUDOperation_CRUDOperationList,
				CRUDOperation_CRUDOperationRead,
				CRUDOperation_CRUDOperationCreate,
				CRUDOperation_CRUDOperationUpdate,
				CRUDOperation_CRUDOperationDelete,
			}
		}
		spec.Roles = append(spec.Roles, &AppRole{
			Name:        defaultAppString(opts.RoleName, "u"),
			Description: defaultAppString(opts.RoleDescription, "Signed-in user"),
			Auth:        cloneAuthBinding(opts.Auth),
			CrudPolicy:  &CRUDPolicy{Operations: ops},
			Scope:       scope,
			Options:     map[string]string{},
		})
	}

	return spec, nil
}

func ExpandRoleScopes(meta *MetaDatabase, spec *AppSpec) (*ExpandedAppSpec, error) {
	expanded, _, err := ExpandRoleScopesWithDiagnostics(meta, spec)
	return expanded, err
}

// ExpandRoleScopesWithDiagnostics resolves role grants and returns the stable
// diagnostic code for every warning emitted during expansion.
func ExpandRoleScopesWithDiagnostics(meta *MetaDatabase, spec *AppSpec) (*ExpandedAppSpec, []Diagnostic, error) {
	if meta == nil {
		return nil, nil, fmt.Errorf("metadata database is nil")
	}
	if spec == nil {
		return nil, nil, fmt.Errorf("app spec is nil")
	}

	index := newAppTableIndex(meta.GetTables())
	graph := buildFKGraph(meta.GetTables(), index, spec.GetSchemaOverrides())
	expanded := &ExpandedAppSpec{
		Spec:     proto.Clone(spec).(*AppSpec),
		Warnings: DiagnosticMessages(graph.diagnostics),
	}
	diagnostics := append([]Diagnostic{}, graph.diagnostics...)

	for _, role := range spec.GetRoles() {
		grants, roleDiagnostics, err := expandRole(meta, spec, role, index, graph)
		if err != nil {
			return nil, nil, err
		}
		diagnostics = append(diagnostics, roleDiagnostics...)
		expanded.Warnings = append(expanded.Warnings, DiagnosticMessages(roleDiagnostics)...)
		expanded.TableGrants = append(expanded.TableGrants, grants...)
	}

	componentByTable := map[string][]*AppComponent{}
	for _, component := range spec.GetComponents() {
		key := objectNameKey(component.GetTableName())
		componentByTable[key] = append(componentByTable[key], component)
	}
	for _, grant := range expanded.GetTableGrants() {
		key := objectNameKey(grant.GetTableName())
		for _, component := range componentByTable[key] {
			expanded.ComponentGrants = append(expanded.ComponentGrants, &ExpandedComponentGrant{
				RoleName:      grant.GetRoleName(),
				ComponentName: component.GetName(),
				TableName:     cloneObjectName(grant.GetTableName()),
				Operations:    cloneOperations(grant.GetOperations()),
			})
		}
	}

	sort.Slice(expanded.TableGrants, func(i, j int) bool {
		a, b := expanded.TableGrants[i], expanded.TableGrants[j]
		if a.GetRoleName() != b.GetRoleName() {
			return a.GetRoleName() < b.GetRoleName()
		}
		return objectNameKey(a.GetTableName()) < objectNameKey(b.GetTableName())
	})
	sort.Slice(expanded.ComponentGrants, func(i, j int) bool {
		a, b := expanded.ComponentGrants[i], expanded.ComponentGrants[j]
		if a.GetRoleName() != b.GetRoleName() {
			return a.GetRoleName() < b.GetRoleName()
		}
		return a.GetComponentName() < b.GetComponentName()
	})
	sort.Strings(expanded.Warnings)
	expanded.Warnings = uniqueAppStrings(expanded.Warnings)
	diagnostics = uniqueDiagnostics(diagnostics)

	return expanded, diagnostics, nil
}

func ObjectNameFromString(name string) *ObjectName {
	name = strings.TrimSpace(name)
	if name == "" {
		return &ObjectName{}
	}
	parts := strings.Split(name, ".")
	idents := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			idents = append(idents, part)
		}
	}
	return &ObjectName{Idents: idents}
}

type appTableIndex struct {
	byKey  map[string]*MetaTable
	byLast map[string][]string
}

type fkEdge struct {
	name        string
	parentKey   string
	childKey    string
	localColumn string
	refColumn   string
}

type fkGraph struct {
	children    map[string][]fkEdge
	parents     map[string][]fkEdge
	diagnostics []Diagnostic
}

type effectiveRelationships struct {
	primaryKeys      map[string][]string
	manualFKColumns  map[string]bool
	manualFKEdgeKeys map[string]bool
	manualFKEdges    []fkEdge
	diagnostics      []Diagnostic
}

func expandRole(meta *MetaDatabase, spec *AppSpec, role *AppRole, index appTableIndex, graph fkGraph) ([]*ExpandedTableGrant, []Diagnostic, error) {
	var diagnostics []Diagnostic
	ops := cloneOperations(role.GetCrudPolicy().GetOperations())
	if len(ops) == 0 {
		ops = []CRUDOperation{CRUDOperation_CRUDOperationList}
	}
	scope := role.GetScope()
	mode := scope.GetMode()
	if mode == FKTraversalMode_FKTraversalModeUnspecified {
		if role.GetAuth() != nil {
			mode = FKTraversalMode_FKTraversalModeAuthUserDescendants
		} else {
			mode = FKTraversalMode_FKTraversalModeNone
		}
	}

	grants := map[string]*ExpandedTableGrant{}
	addGrant := func(key string, path []*ObjectName, scopeColumn string, joins []fkEdge) {
		table := index.byKey[key]
		if table == nil {
			return
		}
		grants[key] = &ExpandedTableGrant{
			RoleName:       role.GetName(),
			TableName:      cloneObjectName(table.GetName()),
			Operations:     cloneOperations(ops),
			TraversalPath:  cloneObjectNames(path),
			ScopeColumn:    scopeColumn,
			TraversalJoins: traversalJoins(joins, index),
		}
	}

	switch mode {
	case FKTraversalMode_FKTraversalModeNone:
	case FKTraversalMode_FKTraversalModeAllTables:
		for _, table := range sortedMetaTables(meta.GetTables()) {
			addGrant(objectNameKey(table.GetName()), []*ObjectName{table.GetName()}, "", nil)
		}
	case FKTraversalMode_FKTraversalModeAuthUserDescendants:
		auth := role.GetAuth()
		if auth == nil || len(auth.GetUserTable().GetIdents()) == 0 {
			return nil, diagnostics, fmt.Errorf("role %s has no auth user table for FK scope expansion", role.GetName())
		}
		startKey, warning := index.resolve(auth.GetUserTable())
		if warning != "" {
			diagnostics = append(diagnostics, WarningDiagnostic(authTableDiagnosticCode(warning), fmt.Sprintf("role %s auth table %q: %s", role.GetName(), objectNameKey(auth.GetUserTable()), warning)))
		}
		if startKey == "" {
			return nil, diagnostics, fmt.Errorf("role %s auth user table %q is required but was not found", role.GetName(), objectNameKey(auth.GetUserTable()))
		}
		if auth.GetUserIDColumn() == "" {
			return nil, diagnostics, fmt.Errorf("role %s auth binding has no user id column", role.GetName())
		} else if !tableHasColumn(index.byKey[startKey], auth.GetUserIDColumn()) {
			return nil, diagnostics, fmt.Errorf("role %s auth table %s has no user id column %s", role.GetName(), startKey, auth.GetUserIDColumn())
		}

		includeStart := scope == nil || scope.GetIncludeStartTable()
		if includeStart {
			addGrant(startKey, []*ObjectName{index.byKey[startKey].GetName()}, "", nil)
		}
		maxDepth := int(scope.GetMaxDepth())
		if maxDepth == 0 {
			maxDepth = 32
		}
		direction := scope.GetDirection()
		if direction == FKTraversalDirection_FKTraversalDirectionUnspecified {
			direction = FKTraversalDirection_FKTraversalDirectionChildren
		}
		diagnostics = append(diagnostics, walkRoleScope(role.GetName(), startKey, auth.GetUserIDColumn(), direction, maxDepth, index, graph, addGrant)...)
	default:
		diagnostics = append(diagnostics, WarningDiagnostic(DiagnosticUnsupportedFKTraversal, fmt.Sprintf("role %s uses unsupported FK traversal mode %s", role.GetName(), mode.String())))
	}

	for _, include := range scope.GetIncludeTables() {
		key, warning := index.resolve(include)
		if warning != "" {
			diagnostics = append(diagnostics, WarningDiagnostic(roleTableDiagnosticCode(warning), fmt.Sprintf("role %s include table %q: %s", role.GetName(), objectNameKey(include), warning)))
		}
		if key != "" {
			addGrant(key, []*ObjectName{index.byKey[key].GetName()}, "", nil)
		}
	}
	for _, exclude := range scope.GetExcludeTables() {
		key, warning := index.resolve(exclude)
		if warning != "" {
			diagnostics = append(diagnostics, WarningDiagnostic(roleTableDiagnosticCode(warning), fmt.Sprintf("role %s exclude table %q: %s", role.GetName(), objectNameKey(exclude), warning)))
		}
		delete(grants, key)
	}

	out := make([]*ExpandedTableGrant, 0, len(grants))
	for _, grant := range grants {
		out = append(out, grant)
	}
	return out, diagnostics, nil
}

func walkRoleScope(roleName, startKey, userIDColumn string, direction FKTraversalDirection, maxDepth int, index appTableIndex, graph fkGraph, addGrant func(string, []*ObjectName, string, []fkEdge)) []Diagnostic {
	var diagnostics []Diagnostic
	type step struct {
		key   string
		depth int
		path  []string
		joins []fkEdge
	}
	queue := []step{{key: startKey, path: []string{startKey}}}
	visited := map[string]bool{startKey: true}
	matchedDirectChild := false

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current.depth >= maxDepth {
			continue
		}
		for _, edge := range nextEdges(current.key, direction, graph) {
			next := edge.childKey
			scopeColumn := edge.localColumn
			if edge.childKey == current.key {
				next = edge.parentKey
				scopeColumn = edge.refColumn
			}
			if current.key == startKey && userIDColumn != "" && !strings.EqualFold(edge.refColumn, userIDColumn) {
				continue
			}
			if containsAppString(current.path, next) {
				diagnostics = append(diagnostics, WarningDiagnostic(DiagnosticRoleFKCycle, fmt.Sprintf("role %s FK scope skipped cycle %s -> %s", roleName, current.key, next)))
				continue
			}
			path := append(append([]string{}, current.path...), next)
			joins := append(append([]fkEdge{}, current.joins...), edge)
			if !visited[next] {
				visited[next] = true
				matchedDirectChild = matchedDirectChild || current.key == startKey
				addGrant(next, pathObjectNames(path, index), scopeColumn, joins)
				queue = append(queue, step{key: next, depth: current.depth + 1, path: path, joins: joins})
			}
		}
	}

	if userIDColumn != "" && !matchedDirectChild {
		diagnostics = append(diagnostics, WarningDiagnostic(DiagnosticRoleFKNoChildren, fmt.Sprintf("role %s found no child foreign keys referencing %s.%s", roleName, startKey, userIDColumn)))
	}
	return diagnostics
}

func nextEdges(key string, direction FKTraversalDirection, graph fkGraph) []fkEdge {
	var out []fkEdge
	if direction == FKTraversalDirection_FKTraversalDirectionChildren || direction == FKTraversalDirection_FKTraversalDirectionBoth {
		out = append(out, graph.children[key]...)
	}
	if direction == FKTraversalDirection_FKTraversalDirectionParents || direction == FKTraversalDirection_FKTraversalDirectionBoth {
		out = append(out, graph.parents[key]...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].childKey != out[j].childKey {
			return out[i].childKey < out[j].childKey
		}
		return out[i].parentKey < out[j].parentKey
	})
	return out
}

func buildFKGraph(tables []*MetaTable, index appTableIndex, overrides *SchemaRelationshipOverrides) fkGraph {
	graph := fkGraph{children: map[string][]fkEdge{}, parents: map[string][]fkEdge{}}
	relationships := buildEffectiveRelationships(tables, index, overrides)
	graph.diagnostics = append(graph.diagnostics, relationships.diagnostics...)
	for _, edge := range relationships.manualFKEdges {
		graph.add(edge)
	}
	for _, table := range tables {
		childKey := objectNameKey(table.GetName())
		for _, elem := range table.GetElements() {
			if col := elem.GetColumnDefElement(); col != nil {
				for _, constraint := range col.GetConstraints() {
					ref := constraint.GetSpec().GetReferenceItem()
					if ref == nil {
						continue
					}
					foreignCols := ref.GetColumns()
					if len(foreignCols) > 1 {
						graph.diagnostics = append(graph.diagnostics, WarningDiagnostic(DiagnosticRoleFKComposite, fmt.Sprintf("%s.%s has composite column FK; skipped role scope edge", childKey, col.GetName())))
						continue
					}
					if relationships.manualFKColumns[fkColumnKey(childKey, []string{col.GetName()})] {
						continue
					}
					parentKey, warning := index.resolve(ref.GetTableName())
					if warning != "" {
						graph.diagnostics = append(graph.diagnostics, WarningDiagnostic(roleFKTargetDiagnosticCode(warning), fmt.Sprintf("%s.%s FK target %q: %s", childKey, col.GetName(), objectNameKey(ref.GetTableName()), warning)))
					}
					if parentKey == "" {
						continue
					}
					refColumn := ""
					if len(foreignCols) == 1 {
						refColumn = foreignCols[0]
					} else if cols := relationships.primaryKeys[parentKey]; len(cols) == 1 {
						refColumn = cols[0]
					}
					edge := fkEdge{name: constraint.GetName(), parentKey: parentKey, childKey: childKey, localColumn: col.GetName(), refColumn: refColumn}
					if relationships.manualFKEdgeKeys[fkEdgeKey(edge)] {
						continue
					}
					graph.add(edge)
				}
				continue
			}
			constraint := elem.GetTableConstraintElement()
			ref := constraint.GetSpec().GetReferenceItem()
			if ref == nil {
				continue
			}
			parentName := ReferenceKeyExprObjectName(ref.GetKeyExpr())
			parentCols := cleanColumns(ref.GetKeyExpr().GetColumns())
			if len(parentCols) == 0 {
				parentKey, _ := index.resolve(parentName)
				parentCols = relationships.primaryKeys[parentKey]
			}
			if len(ref.GetColumns()) != 1 || len(parentCols) != 1 {
				graph.diagnostics = append(graph.diagnostics, WarningDiagnostic(DiagnosticRoleFKComposite, fmt.Sprintf("%s.%s has composite FK; skipped role scope edge", childKey, constraint.GetName())))
				continue
			}
			if relationships.manualFKColumns[fkColumnKey(childKey, ref.GetColumns())] {
				continue
			}
			parentKey, warning := index.resolve(parentName)
			if warning != "" {
				graph.diagnostics = append(graph.diagnostics, WarningDiagnostic(roleFKTargetDiagnosticCode(warning), fmt.Sprintf("%s.%s FK target %q: %s", childKey, constraint.GetName(), objectNameKey(parentName), warning)))
			}
			if parentKey == "" {
				continue
			}
			edge := fkEdge{
				name:        constraint.GetName(),
				parentKey:   parentKey,
				childKey:    childKey,
				localColumn: ref.GetColumns()[0],
				refColumn:   parentCols[0],
			}
			if relationships.manualFKEdgeKeys[fkEdgeKey(edge)] {
				continue
			}
			graph.add(edge)
		}
	}
	return graph
}

func buildEffectiveRelationships(tables []*MetaTable, index appTableIndex, overrides *SchemaRelationshipOverrides) effectiveRelationships {
	relationships := effectiveRelationships{
		primaryKeys:      map[string][]string{},
		manualFKColumns:  map[string]bool{},
		manualFKEdgeKeys: map[string]bool{},
	}
	for _, table := range tables {
		if pks := primaryKeyColumns(table); len(pks) > 0 {
			key := objectNameKey(table.GetName())
			relationships.primaryKeys[key] = pks
		}
	}
	for _, pk := range overrides.GetPrimaryKeys() {
		override := resolveManualPrimaryKeyOverride(index, pk)
		relationships.diagnostics = append(relationships.diagnostics, override.diagnostics...)
		if !override.ok {
			continue
		}
		relationships.primaryKeys[override.key] = override.columns
	}
	for _, fk := range overrides.GetForeignKeys() {
		override := resolveManualForeignKeyOverride(index, relationships.primaryKeys, fk, "role scope")
		relationships.diagnostics = append(relationships.diagnostics, override.diagnostics...)
		if !override.ok {
			continue
		}
		edge := fkEdge{
			name:        fk.GetName(),
			parentKey:   override.parentKey,
			childKey:    override.childKey,
			localColumn: override.childCols[0],
			refColumn:   override.parentCols[0],
		}
		relationships.manualFKColumns[fkColumnKey(override.childKey, override.childCols)] = true
		relationships.manualFKEdgeKeys[fkEdgeKey(edge)] = true
		relationships.manualFKEdges = append(relationships.manualFKEdges, edge)
	}
	sort.Slice(relationships.manualFKEdges, func(i, j int) bool {
		if relationships.manualFKEdges[i].childKey != relationships.manualFKEdges[j].childKey {
			return relationships.manualFKEdges[i].childKey < relationships.manualFKEdges[j].childKey
		}
		return relationships.manualFKEdges[i].localColumn < relationships.manualFKEdges[j].localColumn
	})
	return relationships
}

func authTableDiagnosticCode(warning string) string {
	if strings.Contains(warning, "ambiguous table name") {
		return DiagnosticAuthTableAmbiguous
	}
	return DiagnosticAuthTableMissing
}

func roleFKTargetDiagnosticCode(warning string) string {
	if strings.Contains(warning, "ambiguous table name") {
		return DiagnosticRoleFKAmbiguousTable
	}
	return DiagnosticRoleFKMissingTarget
}

func roleTableDiagnosticCode(warning string) string {
	if strings.Contains(warning, "ambiguous table name") {
		return DiagnosticRoleTableAmbiguous
	}
	return DiagnosticRoleTableMissing
}

func (g fkGraph) add(edge fkEdge) {
	g.children[edge.parentKey] = append(g.children[edge.parentKey], edge)
	g.parents[edge.childKey] = append(g.parents[edge.childKey], edge)
}

func newAppTableIndex(tables []*MetaTable) appTableIndex {
	index := appTableIndex{byKey: map[string]*MetaTable{}, byLast: map[string][]string{}}
	for _, table := range tables {
		key := objectNameKey(table.GetName())
		index.byKey[key] = table
		last := lastAppIdent(table.GetName())
		index.byLast[last] = append(index.byLast[last], key)
	}
	for last := range index.byLast {
		sort.Strings(index.byLast[last])
	}
	return index
}

func (i appTableIndex) resolve(name *ObjectName) (string, string) {
	key := objectNameKey(name)
	if key == "" {
		return "", "empty table name"
	}
	if _, ok := i.byKey[key]; ok {
		return key, ""
	}
	last := lastAppIdent(name)
	matches := i.byLast[last]
	switch len(matches) {
	case 0:
		return "", "table was not found"
	case 1:
		return matches[0], ""
	default:
		return "", fmt.Sprintf("ambiguous table name matched %s", strings.Join(matches, ", "))
	}
}

func sortedMetaTables(tables []*MetaTable) []*MetaTable {
	out := append([]*MetaTable{}, tables...)
	sort.Slice(out, func(i, j int) bool {
		return objectNameKey(out[i].GetName()) < objectNameKey(out[j].GetName())
	})
	return out
}

func tableHasColumn(table *MetaTable, column string) bool {
	for _, elem := range table.GetElements() {
		if col := elem.GetColumnDefElement(); col != nil && strings.EqualFold(col.GetName(), column) {
			return true
		}
	}
	return false
}

func primaryKeyColumns(table *MetaTable) []string {
	var pks []string
	for _, elem := range table.GetElements() {
		if col := elem.GetColumnDefElement(); col != nil {
			for _, constraint := range col.GetConstraints() {
				if constraint.GetSpec().GetUniqueItem().GetIsPrimaryKey() {
					pks = append(pks, col.GetName())
				}
			}
			continue
		}
		if constraint := elem.GetTableConstraintElement(); constraint != nil {
			if unique := constraint.GetSpec().GetUniqueItem(); unique != nil && unique.GetIsPrimary() {
				pks = append(pks, unique.GetColumns()...)
			}
		}
	}
	return uniqueAppStrings(pks)
}

func cleanColumns(cols []string) []string {
	out := make([]string, 0, len(cols))
	for _, col := range cols {
		col = strings.TrimSpace(col)
		if col != "" {
			out = append(out, col)
		}
	}
	return out
}

func fkColumnKey(tableKey string, cols []string) string {
	clean := cleanColumns(cols)
	for i := range clean {
		clean[i] = strings.ToLower(clean[i])
	}
	return tableKey + "|" + strings.Join(clean, ",")
}

func fkEdgeKey(edge fkEdge) string {
	return edge.childKey + "|" + strings.ToLower(edge.localColumn) + "|" + edge.parentKey + "|" + strings.ToLower(edge.refColumn)
}

func pathObjectNames(path []string, index appTableIndex) []*ObjectName {
	out := make([]*ObjectName, 0, len(path))
	for _, key := range path {
		if table := index.byKey[key]; table != nil {
			out = append(out, table.GetName())
		}
	}
	return out
}

func traversalJoins(edges []fkEdge, index appTableIndex) []*ExpandedTraversalJoin {
	out := make([]*ExpandedTraversalJoin, 0, len(edges))
	for _, edge := range edges {
		parent := index.byKey[edge.parentKey]
		child := index.byKey[edge.childKey]
		if parent == nil || child == nil {
			continue
		}
		out = append(out, &ExpandedTraversalJoin{
			ForeignKeyName: edge.name,
			ParentTable:    cloneObjectName(parent.GetName()),
			ParentColumn:   edge.refColumn,
			ChildTable:     cloneObjectName(child.GetName()),
			ChildColumn:    edge.localColumn,
		})
	}
	return out
}

func ReferenceKeyExprObjectName(expr *ReferenceKeyExpr) *ObjectName {
	if expr == nil {
		return &ObjectName{}
	}
	if len(expr.GetTableObjectName().GetIdents()) > 0 {
		return expr.GetTableObjectName()
	}
	return ObjectNameFromString(expr.GetTableName())
}

func lastAppIdent(name *ObjectName) string {
	idents := name.GetIdents()
	if len(idents) == 0 {
		return "table"
	}
	return idents[len(idents)-1]
}

func safeAppName(value string) string {
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

func uniqueAppName(base string, seen map[string]int) string {
	if base == "" {
		base = "component"
	}
	seen[base]++
	if seen[base] == 1 {
		return base
	}
	return fmt.Sprintf("%s_%d", base, seen[base])
}

func cloneAuthBinding(auth *AuthBinding) *AuthBinding {
	if auth == nil {
		return nil
	}
	return proto.Clone(auth).(*AuthBinding)
}

func cloneRoleScope(scope *RoleScope) *RoleScope {
	if scope == nil {
		return nil
	}
	return proto.Clone(scope).(*RoleScope)
}

func cloneSchemaRelationshipOverrides(overrides *SchemaRelationshipOverrides) *SchemaRelationshipOverrides {
	if overrides == nil {
		return nil
	}
	return proto.Clone(overrides).(*SchemaRelationshipOverrides)
}

func cloneObjectName(name *ObjectName) *ObjectName {
	if name == nil {
		return nil
	}
	return &ObjectName{Idents: append([]string{}, name.GetIdents()...)}
}

func cloneObjectNames(names []*ObjectName) []*ObjectName {
	out := make([]*ObjectName, 0, len(names))
	for _, name := range names {
		out = append(out, cloneObjectName(name))
	}
	return out
}

func cloneOperations(ops []CRUDOperation) []CRUDOperation {
	return append([]CRUDOperation{}, ops...)
}

func uniqueAppStrings(values []string) []string {
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

func containsAppString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func defaultAppString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
