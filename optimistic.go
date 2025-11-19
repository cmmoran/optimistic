package optimistic

import (
	"crypto/rand"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

const (
	beforeCreateCallback = "gorm:create"
	afterCreateCallback  = "gorm:after_create"
	beforeUpdateCallback = "gorm:update"
	afterUpdateCallback  = "gorm:after_update"

	contextKeyFromVersion = "optimistic:from_version"
	contextKeyToVersion   = "optimistic:to_version"
	conflictClauseName    = "optimistic:conflict"
)

var (
	ErrOptimisticLock = errors.New("optimistic lock conflict")
	ulidEntropy       = ulid.Monotonic(rand.Reader, 0)
	tyTime            = reflect.TypeOf(time.Time{})
	ty16Byte          = reflect.TypeOf((*[16]byte)(nil)).Elem()
)

type Config struct {
	// DisableReturning overrides your driver's default support for `RETURNING`
	disableReturning bool
	// TagName specifies the `gorm` tag setting name to use for marking the `Version` field on your models
	// Example:
	//	type Model struct {
	//		ID uint64 		`gorm:"type:numeric;primaryKey"`
	//		...
	//		Version uint64 	`gorm:"type:numeric;version"`
	//	}
	//
	// To enable time or uuid or ulid based versioning:
	//	type Model struct {
	//		ID uint64 			`gorm:"type:numeric;primaryKey"`
	//		...
	//		Version uuid.UUID	`gorm:"version:uuid"`
	//	}
	tagName string
}

type ConfigOption func(*Config)

func WithTagName(tagName string) ConfigOption {
	return func(cfg *Config) {
		cfg.tagName = tagName
	}
}

func WithDisableReturning() ConfigOption {
	return func(cfg *Config) {
		cfg.disableReturning = true
	}
}

func WithConfig(cfg Config) ConfigOption {
	return func(c *Config) {
		*c = cfg
	}
}

// Plugin wires up optimistic‐locking callbacks.
type Plugin struct {
	*Config
}

func (Plugin) Name() string { return "optimistic_lock" }

func (p *Plugin) Initialize(db *gorm.DB) error {
	// Simple dialect check: MySQL doesn’t support RETURNING.
	supportsReturning := db.Dialector.Name() != "mysql"
	if p.disableReturning {
		supportsReturning = false
	}
	p.tagName = strings.ToUpper(p.tagName)

	// CREATE → seed and verify initial version
	_ = db.Callback().Create().
		Before(beforeCreateCallback).
		Register("optimistic:initialize_version", p.initializeVersion)
	_ = db.Callback().Create().
		After(afterCreateCallback).
		Register("optimistic:verify_create", p.verifyCreate)

	// UPDATE → inject SET/WHERE, then verify, then optionally resolve conflicts
	_ = db.Callback().Update().
		Before(beforeUpdateCallback).
		Register("optimistic:modify_update", p.modifyUpdate(supportsReturning))
	_ = db.Callback().Update().
		After(afterUpdateCallback).
		Register("optimistic:verify_update", p.verifyUpdate(supportsReturning))
	_ = db.Callback().Update().
		After(afterUpdateCallback).
		Register("optimistic:resolve_conflict", p.resolveConflict)

	return nil
}

// initializeVersion sets version=1/UUID/ULID/time.Now() on new records.
func (p *Plugin) initializeVersion(db *gorm.DB) {
	if db.DryRun || db.Statement.Unscoped {
		return
	}
	f := p.findVersionField(db.Statement.Schema)
	if f == nil {
		return
	}
	ft := f.StructField.Type
	dest := reflect.ValueOf(db.Statement.Dest)
	if dest.Kind() == reflect.Ptr {
		dest = dest.Elem()
	}

	switch dest.Kind() {
	case reflect.Struct:
		p.setInitialVersion(db, dest, f, ft)
	case reflect.Slice:
		for i := 0; i < dest.Len(); i++ {
			elem := dest.Index(i)
			if elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}
			if elem.Kind() == reflect.Struct {
				p.setInitialVersion(db, elem, f, ft)
			}
		}
	default:
	}
}

func (p *Plugin) setInitialVersion(
	db *gorm.DB,
	elem reflect.Value,
	f *schema.Field,
	structFieldType reflect.Type,
) {
	ctx := db.Statement.Context
	switch {
	case isNumericKind(structFieldType.Kind()):
		_ = f.Set(ctx, elem, uint64(1))
	case ty16Byte.AssignableTo(structFieldType):
		if p.paramIs(f, "ulid") || strings.Contains(strings.ToLower(structFieldType.Name()), "ulid") {
			_ = f.Set(ctx, elem, ulid.MustNew(ulid.Timestamp(db.NowFunc()), ulidEntropy))
		} else {
			_ = f.Set(ctx, elem, uuid.New())
		}
	case structFieldType == tyTime:
		_ = f.Set(ctx, elem, db.NowFunc())
	}
}

// verifyCreate ensures the initial version is correct (1, non-zero UUID/ULID, or time).
func (p *Plugin) verifyCreate(db *gorm.DB) {
	if db.DryRun || db.Statement.Unscoped {
		return
	}
	f := p.findVersionField(db.Statement.Schema)
	if f == nil {
		return
	}
	dest := reflect.ValueOf(db.Statement.Dest)
	if dest.Kind() == reflect.Ptr {
		dest = dest.Elem()
	}

	switch dest.Kind() {
	case reflect.Struct:
		p.checkInitialVersionField(db, dest, f)
	case reflect.Slice:
		for i := 0; i < dest.Len(); i++ {
			elem := dest.Index(i)
			if elem.Kind() == reflect.Ptr {
				elem = elem.Elem()
			}
			p.checkInitialVersionField(db, elem, f)
		}
	default:
	}
}

func (p *Plugin) checkInitialVersionField(db *gorm.DB, v reflect.Value, f *schema.Field) {
	if db.DryRun || db.Statement.Unscoped {
		return
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		_ = db.AddError(ErrOptimisticLock)
		return
	}
	ft := f.StructField.Type
	rv := f.ReflectValueOf(db.Statement.Context, v)
	if !rv.IsValid() || rv.IsZero() {
		_ = db.AddError(ErrOptimisticLock)
		return
	}

	switch {
	case isNumericKind(ft.Kind()):
		if rv.Uint() != 1 {
			_ = db.AddError(ErrOptimisticLock)
		}
	case ty16Byte.AssignableTo(ft):
		// OK
	case ft == tyTime:
		// OK
	default:
		_ = db.AddError(ErrOptimisticLock)
	}
}

// modifyUpdate injects the SET … and WHERE … clauses for the update.
func (p *Plugin) modifyUpdate(supportsReturning bool) func(db *gorm.DB) {
	return func(db *gorm.DB) {
		if db.DryRun || db.Statement.Unscoped {
			return
		}
		if !isTargetedModelUpdate(db.Statement) {
			return
		}
		stmt := db.Statement
		f := p.findVersionField(stmt.Schema)
		if f == nil {
			return
		}

		// 1) stash old version
		oldVal, _ := f.ValueOf(stmt.Context, stmt.ReflectValue)
		stmt.DB.InstanceSet(contextKeyFromVersion, oldVal)

		// 2) build or merge SET clause
		if c, ok := stmt.Clauses[clause.Set{}.Name()]; ok {
			set := c.Expression.(clause.Set)
			p.bumpVersion(stmt, f, &set)
			c.Expression = set
		} else {
			var set clause.Set
			p.collectAssignments(stmt, f, &set)
			if len(set) == 0 {
				stmt.Omits = append(stmt.Omits, f.DBName)
				return
			}
			p.bumpVersion(stmt, f, &set)
			stmt.AddClause(set)
		}

		// 3) inject WHERE version = oldVal (plus PK, plus RETURNING if supported)
		p.injectWhereVersion(stmt, f, oldVal, supportsReturning)
	}
}

func (p *Plugin) collectAssignments(stmt *gorm.Statement, f *schema.Field, set *clause.Set) {
	// map-based updates
	if m, ok := stmt.Dest.(map[string]interface{}); ok {
		for col, val := range m {
			name := stmt.NamingStrategy.ColumnName("", col)
			if name == f.DBName {
				continue
			}
			if sf := stmt.Schema.LookUpField(name); sf != nil {
				if len(sf.DBName) == 0 || !sf.Updatable {
					continue
				}
				*set = append(*set, clause.Assignment{
					Column: clause.Column{Name: name},
					Value:  val,
				})
			}
		}
		return
	}
	// struct-based updates
	selectCols, restrict := stmt.SelectAndOmitColumns(false, true)
	for k, v := range selectCols {
		k = stmt.NamingStrategy.ColumnName("", k)
		selectCols[k] = v
	}
	for _, sf := range stmt.Schema.Fields {
		if sf == nil || len(sf.DBName) == 0 || !sf.Updatable {
			continue
		}
		name := stmt.NamingStrategy.ColumnName("", sf.DBName)
		if sf.PrimaryKey || name == f.DBName || !sf.Updatable {
			continue
		}
		sel := selectCols[name]
		if restrict {
			if !sel {
				continue
			}
		} else {
			if _, zero := sf.ValueOf(stmt.Context, stmt.ReflectValue); zero && !sel {
				continue
			}
		}
		val, _ := sf.ValueOf(stmt.Context, stmt.ReflectValue)
		*set = append(*set, clause.Assignment{
			Column: clause.Column{Name: sf.DBName},
			Value:  val,
		})
	}
}

// bumpVersion appends version‐bump to the SET clause and saves the “to” value.
func (p *Plugin) bumpVersion(
	stmt *gorm.Statement,
	f *schema.Field,
	set *clause.Set,
) {
	if set == nil || !isTargetedModelUpdate(stmt) {
		return
	}
	ft := f.StructField.Type
	name := stmt.NamingStrategy.ColumnName("", f.DBName)

	col := clause.Column{Name: name}

	var val any
	switch {
	case isNumericKind(ft.Kind()):
		val = clause.Expr{SQL: "? + 1", Vars: []any{col}}
	case ty16Byte.AssignableTo(ft):
		if p.paramIs(f, "ulid") || strings.Contains(strings.ToLower(f.FieldType.Name()), "ulid") {
			val = ulid.MustNew(ulid.Timestamp(stmt.DB.NowFunc()), ulidEntropy)
		} else {
			val = uuid.New()
		}
	case ft == tyTime:
		val = stmt.DB.NowFunc()
	default:
		return
	}
	*set = append(*set, clause.Assignment{Column: col, Value: val})
	stmt.DB.InstanceSet(contextKeyToVersion, val)
}

func isTargetedModelUpdate(stmt *gorm.Statement) bool {
	if stmt.Schema == nil || stmt.ReflectValue.Kind() == reflect.Invalid {
		return false
	}

	val := stmt.ReflectValue
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	switch val.Kind() {
	case reflect.Struct:
		pk := stmt.Schema.PrioritizedPrimaryField
		if pk == nil {
			return false
		}
		_, isZero := pk.ValueOf(stmt.Context, val)
		return !isZero
	case reflect.Slice:
		return val.Len() > 0
	default:
		return false
	}
}

func (p *Plugin) injectWhereVersion(
	stmt *gorm.Statement,
	f *schema.Field,
	oldVal any,
	supportsReturning bool,
) {
	if !isTargetedModelUpdate(stmt) {
		return
	}
	// gather existing WHERE expressions
	var existing clause.Where
	if c, ok := stmt.Clauses[clause.Where{}.Name()]; ok {
		if wh, ok2 := c.Expression.(clause.Where); ok2 {
			existing = wh
		}
	}
	additions := clause.Where{
		Exprs: make([]clause.Expression, 0),
	}

	// ensure PK(s) in WHERE
	missingPK := false
	pkVals := make([]any, 0)
	pkFlds := make([]*schema.Field, 0)
	for _, pf := range stmt.Schema.PrimaryFields {
		hasPK := false
		val, _ := pf.ValueOf(stmt.Context, stmt.ReflectValue)
		for _, expr := range existing.Exprs {
			if eq, ok := expr.(clause.Eq); ok {
				pfName := stmt.NamingStrategy.ColumnName("", pf.DBName)
				if name, ok2 := eqColumnName(stmt, eq); ok2 && name == pfName {
					hasPK = true
					break
				}
			}
		}
		if !hasPK {
			pkVals = append(pkVals, val)
			pkFlds = append(pkFlds, pf)
			missingPK = true
		}
	}

	if missingPK {
		for i, pkf := range pkFlds {
			additions.Exprs = append(additions.Exprs, clause.Eq{
				Column: clause.Column{Name: pkf.DBName},
				Value:  pkVals[i],
			})
		}
	}

	additions.Exprs = append(additions.Exprs, clause.Eq{
		Column: clause.Column{Name: f.DBName},
		Value:  oldVal,
	})

	stmt.AddClause(additions)

	if supportsReturning {
		stmt.AddClauseIfNotExists(clause.Returning{})
	}
}

// verifyUpdate ensures the DB actually bumped the version.
func (p *Plugin) verifyUpdate(supportsReturning bool) func(db *gorm.DB) {
	return func(db *gorm.DB) {
		if db.DryRun || db.Statement.Unscoped {
			return
		}
		if !isTargetedModelUpdate(db.Statement) {
			return
		}
		f := p.findVersionField(db.Statement.Schema)
		if f == nil {
			return
		}
		oldAny, _ := db.InstanceGet(contextKeyFromVersion)
		toAny, _ := db.InstanceGet(contextKeyToVersion)

		// no rows updated; if toAny was not set → conflict
		if db.RowsAffected == 0 {
			if toAny == nil {
				return
			}
			_ = db.AddError(ErrOptimisticLock)
			return
		}

		// RETURNING dialect: compare new vs expected
		if supportsReturning {
			newAny, _ := f.ValueOf(db.Statement.Context, db.Statement.ReflectValue)

			if !versionMatches(oldAny, toAny, newAny) {
				_ = db.AddError(ErrOptimisticLock)
			}
			return
		}

		// fallback for no RETURNING: reload and overwrite
		fresh := db.Session(&gorm.Session{NewDB: true, SkipHooks: true})
		current, err := p.reloadByPK(fresh, db.Statement)
		if err != nil {
			_ = db.AddError(err)
			return
		}
		reflect.Indirect(reflect.ValueOf(db.Statement.Model)).
			Set(reflect.Indirect(reflect.ValueOf(current)))
	}
}

func (p *Plugin) reloadByPK(
	db *gorm.DB,
	stmt *gorm.Statement,
) (interface{}, error) {
	dest := reflect.New(stmt.Schema.ModelType).Interface()
	rv := stmt.ReflectValue
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	for _, pf := range stmt.Schema.PrimaryFields {
		val, _ := pf.ValueOf(stmt.Context, stmt.ReflectValue)
		_ = pf.Set(stmt.Context, reflect.Indirect(reflect.ValueOf(dest)), val)
	}
	return dest, db.First(dest).Error
}

// resolveConflict runs user‐supplied Conflict handler on ErrOptimisticLock.
func (p *Plugin) resolveConflict(db *gorm.DB) {
	if db == nil || db.Statement == nil || db.DryRun || db.Statement.Unscoped {
		return
	}
	if !errors.Is(db.Error, ErrOptimisticLock) {
		return
	}
	c, ok := db.Statement.Clauses[conflictClauseName]
	if !ok {
		return
	}
	conflict := c.Expression.(Conflict)

	// load fresh row
	fresh := db.Session(&gorm.Session{NewDB: true, SkipHooks: true})
	// Must reset Error
	fresh.Error = nil
	fresh.RowsAffected = 0
	current, err := p.reloadByPK(fresh, db.Statement)
	if err != nil {
		return
	}

	// compute diff
	reporter := newDiffReporter()
	cmp.Diff(db.Statement.ReflectValue.Interface(), current, cmp.Reporter(reporter))

	// call user handler
	rv := anyDeref(current)
	ptr := anyRef(rv)
	resolved := conflict.OnVersionMismatch(current, reporter.Diff())
	current = ptr

	switch {
	case resolved == nil:
		db.Logger.Warn(db.Statement.Context, "[%s] canceled update on conflict", p.Name())
		db.RowsAffected = 0
	case cmp.Equal(current, resolved, cmp.Reporter(reporter.Reset())):
		db.Logger.Warn(db.Statement.Context, "[%s] accepted current value on conflict", p.Name())
		db.RowsAffected = 0
		reflect.Indirect(reflect.ValueOf(db.Statement.Model)).
			Set(reflect.Indirect(reflect.ValueOf(current)))
	default:
		// retry update with resolved object
		retry := fresh.Session(&gorm.Session{NewDB: true}).
			Model(resolved).
			Updates(resolved)
		db.Error = retry.Error
		db.RowsAffected = retry.RowsAffected
		reflect.Indirect(reflect.ValueOf(db.Statement.Model)).
			Set(reflect.Indirect(reflect.ValueOf(resolved)))
	}
}

func (p *Plugin) findVersionField(sch *schema.Schema) *schema.Field {
	if sch == nil {
		return nil
	}
	for _, f := range sch.Fields {
		if _, ok := f.TagSettings[p.tagName]; ok {
			return f
		}
	}
	return nil
}

func (p *Plugin) paramIs(f *schema.Field, s ...string) bool {
	switch len(s) {
	case 0:
		return false
	case 1:
		return strings.EqualFold(f.TagSettings[p.tagName], s[0])
	default:
		for _, set := range s {
			if strings.EqualFold(f.TagSettings[p.tagName], set) {
				return true
			}
		}
		return false
	}
}

func anyDeref(obj any) any {
	if obj == nil {
		return nil
	}

	v := reflect.ValueOf(obj)

	// Unwrap interfaces and pointers
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}

	return v.Interface()
}

func anyRef(obj any, wrapPointers ...bool) any {
	if obj == nil {
		return nil
	}

	v := reflect.ValueOf(obj)

	// Unwrap interfaces
	for v.Kind() == reflect.Interface && !v.IsNil() {
		v = v.Elem()
	}

	// Decide whether to wrap pointers or not
	if v.Kind() == reflect.Ptr {
		if len(wrapPointers) == 0 || !wrapPointers[0] {
			return obj // Leave pointer as-is
		}
	}

	// Create a new pointer to the value
	ptrVal := reflect.New(v.Type())
	ptrVal.Elem().Set(v)

	return ptrVal.Interface()
}

// versionMatches handles numeric, uuid/ulid, and time comparisons.
func versionMatches(oldAny, toAny, newAny any) bool {
	switch to := toAny.(type) {
	case clause.Expr:
		// numeric branch is the only branch with an Expr
		old := oldAny.(uint64)
		return newAny.(uint64) == old+1
	case time.Time:
		tNewAny := newAny.(time.Time)
		tAny := to
		return tNewAny.UnixNano() == tAny.UnixNano()
	case uuid.UUID:
		return reflect.DeepEqual(newAny.(uuid.UUID), to)
	case ulid.ULID:
		return reflect.DeepEqual(newAny.(ulid.ULID), to)
	default:
		return reflect.DeepEqual(newAny, toAny)
	}
}

func isNumericKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func eqColumnName(stmt *gorm.Statement, expr clause.Expression) (string, bool) {
	eq, ok := expr.(clause.Eq)
	if !ok {
		return "", false
	}
	switch c := eq.Column.(type) {
	case clause.Column:
		return stmt.NamingStrategy.ColumnName("", c.Name), true
	case string:
		return stmt.NamingStrategy.ColumnName("", c), true
	default:
		return "", false
	}
}

// Conflict lets users hook into version mismatches to merge or cancel.
type Conflict struct {
	OnVersionMismatch func(current any, diff map[string]Change) any
}

func (x Conflict) Name() string         { return conflictClauseName }
func (x Conflict) Build(clause.Builder) {}

func (x Conflict) MergeClause(c *clause.Clause) {
	if existing, ok := c.Expression.(Conflict); ok {
		if existing.OnVersionMismatch != nil && x.OnVersionMismatch != nil {
			chained := func(current any, diff map[string]Change) any {
				interim := existing.OnVersionMismatch(current, diff)
				reporter := newDiffReporter()
				if !cmp.Equal(current, interim, cmp.Reporter(reporter)) {
					return x.OnVersionMismatch(interim, reporter.Diff())
				}
				return x.OnVersionMismatch(interim, diff)
			}
			c.Expression = Conflict{OnVersionMismatch: chained}
			return
		}
		if existing.OnVersionMismatch != nil {
			c.Expression = existing
			return
		}
	}
	c.Expression = x
}

// NewOptimisticLock returns the plugin for db.Use(...)
func NewOptimisticLock(options ...ConfigOption) gorm.Plugin {
	cfg := &Config{
		tagName: "version",
	}
	for _, opt := range options {
		opt(cfg)
	}
	return &Plugin{
		Config: cfg,
	}
}
