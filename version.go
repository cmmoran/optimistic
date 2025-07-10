package optimistic

import (
	"database/sql/driver"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"reflect"
	"slices"
	"strconv"
)

type Version uint64

var versionType = reflect.TypeOf(Version(0))

//goland:noinspection GoMixedReceiverTypes
func (v Version) Equal(val interface{}) bool {
	switch value := val.(type) {
	case int:
		return uint64(v) == uint64(value)
	case uint64:
		return uint64(v) == value
	case int64:
		return uint64(v) == uint64(value)
	case Version:
		return uint64(v) == uint64(value)
	// Add other cases as needed
	default:
		return false
	}
}

//goland:noinspection GoMixedReceiverTypes
func (v *Version) UnmarshalJSON(bytes []byte) error {
	if bytes == nil || len(bytes) == 0 {
		*v = 0
		return nil
	}
	var (
		err   error
		tuint uint64
	)
	if tuint, err = strconv.ParseUint(string(bytes), 10, 64); err != nil {
		return err
	}
	*v = Version(tuint)
	return nil
}

//goland:noinspection GoMixedReceiverTypes
func (v Version) MarshalJSON() ([]byte, error) {
	return strconv.AppendUint(nil, uint64(v), 10), nil
}

//goland:noinspection GoMixedReceiverTypes
func (v *Version) CreateClauses(field *schema.Field) []clause.Interface {
	return []clause.Interface{VersionCreateClause{Field: field}}
}

//goland:noinspection GoMixedReceiverTypes
func (v Version) Value() (driver.Value, error) {
	return int64(v), nil
}

//goland:noinspection GoMixedReceiverTypes
func (v *Version) Scan(src interface{}) error {
	if src == nil {
		// NULL → leave at zero (or handle as you wish)
		*v = 0
		return nil
	}
	switch x := src.(type) {
	case int64:
		*v = Version(x)
		return nil
	case float64:
		*v = Version(uint64(x))
		return nil
	case []byte:
		// some drivers return []byte for numeric columns
		i, err := strconv.ParseUint(string(x), 10, 64)
		if err != nil {
			return fmt.Errorf("optimistic: cannot scan []byte %q into Version: %w", x, err)
		}
		*v = Version(i)
		return nil
	case string:
		// some drivers return string
		i, err := strconv.ParseUint(x, 10, 64)
		if err != nil {
			return fmt.Errorf("optimistic: cannot scan string %q into Version: %w", x, err)
		}
		*v = Version(i)
		return nil
	default:
		return fmt.Errorf("optimistic: cannot scan %T into Version", src)
	}
}

type VersionCreateClause struct {
	Field *schema.Field
}

func (v VersionCreateClause) Name() string {
	return ""
}

func (v VersionCreateClause) Build(clause.Builder) {
}

func (v VersionCreateClause) MergeClause(*clause.Clause) {
}

// ModifyStatement for CREATE: set version = 1 on insert.
func (v VersionCreateClause) ModifyStatement(stmt *gorm.Statement) {
	if stmt.DB.DryRun || stmt.Unscoped {
		return
	}
	if _, done := stmt.Clauses[optimisticLockingEnabled{}.Name()]; done {
		return
	}

	if stmt.Schema == nil || v.Field.DBName == "" {
		return
	}

	destVal := reflect.ValueOf(stmt.Dest)
	if destVal.Kind() == reflect.Pointer {
		destVal = destVal.Elem()
	}

	switch destVal.Kind() {
	case reflect.Struct:
		// Set version field for single object
		stmt.SetColumn(v.Field.DBName, 1)
		stmt.AddClauseIfNotExists(optimisticLockingEnabled{})

	case reflect.Slice:
		// Set version field for each element in the slice
		for i := 0; i < destVal.Len(); i++ {
			elem := destVal.Index(i)
			if elem.Kind() == reflect.Pointer {
				elem = elem.Elem()
			}
			if !elem.IsValid() || elem.Kind() != reflect.Struct {
				continue
			}
			_ = v.Field.Set(stmt.Context, elem, 1)
		}
		stmt.AddClauseIfNotExists(optimisticLockingEnabled{})
	default:

	}
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

// ModifyStatement rebuilds SET, WHERE and RETURNING clauses on every UPDATE.
func (v VersionUpdateClause) ModifyStatement(stmt *gorm.Statement) {
	clauses := stmt.DB.Callback().Update().Clauses
	supportsReturning := slices.Contains(clauses, "RETURNING")
	// ────────────────────────────────────────────────
	// 0b) skip on DryRun or Unscoped
	// ────────────────────────────────────────────────
	if stmt.DB.DryRun || stmt.Unscoped {
		return
	}

	// ────────────────────────────────────────────────
	// 1) only once per statement
	// ────────────────────────────────────────────────
	if _, applied := stmt.Clauses[optimisticLockingEnabled{}.Name()]; applied {
		return
	}

	// ────────────────────────────────────────────────
	// 2) locate the Version field
	// ────────────────────────────────────────────────
	versionField := stmt.Schema.LookUpField(v.Field.Name)
	if versionField == nil {
		return
	}

	// ────────────────────────────────────────────────
	// 3) ALWAYS use whatever Version is on the model,
	//    even if it's zero
	// ────────────────────────────────────────────────

	if cset, hasCustomSetClause := stmt.Clauses[clause.Set{}.Name()]; !hasCustomSetClause {
		// ────────────────────────────────────────────────
		// 4) build the SET clause + track which cols we updated
		// ────────────────────────────────────────────────
		var (
			set         clause.Set
			updatedCols []string
		)

		// 4a) map-based updates (single-col or map)
		if m, ok := stmt.Dest.(map[string]interface{}); ok {
			for col, val := range m {
				name := stmt.DB.NamingStrategy.ColumnName("", col)
				if name == versionField.DBName {
					continue
				}
				set = append(set, clause.Assignment{
					Column: clause.Column{Name: name},
					Value:  val,
				})
				updatedCols = append(updatedCols, name)
			}
		} else {
			// 4b) struct-based updates
			selectCols, restrict := stmt.SelectAndOmitColumns(false, true)
			for _, f := range stmt.Schema.Fields {
				if f.PrimaryKey || f.DBName == versionField.DBName || !f.Updatable {
					continue
				}
				sel := selectCols[f.DBName]
				if restrict {
					if !sel {
						continue
					}
				} else {
					if _, zero := f.ValueOf(stmt.Context, stmt.ReflectValue); zero && !sel {
						continue
					}
				}
				val, _ := f.ValueOf(stmt.Context, stmt.ReflectValue)
				set = append(set, clause.Assignment{
					Column: clause.Column{Name: f.DBName},
					Value:  val,
				})
				updatedCols = append(updatedCols, f.DBName)
			}
		}

		// 5) if nothing else changed, abort (avoid version-only)
		if len(set) == 0 {
			return
		}

		// ────────────────────────────────────────────────
		// 6) append exactly one bump: version = version + 1
		// ────────────────────────────────────────────────
		bumpExpr := clause.Expr{SQL: stmt.Quote(versionField.DBName) + " + 1"}
		set = append(set, clause.Assignment{
			Column: clause.Column{Name: versionField.DBName},
			Value:  bumpExpr,
		})
		updatedCols = append(updatedCols, versionField.DBName)

		// ────────────────────────────────────────────────
		// 7) install SET
		// ────────────────────────────────────────────────
		stmt.AddClause(set)
	} else {
		bumpExpr := clause.Expr{SQL: stmt.Quote(versionField.DBName) + " + 1"}
		cset.Expression = append(cset.Expression.(clause.Set), clause.Assignment{
			Column: clause.Column{Name: versionField.DBName},
			Value:  bumpExpr,
		})
	}

	wheresClause, hasCustomWhere := stmt.Clauses[clause.Where{}.Name()]
	if !hasCustomWhere || isTargetedModelUpdate(stmt) {
		// 1) pull out any existing WHERE expressions
		existingExprs := make([]clause.Expression, 0)
		if hasCustomWhere {
			if wh, ok := wheresClause.Expression.(clause.Where); ok {
				existingExprs = append(existingExprs, wh.Exprs...)
			}
		}

		// 2) determine old version
		verAny, _ := versionField.ValueOf(stmt.Context, stmt.ReflectValue)
		oldVer := verAny.(Version)
		stmt.DB.InstanceSet(contextKey, oldVer)

		// 3) start building a merged WHERE list
		whereExprs := make([]clause.Expression, 0, len(existingExprs)+2)
		whereExprs = append(whereExprs, existingExprs...)

		// 4) only add PK = ? if it isn’t already in existingExprs
		var hasPK bool
		var pkVal any
		for _, f := range stmt.Schema.Fields {
			if !f.PrimaryKey {
				continue
			}
			val, _ := f.ValueOf(stmt.Context, stmt.ReflectValue)
			pkVal = val

			for _, expr := range existingExprs {
				if eq, ok := expr.(clause.Eq); ok {
					if eqcn, eok := eqColumnName(eq); eok && eqcn == f.DBName {
						hasPK = true
						break
					}
				}
			}
			break
		}
		if !hasPK {
			whereExprs = append(whereExprs, clause.Eq{
				Column: clause.Column{Name: stmt.Schema.PrimaryFields[0].DBName},
				Value:  pkVal,
			})
		}

		// 5) always add version = oldVer
		whereExprs = append(whereExprs, clause.Eq{
			Column: clause.Column{Name: versionField.DBName},
			Value:  oldVer,
		})

		// 6) replace the WHERE clause with our merged list
		stmt.AddClause(clause.Where{Exprs: whereExprs})

		// 7) returning + hook as before
		if supportsReturning {
			stmt.AddClauseIfNotExists(clause.Returning{})
		}
		stmt.AddClauseIfNotExists(optimisticLockingEnabled{})
	}
}

func eqColumnName(expr clause.Expression) (string, bool) {
	eq, ok := expr.(clause.Eq)
	if !ok {
		return "", false
	}
	switch c := eq.Column.(type) {
	case clause.Column:
		return c.Name, true
	case string:
		return c, true
	default:
		return "", false
	}
}

type optimisticLockingEnabled struct {
}

func (v optimisticLockingEnabled) Name() string {
	return "OPTIMISTIC_LOCKING_ENABLED"
}

func (v optimisticLockingEnabled) Build(_ clause.Builder) {
}

func (v optimisticLockingEnabled) MergeClause(_ *clause.Clause) {
}

// loadOriginalVersion SELECTs just the version of the row we’re updating.
func (v VersionUpdateClause) loadOriginalVersion(stmt *gorm.Statement) (Version, error) {
	// fresh T
	origPtr := reflect.New(stmt.Schema.ModelType).Interface()
	tx := stmt.DB.
		Session(&gorm.Session{NewDB: true}).
		Model(origPtr).
		Select(v.Field.DBName)

	// carry over the PK filter
	for _, f := range stmt.Schema.Fields {
		if f.PrimaryKey {
			if pkVal, _ := f.ValueOf(stmt.Context, stmt.ReflectValue); !reflect.ValueOf(pkVal).IsZero() {
				tx = tx.Where(f.DBName+" = ?", pkVal)
			}
			break
		}
	}

	if err := tx.First(origPtr).Error; err != nil {
		return 0, ErrOptimisticLock
	}

	// extract the version value
	val := reflect.ValueOf(origPtr).Elem()
	anyVer, _ := v.Field.ValueOf(tx.Statement.Context, val)
	switch vt := anyVer.(type) {
	case Version:
		return vt, nil
	case uint64:
		return Version(vt), nil
	default:
		return 0, ErrOptimisticLock
	}
}

//goland:noinspection GoMixedReceiverTypes
func (v *Version) UpdateClauses(field *schema.Field) []clause.Interface {
	return []clause.Interface{VersionUpdateClause{Field: field}}
}

type VersionUpdateClause struct {
	Field *schema.Field
}

func (v VersionUpdateClause) Name() string {
	return ""
}

func (v VersionUpdateClause) Build(clause.Builder) {
}

func (v VersionUpdateClause) MergeClause(*clause.Clause) {
}

// scans the schema for the field whose Go type is optimistic.Version
func findVersionField(sch *schema.Schema) *schema.Field {
	if sch == nil {
		return nil
	}
	for _, f := range sch.Fields {
		if f.FieldType == versionType {
			return f
		}
	}
	return nil
}
