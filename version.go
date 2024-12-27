package optimistic

import (
	"golang.org/x/exp/maps"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"gorm.io/gorm/utils"
	"log/slog"
	"reflect"
	"strconv"
	"sync"
)

type Version uint64

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
func (v Version) Value() uint64 {
	return uint64(v)
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

func (v VersionCreateClause) ModifyStatement(stmt *gorm.Statement) {
	switch stmt.ReflectValue.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < stmt.ReflectValue.Len(); i++ {
			v.setVersionColumn(stmt, stmt.ReflectValue.Index(i))
		}
	case reflect.Struct:
		v.setVersionColumn(stmt, stmt.ReflectValue)
	default:
		panic("unhandled default case")
	}
}

func (v VersionCreateClause) setVersionColumn(stmt *gorm.Statement, reflectValue reflect.Value) {
	var value Version = 1
	if val, zero := v.Field.ValueOf(stmt.Context, reflectValue); !zero {
		if version, ok := val.(Version); ok {
			value = version
		}
	}
	if err := v.Field.Set(stmt.Context, reflectValue, value); err != nil {
		slog.With("error", err).Error("failed to set version column")
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

func (v VersionUpdateClause) ModifyStatement(stmt *gorm.Statement) {
	if _, ok := stmt.Clauses["version_enabled"]; ok {
		return
	}
	var (
		modelSchema                   = stmt.Schema
		ok                            bool
		primaryKeyField, versionField *schema.Field
	)

	primaryKeyField, versionField, ok = v.hasFields(modelSchema)
	if !ok {
		return
	}
	var currentValue any
	if stmt.ReflectValue.CanAddr() {
		elem := reflect.New(stmt.ReflectValue.Type()).Elem()
		elem.Set(stmt.ReflectValue)
		currentValue = elem.Interface()
	} else {
		currentValue = stmt.ReflectValue.Elem().Interface()
	}

	pkey, _ := primaryKeyField.ValueOf(stmt.Context, stmt.ReflectValue)
	ver, _ := versionField.ValueOf(stmt.Context, stmt.ReflectValue)

	if reflect.ValueOf(ver).IsZero() || reflect.ValueOf(pkey).IsZero() {
		last := reflect.New(reflect.ValueOf(currentValue).Type()).Elem()
		var nLastValue any
		if last.CanAddr() {
			nLastValue = last.Addr().Interface()
		} else {
			nLastValue = last.Interface()
		}
		if wc, exists := stmt.Clauses["WHERE"]; exists {
			err := stmt.DB.Session(&gorm.Session{
				NewDB: true,
			}).Model(nLastValue).Clauses(wc.Expression).First(nLastValue).Error
			if err != nil {
				_ = stmt.AddError(ErrOptimisticLock)
				return
			}
		}

		if last.CanAddr() {
			currentValue = last.Interface()
		} else {
			currentValue = last.Elem().Interface()
		}

		pkey, _ = primaryKeyField.ValueOf(stmt.Context, last)
		ver, _ = versionField.ValueOf(stmt.Context, last)
	}
	ctxValue := &contextValue{
		Current:         currentValue,
		PrimaryKey:      pkey,
		Version:         ver.(Version),
		PrimaryKeyField: primaryKeyField,
		VersionField:    versionField,
	}
	stmt.DB.InstanceSet(contextKey, ctxValue)

	values := maps.Values(stmt.Clauses)

	if clauseNames, err := ExtractField[clause.Clause, string](values, "Name"); err == nil && !utils.Contains(clauseNames, "RETURNING") {
		stmt.AddClause(clause.Returning{})
	}

	if c, sok := stmt.Clauses["WHERE"]; sok {
		if where, wok := c.Expression.(clause.Where); wok && len(where.Exprs) > 1 {
			for _, expr := range where.Exprs {
				if orCond, ook := expr.(clause.OrConditions); ook && len(orCond.Exprs) == 1 {
					where.Exprs = []clause.Expression{clause.And(where.Exprs...)}
					c.Expression = where
					stmt.Clauses["WHERE"] = c
					break
				}
			}
		}
	}

	if !stmt.Unscoped {
		if val, zero := v.Field.ValueOf(stmt.Context, stmt.ReflectValue); !zero {
			if version, vok := val.(Version); vok {
				stmt.AddClause(clause.Where{Exprs: []clause.Expression{
					clause.Eq{Column: clause.Column{Table: clause.CurrentTable, Name: v.Field.DBName}, Value: version},
				}})
			}
		} else {
			stmt.AddClause(clause.Where{Exprs: []clause.Expression{
				clause.Eq{Column: clause.Column{Table: clause.CurrentTable, Name: v.Field.DBName}, Value: 0},
			}})
		}
	}
	if len(stmt.Selects) > 0 && !utils.Contains(stmt.Selects, "*") && !utils.Contains(stmt.Selects, v.Field.DBName) {
		stmt.Selects = append(stmt.Selects, v.Field.DBName)
	}

	// convert struct to map[string]interface{}, we need to handle the version field with string, but which is an int64.
	dv := reflect.ValueOf(stmt.Dest)
	setVersion := true
	hasAnyNonZeroField := false
	selectColumns, restricted := stmt.SelectAndOmitColumns(false, true)
	if reflect.Indirect(dv).Kind() == reflect.Struct {

		sd, _ := schema.Parse(stmt.Dest, &sync.Map{}, stmt.DB.NamingStrategy)
		d := make(map[string]interface{})
		for _, field := range sd.Fields {
			if field.DBName == v.Field.DBName {
				continue
			}
			if field.DBName == "" {
				continue
			}

			if selectColVal, svok := selectColumns[field.DBName]; (svok && selectColVal) || (!svok && (!restricted || !stmt.SkipHooks)) {
				if field.AutoUpdateTime > 0 {
					continue
				}

				val, isZero := field.ValueOf(stmt.Context, dv)
				if (svok || !isZero) && field.Updatable {
					if !isZero {
						hasAnyNonZeroField = true
					}
					d[field.DBName] = val
				}
			}
		}

		stmt.Dest = d
		if len(d) == 0 || (len(d) > 0 && !hasAnyNonZeroField) {
			if enabled, sv := selectColumns[v.Field.DBName]; !enabled || !sv {
				setVersion = false
			}
			if dval, dok := d[v.Field.DBName]; dok && dval != nil {
				setVersion = false
			}
		}
	} else if reflect.Indirect(dv).Kind() == reflect.Map {
		if sd, sdok := stmt.Dest.(map[string]interface{}); sdok {
			if len(sd) > 0 {
				if ook, sv := selectColumns[v.Field.DBName]; !ook || !sv {
					setVersion = true
				}
				if dval, ook := sd[v.Field.DBName]; ook && dval != nil {
					setVersion = !stmt.Unscoped
				}
			}
		}
	}

	if setVersion {
		stmt.SetColumn(v.Field.DBName, clause.Expr{SQL: stmt.Quote(v.Field.DBName) + "+1"}, true)
		stmt.Clauses["version_enabled"] = clause.Clause{}
	}
}

func (v VersionUpdateClause) hasFields(modelSchema *schema.Schema) (*schema.Field, *schema.Field, bool) {
	versionField := modelSchema.LookUpField(v.Field.DBName)
	if versionField == nil {
		return nil, nil, false
	}

	primaryKeyField := modelSchema.PrioritizedPrimaryField

	return primaryKeyField, versionField, true
}
