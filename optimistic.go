package optimistic

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"reflect"
)

const (
	afterUpdateCallbackTarget = "gorm:after_update"
	contextKey                = "optimistic_lock:context_key"
	conflictClauseName        = "optimistic:conflict"
)

var (
	TestDatabase      string
	ErrOptimisticLock = errors.New("db record version mismatch")
)

// Conflict lets you resolve a version‐mismatch by returning:
//
//	– nil       → cancel the update
//	– *Model    → re-run Updates(...) on that model
type Conflict struct {
	OnVersionMismatch func(current any, diff map[string]Change) any
}

func (x Conflict) MergeClause(c *clause.Clause) {
	if existing, ok := c.Expression.(Conflict); ok {
		// If both have OnVersionMismatch, chain them
		if existing.OnVersionMismatch != nil && x.OnVersionMismatch != nil {
			chained := func(current any, diff map[string]Change) any {
				interim := existing.OnVersionMismatch(current, diff)
				reporter := newDiffReporter()
				opts := []cmp.Option{
					//cmpopts.IgnoreFields(tx.Statement.ReflectValue.Interface(), "Version"),
					cmp.Reporter(reporter),
				}
				if !cmp.Equal(
					current,
					interim,
					opts...,
				) {
					return x.OnVersionMismatch(interim, reporter.Diff())
				}
				return x.OnVersionMismatch(interim, diff)
			}
			c.Expression = Conflict{OnVersionMismatch: chained}
			return
		}

		// Only existing has a handler
		if existing.OnVersionMismatch != nil {
			c.Expression = existing
			return
		}
	}

	// Fall back to receiver
	c.Expression = x
}

func (Conflict) Name() string { return conflictClauseName }
func (x Conflict) Build(clause.Builder) {
}

// Plugin implements gorm.Plugin and wires up callbacks.
type Plugin struct{}

func (*Plugin) Name() string {
	return "optimistic_lock"
}

func checkVersionField(db *gorm.DB, v reflect.Value, fieldName string) {
	if !v.IsValid() || v.Kind() != reflect.Struct {
		_ = db.AddError(ErrOptimisticLock)
		return
	}
	field := v.FieldByName(fieldName)
	if !field.IsValid() || field.Kind() != reflect.Uint64 {
		_ = db.AddError(ErrOptimisticLock)
		return
	}
	if field.Uint() != 1 {
		_ = db.AddError(ErrOptimisticLock)
	}
}

func (p *Plugin) Initialize(idb *gorm.DB) error {
	//
	// AFTER CREATE: initial version must be 1
	//
	_ = idb.Callback().
		Create().
		After("gorm:after_create").
		Register("optimistic:verify_create", func(db *gorm.DB) {
			stmt := db.Statement

			if db.DryRun || stmt.Unscoped {
				return
			}
			if _, ok := stmt.Clauses[optimisticLockingEnabled{}.Name()]; !ok {
				return
			}
			f := findVersionField(stmt.Schema)
			if f == nil {
				return
			}

			destVal := reflect.ValueOf(stmt.Dest)
			if destVal.Kind() == reflect.Pointer {
				destVal = destVal.Elem()
			}

			// Handle slice or single struct
			switch destVal.Kind() {
			case reflect.Struct:
				// Singular
				checkVersionField(db, destVal, f.Name)

			case reflect.Slice:
				// Batch
				for i := 0; i < destVal.Len(); i++ {
					elem := destVal.Index(i)
					if elem.Kind() == reflect.Pointer {
						elem = elem.Elem()
					}
					checkVersionField(db, elem, f.Name)
				}

			default:
				_ = db.AddError(fmt.Errorf("optimistic locking: unsupported Dest kind: %s", destVal.Kind()))
			}
		})

	//
	// AFTER UPDATE: verify version bumped by exactly +1
	//
	_ = idb.Callback().
		Update().
		After(afterUpdateCallbackTarget).
		Register("optimistic:verify_update", func(db *gorm.DB) {
			if db.DryRun || db.Statement.Unscoped {
				return
			}
			stmt := db.Statement
			if _, ok := stmt.Clauses[optimisticLockingEnabled{}.Name()]; !ok {
				return
			}

			f := findVersionField(stmt.Schema)
			if f == nil {
				return
			}
			oldAny, _ := db.InstanceGet(contextKey)
			oldVer, _ := oldAny.(Version)
			newAny, _ := f.ValueOf(stmt.Context, stmt.ReflectValue)
			newVer, _ := newAny.(Version)
			if newVer != oldVer+1 {
				_ = db.AddError(ErrOptimisticLock)
			}
		})

	//
	// AFTER UPDATE: if we got ErrOptimisticLock *and* the user attached a Conflict clause,
	// load the current row, call OnVersionMismatch, then either cancel or retry.
	//
	_ = idb.Callback().
		Update().
		After(afterUpdateCallbackTarget).
		Register("optimistic:resolve_conflict", func(db *gorm.DB) {
			if db.DryRun || db.Statement.Unscoped {
				return
			}
			stmt := db.Statement

			// only proceed if user did db.Clauses(optimistic.Conflict{…})
			cClause, ok := stmt.Clauses[conflictClauseName]
			if !ok {
				return
			}
			conflict := cClause.Expression.(Conflict)

			// only handle the version-mismatch error
			if !errors.Is(db.Error, ErrOptimisticLock) {
				return
			}

			// load fresh record by primary key(s)
			current := reflect.New(stmt.Schema.ModelType).Interface()
			for _, pf := range stmt.Schema.PrimaryFields {
				i := pf.ReflectValueOf(stmt.Context, stmt.ReflectValue).Interface()
				rv := reflect.Indirect(reflect.ValueOf(current))
				_ = pf.Set(stmt.Context, rv, i)
			}
			fresh := db.WithContext(context.Background()).Session(&gorm.Session{NewDB: true, SkipHooks: true})
			fresh.Error = nil
			fresh.RowsAffected = 0
			loadErr := fresh.First(current).Error
			if loadErr != nil {
				return
			}

			// let user resolve or cancel
			reporter := newDiffReporter()
			cmp.Diff(stmt.ReflectValue.Interface(), current, cmp.Reporter(reporter))
			rv := anyDeref(current)
			ptr := anyRef(rv)
			resolved := conflict.OnVersionMismatch(current, reporter.Diff())
			current = ptr

			if resolved == nil { // Error optimistic.ErrOptimisticLock
				db.Logger.Warn(db.Statement.Context, "[%s] ignored version mismatch, cancelled update, no rows affected", p.Name())
				db.RowsAffected = 0
			} else if cmp.Equal(current, resolved, cmp.Reporter(reporter.Reset())) { // Error optimistic.ErrOptimisticLock
				db.Logger.Warn(db.Statement.Context, "[%s] ignored version mismatch, accepted current value, no rows affected", p.Name())
				db.RowsAffected = 0
				reflect.Indirect(reflect.ValueOf(stmt.Model)).Set(reflect.Indirect(reflect.ValueOf(current)))
			} else { // Error is propagated from the update attempt
				// retry update with the resolved object
				retry := fresh.Session(&gorm.Session{NewDB: true, Logger: nil}).
					Model(resolved).
					Updates(resolved)
				reflect.Indirect(reflect.ValueOf(stmt.Model)).Set(reflect.Indirect(reflect.ValueOf(resolved)))
				db.Error = retry.Error
				db.RowsAffected = retry.RowsAffected
			}
		})

	return nil
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
		// wrapPointers[0] is true → wrap pointer again
	}

	// Create a new pointer to the value
	ptrVal := reflect.New(v.Type())
	ptrVal.Elem().Set(v)

	return ptrVal.Interface()
}

// NewOptimisticLock returns the plugin for db.Use(...)
func NewOptimisticLock() gorm.Plugin {
	return &Plugin{}
}
