package optimistic

import (
	"errors"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"log/slog"
	"reflect"
)

const (
	afterUpdateCallbackTarget = "gorm:after_update"
	contextKey                = "optimistic_lock:context_key"
)

var ErrOptimisticLock = errors.New("db record version mismatch")

type options struct {
	versionFieldName string
	disabled         bool
}
type Option interface {
	apply(*options)
}
type funcOption struct {
	f func(*options)
}

func (fdo *funcOption) apply(do *options) {
	fdo.f(do)
}

func newFuncOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

// VersionFieldName sets the version field name in the options configuration using the provided string value.
//
// Default value: `Version`
func VersionFieldName(versionFieldName string) Option {
	return newFuncOption(func(do *options) {
		do.versionFieldName = versionFieldName
	})
}

// Disabled disables optimstic locking
func Disabled() Option {
	return newFuncOption(func(do *options) {
		do.disabled = true
	})
}

// OptimisticLock plugin
type optimisticLock struct {
	*options
}

func NewOptimisticLock(opts ...Option) gorm.Plugin {
	ol := &options{}
	for _, o := range opts {
		o.apply(ol)
	}
	if ol.versionFieldName == "" {
		ol.versionFieldName = "Version"
	}

	return &optimisticLock{
		ol,
	}
}

// Name returns the plugin name
func (g optimisticLock) Name() string {
	return "optimistic_lock"
}

// Initialize registers the plugin with GORM
func (g optimisticLock) Initialize(db *gorm.DB) error {
	if g.disabled {
		return nil
	}
	return errors.Join(
		db.Callback().Update().After(afterUpdateCallbackTarget).Register(fmt.Sprintf("%s:%s", g.Name(), afterUpdateCallbackTarget), g.afterUpdate),
	)
}

func (g optimisticLock) afterUpdate(tx *gorm.DB) {
	// Check if the statement and schema are valid
	if g.disabled || tx.Statement == nil || tx.Statement.Schema == nil || tx.DryRun {
		return
	}
	var (
		stmt                          = tx.Statement
		primaryKeyField, versionField *schema.Field
		ctxValue                      *contextValue
	)
	if ctxValueRaw, cvok := tx.InstanceGet(contextKey); cvok {
		ctxValue = ctxValueRaw.(*contextValue)
	}
	if ctxValue == nil {
		return
	}

	primaryKeyField, versionField = ctxValue.PrimaryKeyField, ctxValue.VersionField

	var (
		toValue, fromValue     any
		toVersion, fromVersion Version
	)
	toValue = stmt.ReflectValue.Interface()
	if cv, zero := versionField.ValueOf(tx.Statement.Context, stmt.ReflectValue); zero {
		toVersion = 0
	} else {
		toVersion = cv.(Version)
	}

	from := reflect.ValueOf(ctxValue.Current)
	fromValue = from.Interface()

	if tx.RowsAffected == 0 && toVersion > 0 {
		from = reflect.New(stmt.ReflectValue.Type())
		var nLastValue any
		if from.CanAddr() {
			nLastValue = from.Addr().Interface()
		} else {
			nLastValue = from.Interface()
		}
		err := tx.Session(&gorm.Session{
			NewDB: true,
		}).Model(nLastValue).Where(fmt.Sprintf("%s = ?", primaryKeyField.DBName), ctxValue.PrimaryKey).First(nLastValue).Error
		if err != nil {
			_ = tx.AddError(ErrOptimisticLock)
			return
		}
		if from.CanAddr() {
			fromValue = from.Addr().Interface()
		} else {
			fromValue = from.Elem().Interface()
		}
	} else {
		// Diff the fields for debugging
		reporter := newDiffReporter()
		opts := []cmp.Option{
			//cmpopts.IgnoreFields(tx.Statement.ReflectValue.Interface(), "Version"),
			cmp.Reporter(reporter),
		}
		if !cmp.Equal(
			fromValue,
			toValue,
			opts...,
		) {
			if tx.Statement != nil && tx.Statement.Context != nil {
				slog.With("diff", reporter.Diff()).Log(tx.Statement.Context, slog.Level(-8), "differences detected")
			}
		}
	}

	if fv, isZero := versionField.ValueOf(tx.Statement.Context, from); isZero {
		fromVersion = 1
	} else {
		fromVersion = fv.(Version)
	}

	// Ensure the version has been increased
	if tx.RowsAffected == 0 {
		if fromVersion == toVersion {
			slog.With("fromVersion", fromVersion, "toVersion", toVersion).Debug("zero values")
			return
		} else if toVersion < fromVersion {
			slog.With("fromVersion", fromVersion, "toVersion", toVersion).Debug("stale")
			if !stmt.Unscoped {
				_ = tx.AddError(ErrOptimisticLock)
			}
			return
		}
	} else if toVersion != fromVersion+1 {
		if !stmt.Unscoped {
			slog.With("fromVersion", fromVersion, "toVersion", toVersion, "unscoped", stmt.Unscoped).Debug("unexpected fromVersion")
			_ = tx.AddError(ErrOptimisticLock)
		}
	}
}
