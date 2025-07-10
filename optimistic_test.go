package optimistic_test

import (
	"context"
	"fmt"
	"github.com/cmmoran/optimistic"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"gorm.io/gorm"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
)

var (
	pgContainer    *postgres.PostgresContainer
	mysqlContainer *mysql.MySQLContainer
	oraContainer   tc.Container
	dbOnces        = map[string]*sync.Once{
		testOracle:   {},
		testPostgres: {},
		testMysql:    {},
		testSqlite:   {},
	}
	testDbContexts = map[string]context.Context{
		testOracle:   context.Background(),
		testPostgres: context.Background(),
		testMysql:    context.Background(),
		testSqlite:   context.Background(),
	}
	dbs []string
)

func init() {
	slog.SetDefault(
		slog.New(
			slog.NewJSONHandler(
				os.Stdout,
				&slog.HandlerOptions{Level: slog.LevelDebug},
			),
		),
	)

	if _, ok := os.LookupEnv("databases"); !ok {
		_ = os.Setenv("databases", testAll)
	}
}

type testingT interface {
	require.TestingT
	DB() string
}

type errorF struct {
	l  *slog.Logger
	db string
}

type ow struct {
	testingT
}

func (e *ow) Printf(s string, i ...interface{}) {
	fmt.Printf(fmt.Sprintf("%s\n", s), i...)
}

func (e *errorF) Errorf(format string, args ...interface{}) {
	e.l.Error(fmt.Sprintf(format, args...))
}

func (e *errorF) FailNow() {
	panic("fail now")
}

func (e *errorF) DB() string {
	return e.db
}

func TestMain(m *testing.M) {
	var (
		testsStr string
		ok       bool
	)
	if testsStr, ok = os.LookupEnv("databases"); !ok || strings.EqualFold(testsStr, "all") {
		dbs = []string{testSqlite, testOracle, testPostgres, testMysql}
	} else {
		dbs = strings.Split(testsStr, ",")
	}
	// Run tests
	exitCode := m.Run()

	os.Exit(exitCode)
}

type TestModelPtr struct {
	ID          uint64             `gorm:"<-:create;autoIncrement;primaryKey"`
	Description *string            `gorm:"type:text;"`
	Code        *uint64            `gorm:"type:numeric;"`
	Enabled     *bool              `gorm:"type:bool;"`
	Version     optimistic.Version `gorm:"type:numeric;not null;"`
}

type TestModel struct {
	ID          uint64             `gorm:"<-:create;autoIncrement;primaryKey"`
	Description string             `gorm:"type:text;"`
	Code        uint64             `gorm:"type:numeric;"`
	Enabled     bool               `gorm:"type:bool;"`
	Version     optimistic.Version `gorm:"type:numeric;not null;"`
}

type TestModelNoVersion struct {
	ID          uint64 `gorm:"<-:create;primaryKey"`
	Description string `gorm:"type:text;"`
	Code        uint64 `gorm:"type:numeric;"`
	Enabled     bool   `gorm:"type:bool;"`
}

func runSuite(f *testing.T, testDatabaseName string) {
	f.Run(fmt.Sprintf("%s-%s", testDatabaseName, "TestOptimisticLockingSuite"), func(t *testing.T) {
		tt := &errorF{
			l:  slog.Default(),
			db: testDatabaseName,
		}
		db, _ := setupDatabase(tt)

		// Create sets initial version to 1
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "CreateInitialVersion"), func(t *testing.T) {
			m := &TestModel{Description: "foo"}
			err := db.Create(m).Error
			require.NoError(t, err)
			require.EqualValues(t, 1, m.Version)
		})

		// Update increments version
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateIncrementsVersion"), func(t *testing.T) {
			m := &TestModel{Description: "foo"}
			require.NoError(t, db.Create(m).Error)

			m.Description = "bar"
			require.NoError(t, db.Updates(m).Error)
			require.EqualValues(t, 2, m.Version)
		})

		// Map-based Updates increments version
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "MapUpdatesIncrementVersion"), func(t *testing.T) {
			m := &TestModel{Description: "foo"}
			require.NoError(t, db.Create(m).Error)

			require.NoError(t, db.Model(m).Updates(map[string]any{"description": "baz"}).Error)
			require.EqualValues(t, 2, m.Version)
			require.EqualValues(t, "baz", m.Description)
		})

		// Single-column Update increments version
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "SingleColumnUpdate"), func(t *testing.T) {
			m := &TestModel{Description: "foo"}
			require.NoError(t, db.Create(m).Error)

			require.NoError(t, db.Model(m).Update("description", "qux").Error)
			require.EqualValues(t, 2, m.Version)
		})

		// DryRun does not persist or bump version
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "DryRunDoesNotIncrement"), func(t *testing.T) {
			m := &TestModel{Description: "foo"}
			require.NoError(t, db.Create(m).Error)

			m.Description = "dryrun"
			sess := db.Session(&gorm.Session{DryRun: true})
			require.NoError(t, sess.Updates(m).Error)
			require.EqualValues(t, 1, m.Version)

			// verify underlying record unchanged
			m2 := &TestModel{ID: m.ID}
			require.NoError(t, db.First(m2).Error)
			require.EqualValues(t, 1, m2.Version)
		})

		// Unscoped update skips optimistic locking
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UnscopedUpdateSkipsLock"), func(t *testing.T) {
			m := &TestModel{Description: "foo"}
			require.NoError(t, db.Create(m).Error)

			m.Description = "unscope"
			require.NoError(t, db.Unscoped().Updates(m).Error)
			require.EqualValues(t, 1, m.Version)
		})

		// Zero-value fields without Select do not increment
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "ZeroValueFieldsNoIncrement"), func(t *testing.T) {
			m := &TestModel{Description: "foo", Enabled: true}
			require.NoError(t, db.Create(m).Error)

			// clear fields but don't force update them
			m.Description = ""
			m.Enabled = false
			require.NoError(t, db.Updates(m).Error)
			require.EqualValues(t, 1, m.Version)
		})

		// Force-zero-value update via Select
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "ForceZeroValueUpdateWithSelect"), func(t *testing.T) {
			m := &TestModel{Description: "foo", Enabled: true}
			require.NoError(t, db.Create(m).Error)

			m.Description = ""
			m.Enabled = false
			require.NoError(t, db.Select("description", "enabled").Updates(m).Error)
			require.EqualValues(t, 2, m.Version)
		})

		// Select("*") updates all values even zero values
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "SelectStarUpdatesAllEvenZeroValues"), func(t *testing.T) {
			m := &TestModel{Description: "foo", Enabled: true}
			require.NoError(t, db.Create(m).Error)

			m.Description = ""
			m.Code = 0
			m.Enabled = false
			require.NoError(t, db.Select("*").Updates(m).Error)
			require.EqualValues(t, 2, m.Version, "expecting version to increment")
			require.EqualValues(t, "", m.Description, "expecting description to be updated")
			require.EqualValues(t, 0, m.Code, "expecting code to be updated")
		})

		// Pointer field handling
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "PointerFieldUpdates"), func(t *testing.T) {
			d := "initial"
			e := true
			mp := &TestModelPtr{Description: &d, Enabled: &e}
			require.NoError(t, db.Create(mp).Error)
			require.EqualValues(t, 1, mp.Version)

			// update via pointer nil
			mp.Description = nil
			require.NoError(t, db.Select("description").Updates(mp).Error)
			require.EqualValues(t, 2, mp.Version)
		})

		// Conflict resolution hook
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "ConflictResolutionCallback_ReturnUnchangedCurrent"), func(t *testing.T) {
			m := &TestModel{Description: "foo"}
			require.NoError(t, db.Create(m).Error)

			// simulate concurrent edit
			m2 := &TestModel{ID: m.ID, Description: "bar", Version: 0}
			err := db.Clauses(optimistic.Conflict{
				OnVersionMismatch: func(current any, diffs map[string]optimistic.Change) any {
					// restore current
					// m2 will be set exactly to the current value overwriting m2 and any changes to it completely
					return current
				},
			}).Updates(m2).Error
			require.ErrorIs(t, err, optimistic.ErrOptimisticLock)
			require.EqualValues(t, 1, m2.Version, "version restored to current persisted value")
		})

		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "ConflictResolutionCallback_ReturnNil"), func(t *testing.T) {
			m := &TestModel{Description: "foo"}
			require.NoError(t, db.Create(m).Error)

			// simulate concurrent edit
			m2 := &TestModel{ID: m.ID, Description: "bar", Version: 0}
			err := db.Clauses(optimistic.Conflict{
				OnVersionMismatch: func(current any, diffs map[string]optimistic.Change) any {
					// abort
					// m2 will be untouched, there will be no changes to the db, error will be returned since this handler returned nil (implying do nothing)
					return nil
				},
			}).Updates(m2).Error
			require.ErrorIs(t, err, optimistic.ErrOptimisticLock)
			require.EqualValues(t, 0, m2.Version, "version is untouched from before update attempt")
			require.EqualValues(t, "bar", m2.Description, "description is untouched from before update attempt")
		})

		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "ConflictResolutionCallback_ReturnUpdatedCurrent"), func(t *testing.T) {
			m := &TestModel{Description: "foo"}
			require.NoError(t, db.Create(m).Error)

			// simulate concurrent edit
			m2 := &TestModel{ID: m.ID, Description: "bar", Version: 0}
			err := db.Clauses(optimistic.Conflict{
				OnVersionMismatch: func(current any, diffs map[string]optimistic.Change) any {
					// merge changes
					// return value will be submitted to the DB, the full result will be stored into m2
					cv := current.(*TestModel)
					cv.Description = "baz"
					return cv
				},
			}).Updates(m2).Error
			require.NoError(t, err, "expecting no error when resolving conflict with updated value")
			require.EqualValues(t, 2, m2.Version, "expecting version updated because value was persisted")
			require.EqualValues(t, "baz", m2.Description, "expecting description updated to merged persisted value")
		})

		// JSON marshalling/unmarshalling of Version
		t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "JSONMarshalUnmarshalVersion"), func(t *testing.T) {
			var v optimistic.Version
			// marshal
			b, err := v.MarshalJSON()
			require.NoError(t, err)
			require.Equal(t, []byte("0"), b)

			// unmarshal
			err = v.UnmarshalJSON([]byte("5"))
			require.NoError(t, err)
			require.EqualValues(t, 5, v)
		})
	})

}

func TestOptimisticLockingSuite(f *testing.T) {
	l := slog.Default()
	for _, db := range dbs {
		switch db {
		case testSqlite:
		default:
			setupDatabase(&errorF{l: l, db: db})
		}
		runSuite(f, db)
		switch db {
		case testOracle:
			_ = oraContainer.Terminate(testDbContexts[db])
		case testPostgres:
			_ = pgContainer.Terminate(testDbContexts[db])
		case testMysql:
			_ = mysqlContainer.Terminate(testDbContexts[db])
		}
	}
}
