package optimistic_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/cmmoran/optimistic"
)

var (
	dbOnces = map[string]*sync.Once{
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

func runSuite(testDatabaseName string) func(f *testing.T) {
	tt := &errorF{
		l:  slog.Default(),
		db: testDatabaseName,
	}
	return func(f *testing.T) {
		f.Run(testDatabaseName, func(t *testing.T) {
			db, _ := setupDatabase(tt, false)

			// Create sets initial version to 1
			t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "CreateInitialVersion"), func(t *testing.T) {
				m := &TestModel{Description: "foo"}
				err := db.Create(m).Error
				require.NoError(t, err)
				require.EqualValues(t, 1, m.Version)
			})

			if testDatabaseName == testOracle {
				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "CreateWithUUIDVersion"), func(t *testing.T) {
					m := &TestOracleModelUUIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateChangesUUIDVersion"), func(t *testing.T) {
					m := &TestOracleModelUUIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateWithInvalidUUIDVersion"), func(t *testing.T) {
					m := &TestOracleModelUUIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")

					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					cver = m.Version

					m.Version = uuid.Nil
					m.Description = "baz"
					results := db.Updates(m)

					require.ErrorIs(t, results.Error, optimistic.ErrOptimisticLock)
					require.Zerof(t, results.RowsAffected, "expected no rows affected, got %d", results.RowsAffected)
					require.EqualValuesf(t, m.Version, uuid.Nil, "expected version to be unchanged")
					require.EqualValuesf(t, "baz", m.Description, "expected desciption on model to be unchanged")
					m.Version = cver

					// Correct the issue
					m.Description = "boo"
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					require.EqualValuesf(t, "boo", m.Description, "expected desciption on model to be unchanged")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "CreateWithULIDVersion"), func(t *testing.T) {
					m := &TestOracleModelULIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateChangesULIDVersion"), func(t *testing.T) {
					m := &TestOracleModelULIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					m.Description = "bar"
					cver := m.Version
					results := db.Updates(m)
					require.NoError(t, results.Error)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateWithInvalidULIDVersion"), func(t *testing.T) {
					m := &TestOracleModelULIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")

					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					cver = m.Version

					m.Version = ulid.Zero
					m.Description = "baz"
					results := db.Updates(m)

					require.ErrorIs(t, results.Error, optimistic.ErrOptimisticLock)
					require.Zerof(t, results.RowsAffected, "expected no rows affected, got %d", results.RowsAffected)
					require.EqualValuesf(t, m.Version, uuid.Nil, "expected version to be unchanged")
					require.EqualValuesf(t, "baz", m.Description, "expected desciption on model to be unchanged")
					m.Version = cver

					// Correct the issue
					m.Description = "boo"
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					require.EqualValuesf(t, "boo", m.Description, "expected desciption on model to be unchanged")
				})
			} else {
				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "CreateWithUUIDVersion"), func(t *testing.T) {
					m := &TestModelUUIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateChangesUUIDVersion"), func(t *testing.T) {
					m := &TestModelUUIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateWithInvalidUUIDVersion"), func(t *testing.T) {
					m := &TestModelUUIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")

					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					cver = m.Version

					m.Version = uuid.Nil
					m.Description = "baz"
					results := db.Updates(m)

					require.ErrorIs(t, results.Error, optimistic.ErrOptimisticLock)
					require.Zerof(t, results.RowsAffected, "expected no rows affected, got %d", results.RowsAffected)
					require.EqualValuesf(t, m.Version, uuid.Nil, "expected version to be unchanged")
					require.EqualValuesf(t, "baz", m.Description, "expected desciption on model to be unchanged")
					m.Version = cver

					// Correct the issue
					m.Description = "boo"
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					require.EqualValuesf(t, "boo", m.Description, "expected desciption on model to be unchanged")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "CreateWithULIDVersion"), func(t *testing.T) {
					m := &TestModelULIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateChangesULIDVersion"), func(t *testing.T) {
					m := &TestModelULIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateWithInvalidULIDVersion"), func(t *testing.T) {
					m := &TestModelULIDVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")

					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					cver = m.Version

					m.Version = ulid.Zero
					m.Description = "baz"
					results := db.Updates(m)

					require.ErrorIs(t, results.Error, optimistic.ErrOptimisticLock)
					require.Zerof(t, results.RowsAffected, "expected no rows affected, got %d", results.RowsAffected)
					require.EqualValuesf(t, m.Version, uuid.Nil, "expected version to be unchanged")
					require.EqualValuesf(t, "baz", m.Description, "expected desciption on model to be unchanged")
					m.Version = cver

					// Correct the issue
					m.Description = "boo"
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					require.EqualValuesf(t, "boo", m.Description, "expected desciption on model to be unchanged")
				})
			}

			if testDatabaseName == testMysql {
				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "CreateWithTimeVersion"), func(t *testing.T) {
					m := &TestMysqlModelTimeVersion{
						Description: "foo",
						StartTime:   db.NowFunc(),
					}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.False(t, m.Version.IsZero(), "expected version to be non-zero")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateWithInvalidTimeVersion"), func(t *testing.T) {
					m := &TestMysqlModelTimeVersion{
						Description: "foo",
						StartTime:   db.NowFunc(),
					}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")

					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					cver = m.Version

					m.Version = time.Time{}
					m.Description = "baz"
					results := db.Updates(m)

					require.ErrorIs(t, results.Error, optimistic.ErrOptimisticLock)
					require.Zerof(t, results.RowsAffected, "expected no rows affected, got %d", results.RowsAffected)
					require.True(t, m.Version.IsZero(), "expected version to be unchanged")
					require.EqualValuesf(t, "baz", m.Description, "expected desciption on model to be unchanged")
					m.Version = cver

					// Correct the issue
					m.Description = "boo"
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.False(t, m.Version.IsZero(), "expected version to be non-zero")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					require.EqualValuesf(t, "boo", m.Description, "expected desciption on model to be unchanged")
				})
			} else if testDatabaseName == testOracle {
				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "CreateWithTimeVersion"), func(t *testing.T) {
					m := &TestOracleModelTimeVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.False(t, m.Version.IsZero(), "expected version to be non-zero")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateTimeDirect"), func(t *testing.T) {
					_ = db.Exec("truncate table test_models_time_version")
					m := &TestOracleModelTimeVersion{
						Description: "boo",
						StartTime:   db.NowFunc(),
					}
					result := db.Create(m)
					require.NoError(t, result.Error)
					ntime := m.StartTime
					m = &TestOracleModelTimeVersion{
						Description: "thisone",
						StartTime:   db.NowFunc(),
					}
					result = db.Create(m)
					require.NoError(t, result.Error)
					nntime := m.StartTime
					m = &TestOracleModelTimeVersion{
						Description: "anotherone",
						StartTime:   db.NowFunc(),
					}
					result = db.Create(m)
					require.NoError(t, result.Error)
					m = &TestOracleModelTimeVersion{
						ID: m.ID,
					}
					result = db.First(m)
					require.NoError(t, result.Error)
					ftime := m.StartTime
					m = &TestOracleModelTimeVersion{}
					result = db.Model(m).Where(`start_time = ?`, ftime).First(m)
					require.NoError(t, result.Error)
					require.EqualValues(t, "anotherone", m.Description)
					require.EqualValues(t, result.RowsAffected, int64(1))

					var multi []*TestOracleModelTimeVersion
					result = db.Model(m).Find(&multi, `start_time >= ?`, ntime)
					require.NoError(t, result.Error)
					require.EqualValues(t, 3, len(multi))
					require.EqualValues(t, "boo", multi[0].Description)
					require.EqualValues(t, "thisone", multi[1].Description)
					require.EqualValues(t, "anotherone", multi[2].Description)

					result = db.Model(&TestOracleModelTimeVersion{}).Where(`start_time < ?`, nntime).Update("start_time", time.Now())
					require.NoError(t, result.Error)
					require.EqualValues(t, 1, result.RowsAffected)
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateWithInvalidTimeVersion"), func(t *testing.T) {
					m := &TestOracleModelTimeVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")

					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					cver = m.Version

					m.Version = time.Time{}
					m.Description = "baz"
					results := db.Updates(m)

					require.ErrorIs(t, results.Error, optimistic.ErrOptimisticLock)
					require.Zerof(t, results.RowsAffected, "expected no rows affected, got %d", results.RowsAffected)
					require.True(t, m.Version.IsZero(), "expected version to be unchanged")
					require.EqualValuesf(t, "baz", m.Description, "expected desciption on model to be unchanged")
					m.Version = cver

					// Correct the issue
					m.Description = "boo"
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.False(t, m.Version.IsZero(), "expected version to be non-zero")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					require.EqualValuesf(t, "boo", m.Description, "expected desciption on model to be unchanged")
				})
			} else if testDatabaseName == testPostgres {
				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "CreateWithTimeVersion"), func(t *testing.T) {
					m := &TestPostgresModelTimeVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.False(t, m.Version.IsZero(), "expected version to be non-zero")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateTimeDirect"), func(t *testing.T) {
					_ = db.Exec("truncate table test_models_time_version")
					m := &TestPostgresModelTimeVersion{
						Description: "boo",
						StartTime:   db.NowFunc(),
					}
					result := db.Create(m)
					require.NoError(t, result.Error)
					ntime := m.StartTime
					m = &TestPostgresModelTimeVersion{
						Description: "thisone",
						StartTime:   db.NowFunc(),
					}
					result = db.Create(m)
					require.NoError(t, result.Error)
					nntime := m.StartTime
					m = &TestPostgresModelTimeVersion{
						Description: "anotherone",
						StartTime:   db.NowFunc(),
					}
					result = db.Create(m)
					require.NoError(t, result.Error)
					m = &TestPostgresModelTimeVersion{
						ID: m.ID,
					}
					result = db.Find(m)
					require.NoError(t, result.Error)
					ftime := m.StartTime
					m = &TestPostgresModelTimeVersion{}
					result = db.Model(m).Where("start_time = ?", ftime).Find(m)
					require.NoError(t, result.Error)
					require.EqualValues(t, "anotherone", m.Description)

					var multi []*TestPostgresModelTimeVersion
					result = db.Model(m).Find(&multi, "start_time >= ?", ntime.Truncate(time.Microsecond))
					require.NoError(t, result.Error)
					require.EqualValues(t, 3, len(multi))
					require.EqualValues(t, "boo", multi[0].Description)
					require.EqualValues(t, "thisone", multi[1].Description)
					require.EqualValues(t, "anotherone", multi[2].Description)

					result = db.Model(&TestPostgresModelTimeVersion{}).Where("start_time < ?", nntime).Update("start_time", time.Now())
					require.NoError(t, result.Error)
					require.EqualValues(t, 1, result.RowsAffected)
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateWithInvalidTimeVersion"), func(t *testing.T) {
					m := &TestPostgresModelTimeVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")

					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					cver = m.Version

					m.Version = time.Time{}
					m.Description = "baz"
					results := db.Updates(m)

					require.ErrorIs(t, results.Error, optimistic.ErrOptimisticLock)
					require.Zerof(t, results.RowsAffected, "expected no rows affected, got %d", results.RowsAffected)
					require.True(t, m.Version.IsZero(), "expected version to be unchanged")
					require.EqualValuesf(t, "baz", m.Description, "expected desciption on model to be unchanged")
					m.Version = cver

					// Correct the issue
					m.Description = "boo"
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.False(t, m.Version.IsZero(), "expected version to be non-zero")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					require.EqualValuesf(t, "boo", m.Description, "expected desciption on model to be unchanged")
				})
			} else {
				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "CreateWithTimeVersion"), func(t *testing.T) {
					m := &TestModelTimeVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.False(t, m.Version.IsZero(), "expected version to be non-zero")
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateTimeDirect"), func(t *testing.T) {
					_ = db.Exec("truncate table \"test_models_time_version\"")
					m := &TestModelTimeVersion{
						Description: "boo",
						StartTime:   time.Now(),
					}
					result := db.Create(m)
					require.NoError(t, result.Error)
					ntime := m.StartTime
					m = &TestModelTimeVersion{
						Description: "thisone",
						StartTime:   time.Now(),
					}
					result = db.Create(m)
					require.NoError(t, result.Error)
					nntime := m.StartTime
					m = &TestModelTimeVersion{
						Description: "anotherone",
						StartTime:   time.Now(),
					}
					result = db.Create(m)
					require.NoError(t, result.Error)
					m.StartTime = time.Now()
					m = &TestModelTimeVersion{}
					result = db.Model(m).Find(m, "start_time = ?", ntime)
					require.NoError(t, result.Error)
					require.EqualValues(t, "boo", m.Description)

					var multi []*TestModelTimeVersion
					result = db.Model(m).Find(&multi, "start_time >= ?", ntime)
					require.NoError(t, result.Error)
					require.EqualValues(t, 3, len(multi))
					require.EqualValues(t, "boo", multi[0].Description)
					require.EqualValues(t, "thisone", multi[1].Description)
					require.EqualValues(t, "anotherone", multi[2].Description)

					result = db.Model(&TestModelTimeVersion{}).Where("start_time < ?", nntime).Update("start_time", time.Now())
					require.NoError(t, result.Error)
					require.EqualValues(t, 2, result.RowsAffected)
				})

				t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateWithInvalidTimeVersion"), func(t *testing.T) {
					m := &TestModelTimeVersion{Description: "foo"}
					err := db.Create(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")

					m.Description = "bar"
					cver := m.Version
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.NotNil(t, m.Version, "expected version to be non-nil")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					cver = m.Version

					m.Version = time.Time{}
					m.Description = "baz"
					results := db.Updates(m)

					require.ErrorIs(t, results.Error, optimistic.ErrOptimisticLock)
					require.Zerof(t, results.RowsAffected, "expected no rows affected, got %d", results.RowsAffected)
					require.True(t, m.Version.IsZero(), "expected version to be unchanged")
					require.EqualValuesf(t, "baz", m.Description, "expected desciption on model to be unchanged")
					m.Version = cver

					// Correct the issue
					m.Description = "boo"
					err = db.Updates(m).Error
					require.NoError(t, err)
					require.False(t, m.Version.IsZero(), "expected version to be non-zero")
					require.NotEqual(t, cver, m.Version, "expected version to be different from previous version")
					require.EqualValuesf(t, "boo", m.Description, "expected desciption on model to be unchanged")
				})
			}

			// Update increments version
			t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateIncrementsVersion"), func(t *testing.T) {
				m := &TestModel{Description: "foo"}
				require.NoError(t, db.Create(m).Error)

				m.Description = "bar"
				require.NoError(t, db.Updates(m).Error)
				require.EqualValues(t, 2, m.Version)
			})
			// Update increments version
			t.Run(fmt.Sprintf("%s-%s", testDatabaseName, "UpdateTimeIncrementsVersion"), func(t *testing.T) {
				m := &TestModelWithTime{Description: "foo"}
				require.NoError(t, db.Create(m).Error)

				m.Activation = time.Now().UTC()
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
				require.EqualValues(t, 1, m.Version)

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
				require.NoError(t, db.Model(m).Select("description", "enabled").Updates(m).Error)
				require.NoError(t, db.Find(m).Error)
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

		})
	}
}

func TestOptimisticLockingSuite(f *testing.T) {
	l := slog.Default()
	for _, db := range dbs {
		switch db {
		case testSqlite:
		default:
			setupDatabase(&errorF{l: l, db: db})
		}
		f.Run("OptimisticLockSuite", runSuite(db))
		switch db {
		case testSqlite:
		default:
			if _, container := findDbContextInfo(testDbContexts[db]); container != nil {
				_ = container.Terminate(testDbContexts[db])
			}
		}
	}
}
