package optimistic_test

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	oracle "github.com/cmmoran/gorm-oracle"
	"github.com/cmmoran/optimistic"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	gpgx "gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var (
	testDbContext = context.Background()
	pgContainer   *postgres.PostgresContainer
	oraContainer  tc.Container
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
}

type errorF struct {
	l *slog.Logger
}

func (e *errorF) Errorf(format string, args ...interface{}) {
	e.l.Error(fmt.Sprintf(format, args...))
}
func TestMain(m *testing.M) {
	l := slog.Default()
	setupDatabase(&errorF{l: l})
	// Run tests
	exitCode := m.Run()

	switch optimistic.TestDatabase {
	case testOracle:
		_ = oraContainer.Terminate(testDbContext)
	case testPostgres:
		_ = pgContainer.Terminate(testDbContext)

	}

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

type ow struct{}

func (ow) Printf(s string, i ...interface{}) {
	fmt.Printf(fmt.Sprintf("%s\n", s), i...)
}

const (
	testOracle   = "oracle"
	testSqlite   = "sqlite"
	testPostgres = "postgres"
)

var (
	dbOnce sync.Once
)

func setupDatabase(t assert.TestingT) (*gorm.DB, context.Context) {
	switch optimistic.TestDatabase {
	case testOracle:
		dbOnce.Do(func() {
			testDbContext = startOracleDatabase(t)
		})
		return setupOracleDatabase(t)
	case testSqlite:
		return setupSqliteDatabase(t)
	case testPostgres:
		dbOnce.Do(func() {
			testDbContext = startPostgresDatabase()
		})
		return setupPostgresDatabase(t)
	}

	return setupSqliteDatabase(t)
}

func startPostgresDatabase() context.Context {
	l := slog.Default()
	var (
		err   error
		dbCtx = context.Background()
	)

	dbName := os.Getenv("POSTGRES_DB")
	dbUser := os.Getenv("POSTGRES_USER")
	dbPass := os.Getenv("POSTGRES_PASS")
	dbDriver := os.Getenv("POSTGRES_DRIVER")
	pgContainer, err = postgres.Run(
		dbCtx,
		"postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPass),
		postgres.WithSQLDriver(dbDriver),
		postgres.BasicWaitStrategies(),
	)

	if err != nil {
		l.With("error", err).Error("failed to start container")
	}

	return dbCtx
}

func setupPostgresDatabase(t assert.TestingT) (*gorm.DB, context.Context) {
	var db *gorm.DB
	dsn, _ := pgContainer.ConnectionString(context.Background())

	o := ow{}
	l := gormlogger.New(o, gormlogger.Config{
		SlowThreshold: time.Second,
		Colorful:      true,
		LogLevel:      gormlogger.Info,
	})
	gcfg := &gorm.Config{
		Logger: l,
		NowFunc: func() time.Time {
			return time.Now().UTC().Truncate(time.Microsecond)
		},
	}

	var err error
	db, err = gorm.Open(gpgx.Open(dsn), gcfg)
	assert.NoError(t, err, "failed to connect to postgres database")
	err = db.Use(optimistic.NewOptimisticLock())
	assert.NoError(t, err, "failed to set up optimistic locking")

	err = db.AutoMigrate(&TestModel{}, &TestModelPtr{}, &TestModelNoVersion{})
	assert.NoError(t, err, "failed to migrate models")

	//err = pgContainer.Snapshot(testDbContext)
	//assert.NoError(t, err, "failed to snapshot container")

	return db, testDbContext
}

func startOracleDatabase(t assert.TestingT) context.Context {
	ctx := context.Background()

	hostAccessPorts := make([]int, 0)
	oraPort := os.Getenv("ORA_PORT")
	if len(oraPort) > 0 {
		port, _ := strconv.Atoi(oraPort)
		hostAccessPorts = append(hostAccessPorts, port)
	}
	user := os.Getenv("ORA_USER")
	if user == "" {
		user = "gorm"
	}
	pass := os.Getenv("ORA_PASS")
	if pass == "" {
		pass = "gorm"
	}
	env := map[string]string{
		"ORACLE_PASSWORD":   pass,
		"APP_USER":          user,
		"APP_USER_PASSWORD": pass,
	}
	service := os.Getenv("ORA_SERVICE")
	if service != "" && service != "FREEPDB1" {
		env["ORACLE_DATABASE"] = service
	}
	req := tc.ContainerRequest{
		Image:           "gvenzl/oracle-free:slim",
		HostAccessPorts: hostAccessPorts,
		ExposedPorts:    []string{"1521/tcp"},
		Env:             env,
		WaitingFor:      wait.ForLog("Completed: ALTER DATABASE OPEN").WithStartupTimeout(2 * time.Minute),
	}

	var err error
	oraContainer, err = tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("Failed to start container: %v", err)
	}

	ip, err := oraContainer.Host(ctx)
	if err != nil {
		log.Fatalf("Failed to get container host: %v", err)
	}

	port, err := oraContainer.MappedPort(ctx, "1521")
	if err != nil {
		log.Fatalf("Failed to get mapped port: %v", err)
	}
	db, _ := setupOracleDatabase(&errorF{l: slog.Default()})
	err = db.Migrator().AutoMigrate(&TestModel{}, &TestModelPtr{}, &TestModelNoVersion{})
	assert.NoError(t, err, "failed to migrate models")
	sdb, _ := db.DB()
	_ = sdb.Close()
	fmt.Printf("Oracle Free is running at %s:%s\n", ip, port.Port())

	return ctx
}

func setupOracleDatabase(t assert.TestingT) (*gorm.DB, context.Context) {
	o := ow{}
	l := gormlogger.New(o, gormlogger.Config{
		SlowThreshold: time.Second,
		Colorful:      true,
		LogLevel:      gormlogger.Info,
	})

	var db *gorm.DB
	host, err := oraContainer.Host(testDbContext)
	assert.NoError(t, err, "Failed to get container host")

	port, err := oraContainer.MappedPort(testDbContext, "1521")
	assert.NoError(t, err, "Failed to get mapped port")

	service := os.Getenv("ORA_SERVICE")
	if service == "" {
		service = "FREEPDB1"
	}
	user := os.Getenv("ORA_USER")
	if user == "" {
		user = "gorm"
	}
	pass := os.Getenv("ORA_PASS")
	if pass == "" {
		pass = "gorm"
	}
	url := oracle.BuildUrl(
		host,
		port.Int(),
		service,
		user,
		pass,
		nil,
	)
	db, err = gorm.Open(oracle.New(oracle.Config{
		DSN:                     url,
		VarcharSizeIsCharLength: true,
		UseClobForTextType:      false,
		IgnoreCase:              true,
		NamingCaseSensitive:     true,
	}), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: false,
		IgnoreRelationshipsWhenMigrating:         false,
		NamingStrategy: schema.NamingStrategy{
			IdentifierMaxLength: 30,
		},
		Logger: l,
		NowFunc: func() time.Time {
			return time.Now().UTC()
		},
	})
	assert.NoError(t, err)
	err = db.Use(optimistic.NewOptimisticLock())
	assert.NoError(t, err)

	return db, context.Background()
}

func setupSqliteDatabase(t assert.TestingT) (*gorm.DB, context.Context) {
	o := ow{}
	l := gormlogger.New(o, gormlogger.Config{
		SlowThreshold: time.Second,
		Colorful:      true,
		LogLevel:      gormlogger.Info,
	})
	// Initialize a SQLite database in memory for testing
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:memdb-%s?mode=memory&cache=shared", uuid.New().String())), &gorm.Config{
		SkipDefaultTransaction: false,
		Logger:                 l,
		NowFunc:                time.Now().UTC,
	})
	assert.NoError(t, err)
	err = db.Use(optimistic.NewOptimisticLock())
	assert.NoError(t, err)

	// Migrate the schema for TestModel
	err = db.AutoMigrate(&TestModel{}, &TestModelPtr{}, &TestModelNoVersion{})
	assert.NoError(t, err)

	return db, context.Background()
}

func TestOptimisticLockingSuite(t *testing.T) {
	db, _ := setupDatabase(t)

	// Create sets initial version to 1
	t.Run("CreateInitialVersion", func(t *testing.T) {
		m := &TestModel{Description: "foo"}
		err := db.Create(m).Error
		require.NoError(t, err)
		require.EqualValues(t, 1, m.Version)
	})

	// Update increments version
	t.Run("UpdateIncrementsVersion", func(t *testing.T) {
		m := &TestModel{Description: "foo"}
		require.NoError(t, db.Create(m).Error)

		m.Description = "bar"
		require.NoError(t, db.Updates(m).Error)
		require.EqualValues(t, 2, m.Version)
	})

	// Map-based Updates increments version
	t.Run("MapUpdatesIncrementVersion", func(t *testing.T) {
		m := &TestModel{Description: "foo"}
		require.NoError(t, db.Create(m).Error)

		require.NoError(t, db.Model(m).Updates(map[string]any{"description": "baz"}).Error)
		require.EqualValues(t, 2, m.Version)
	})

	// Single-column Update increments version
	t.Run("SingleColumnUpdate", func(t *testing.T) {
		m := &TestModel{Description: "foo"}
		require.NoError(t, db.Create(m).Error)

		require.NoError(t, db.Model(m).Update("description", "qux").Error)
		require.EqualValues(t, 2, m.Version)
	})

	// DryRun does not persist or bump version
	t.Run("DryRunDoesNotIncrement", func(t *testing.T) {
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
	t.Run("UnscopedUpdateSkipsLock", func(t *testing.T) {
		m := &TestModel{Description: "foo"}
		require.NoError(t, db.Create(m).Error)

		m.Description = "unscope"
		require.NoError(t, db.Unscoped().Updates(m).Error)
		require.EqualValues(t, 1, m.Version)
	})

	// Zero-value fields without Select do not increment
	t.Run("ZeroValueFieldsNoIncrement", func(t *testing.T) {
		m := &TestModel{Description: "foo", Enabled: true}
		require.NoError(t, db.Create(m).Error)

		// clear fields but don't force update them
		m.Description = ""
		m.Enabled = false
		require.NoError(t, db.Updates(m).Error)
		require.EqualValues(t, 1, m.Version)
	})

	// Force-zero-value update via Select
	t.Run("ForceZeroValueUpdateWithSelect", func(t *testing.T) {
		m := &TestModel{Description: "foo", Enabled: true}
		require.NoError(t, db.Create(m).Error)

		m.Description = ""
		m.Enabled = false
		require.NoError(t, db.Select("description", "enabled").Updates(m).Error)
		require.EqualValues(t, 2, m.Version)
	})

	// Select("*") updates all values even zero values
	t.Run("SelectStarUpdatesAllEvenZeroValues", func(t *testing.T) {
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
	t.Run("PointerFieldUpdates", func(t *testing.T) {
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
	t.Run("ConflictResolutionCallback_ReturnUnchangedCurrent", func(t *testing.T) {
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

	t.Run("ConflictResolutionCallback_ReturnNil", func(t *testing.T) {
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

	t.Run("ConflictResolutionCallback_ReturnUpdatedCurrent", func(t *testing.T) {
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
	t.Run("JSONMarshalUnmarshalVersion", func(t *testing.T) {
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
}

func TestDifferentUpdates(t *testing.T) {
	db, _ := setupDatabase(t)

	model := &TestModel{Description: "Test"}
	result := db.Create(model)
	cmodel := &TestModel{ID: model.ID}
	assert.NoError(t, result.Error, "expecting no error when creating a new record")
	assert.True(t, model.Version.Equal(1), "expecting version to be 1")
	assert.Equal(t, "Test", model.Description, "expecting description to be Test")

	model.Description = "Test2"
	result = db.Updates(model)
	assert.NoError(t, result.Error, "expecting no error when creating a new record")
	assert.True(t, model.Version.Equal(2), "expecting version to be 2")
	assert.Equal(t, "Test2", model.Description, "expecting description to be Test2")

	result = db.Model(model).Updates(map[string]any{"description": "Test3"})
	assert.NoError(t, result.Error, "expecting no error when creating a new record")
	assert.True(t, model.Version.Equal(3), "expecting version to be 3")
	assert.Equal(t, "Test3", model.Description, "expecting description to be Test3")

	result = db.Model(model).Update("description", "Test4")
	assert.NoError(t, result.Error, "expecting no error when creating a new record")
	assert.True(t, model.Version.Equal(4), "expecting version to be 4")
	assert.Equal(t, "Test4", model.Description, "expecting description to be Test4")

	cmodel.Description = "Test2"
	cmodel.Version = 0
	result = db.Updates(cmodel)
	assert.ErrorIs(t, result.Error, optimistic.ErrOptimisticLock, "expecting error when updating a record")
}

func TestUpdateConditions(t *testing.T) {
	db, _ := setupDatabase(t)

	var results []*TestModel
	db.Session(&gorm.Session{AllowGlobalUpdate: true}).Model(&TestModel{}).Where("1 = 1")

	models := make([]*TestModel, 100)
	for i := 0; i < 100; i++ {
		models[i] = &TestModel{Description: fmt.Sprintf("Multiple-Test%d", i%9)}
	}
	result := db.Create(models)
	assert.NoError(t, result.Error, "expecting no error when creating new records")
	results = make([]*TestModel, 0)
	result = db.Model(&TestModel{}).Where("\"description\" = ?", "Multiple-Test4").Update("description", "Multiple-Test44")
	assert.NoError(t, result.Error, "expecting no error when updating a record")
	rowsAffected := int(result.RowsAffected)
	assert.NotZero(t, rowsAffected, "expecting at least one affected record")
	result = db.Find(&results, "\"description\" = ?", "Multiple-Test44")
	assert.NoError(t, result.Error, "expecting no error when fetching multiple records")
	assert.Lenf(t, results, rowsAffected, "expecting %d changed models for this update", rowsAffected)
	for _, model := range results {
		assert.Equalf(t, "Multiple-Test44", model.Description, "expecting description for %d to be 'Multiple-Test44' but was '%s'", model.ID, model.Description)
		assert.EqualValuesf(t, 2, model.Version, "expecting version for %d to be 2 but was %d", model.ID, model.Version)
	}

	ncm := &TestModel{Description: "TestFakeId"}
	result = db.Create(ncm)
	assert.NoError(t, result.Error, "expecting no error when creating a new record")
	result = db.Model(ncm).Where(gorm.Expr("\"id\" = ?", ncm.ID)).Updates(map[string]any{"description": "TestFakeId2"})
	assert.NoError(t, result.Error, "expecting no error when updating a record")
	assert.EqualValuesf(t, 2, ncm.Version, "expecting version to be 2")
	assert.EqualValuesf(t, "TestFakeId2", ncm.Description, "expecting version to be 2")

	checkedResult := &TestModel{}
	result = db.First(checkedResult, "\"description\" = ?", "Multiple-Test44")
	assert.NoError(t, result.Error, "expecting no error when fetching a specific")
	assert.Equal(t, "Multiple-Test44", checkedResult.Description, "expecting description to be Test44")

	concurrentResult := &TestModel{ID: checkedResult.ID}
	concurrentResult.Description = "Test442"
	result = db.Updates(concurrentResult)
	assert.ErrorIs(t, result.Error, optimistic.ErrOptimisticLock, "expecting error when updating a record with incorrect version")

	// simulate out-of-sync
	concurrentResult.Version = 0
	result = db.Clauses(optimistic.Conflict{
		OnVersionMismatch: func(current any, diff map[string]optimistic.Change) any {
			db.Logger.Warn(db.Statement.Context, "[optimistic] resolving conflict %v", diff)
			return nil
		},
	}).Updates(concurrentResult)
	assert.ErrorIs(t, result.Error, optimistic.ErrOptimisticLock, "expecting error when updating a record with incorrect version")
	assert.EqualValuesf(t, 0, concurrentResult.Version, "expecting version to be unchanged at 0")
	assert.EqualValuesf(t, "Test442", concurrentResult.Description, "expecting description to be 'Test442' was %s", concurrentResult.Description)

	// simulate out-of-sync
	concurrentResult.Version = 0
	result = db.Clauses(optimistic.Conflict{
		OnVersionMismatch: func(current any, diff map[string]optimistic.Change) any {
			db.Logger.Warn(db.Statement.Context, "[optimistic] resolving conflict %v", diff)
			return current
		},
	}).Updates(concurrentResult)
	assert.ErrorIs(t, result.Error, optimistic.ErrOptimisticLock, "expecting error when updating a record with incorrect version")
	assert.EqualValuesf(t, 2, concurrentResult.Version, "expecting version to be restored to current model persisted value at 2")
	assert.EqualValuesf(t, "Multiple-Test44", concurrentResult.Description, "expecting description to be 'Test442' was %s", concurrentResult.Description)

	concurrentResult.Version = 0
	result = db.Clauses(optimistic.Conflict{
		OnVersionMismatch: func(current any, diff map[string]optimistic.Change) any {
			db.Logger.Warn(db.Statement.Context, "[optimistic] resolving conflict %v", diff)
			currentobj := current.(*TestModel)
			currentobj.Description = "Conflict-Test442"
			return currentobj
		},
	}).Updates(concurrentResult)
	assert.NoError(t, result.Error, "expecting no error when resolving conflict")
	assert.EqualValuesf(t, 3, concurrentResult.Version, "expecting version to be updated to 3")
	assert.EqualValuesf(t, "Conflict-Test442", concurrentResult.Description, "expecting description to be 'Conflict-Test442' was %s", concurrentResult.Description)
}

func TestTransactionUpdate(t *testing.T) {
	dbx, _ := setupDatabase(t)

	testModel := &TestModel{Description: "Test"}
	dbx.Create(testModel)

	db := dbx.Begin()
	testModel.Description = "Updated Test"
	result := db.Updates(testModel)
	assert.NoError(t, result.Error, "expecting no error when updating test record")
	assert.EqualValues(t, 2, testModel.Version, "expecting version to be 2")
	assert.EqualValues(t, "Updated Test", testModel.Description, "expecting description to be updated")
	result = db.Rollback()
	assert.NoError(t, result.Error, "expecting no error when performing rollback")

	result = dbx.First(testModel)
	assert.NoError(t, result.Error, "expecting no error when fetching the record")
	assert.EqualValues(t, 1, testModel.Version, "expecting version to be 1")
	assert.EqualValues(t, "Test", testModel.Description, "expecting description to be 'Test'")
}

func TestUpdateSelectStar(t *testing.T) {
	db, _ := setupDatabase(t)

	testModel := &TestModel{Description: "Test", Code: 1}
	db.Create(testModel)

	testModel.Description = ""
	testModel.Code = 0
	result := db.Select("*").Updates(testModel)
	assert.NoError(t, result.Error, "expecting no error when updating all fields")
	assert.EqualValues(t, 2, testModel.Version, "expecting version to increment")
	assert.EqualValues(t, "", testModel.Description, "expecting description to be updated")
	assert.EqualValues(t, 0, testModel.Code, "expecting code to be updated")
}

func TestMultipleFieldUpdate(t *testing.T) {
	db, _ := setupDatabase(t)

	testModel := &TestModel{Description: "Test"}
	db.Create(testModel)

	testModel.Description = "Updated Test multiple fields"
	testModel.Code = 123
	result := db.Updates(testModel)
	assert.NoError(t, result.Error, "expecting no error when updating multiple fields")
	assert.EqualValues(t, 2, testModel.Version, "expecting version to increment")
}

func TestUpdateUsingUpdateMethod(t *testing.T) {
	setupDb := func(t *testing.T) *gorm.DB {
		db, _ := setupDatabase(t)
		return db
	}
	tests := []struct {
		name   string
		db     *gorm.DB
		model  func(*gorm.DB) *TestModel
		action func(*testing.T, *gorm.DB, *TestModel)
	}{
		{
			name: "when Update() single column",
			db:   setupDb(t),
			model: func(db *gorm.DB) *TestModel {
				tm := &TestModel{
					Description: "Test",
					Code:        1,
					Version:     1,
				}
				db.Session(&gorm.Session{}).Create(tm)
				return tm
			},
			action: func(t *testing.T, db *gorm.DB, model *TestModel) {
				tdb := db.Model(model).Update("description", "Test Update")
				assert.NoError(t, tdb.Error, "expecting no error when updating using Update method")
				assert.EqualValues(t, 1, tdb.RowsAffected, "expecting one row updated")
				assert.EqualValues(t, TestModel{
					ID:          model.ID,
					Description: "Test Update",
					Code:        1,
					Version:     2,
				}, *model, "expecting model description to be changed")
				tm := &TestModel{
					ID: model.ID,
				}
				tdb = db.First(tm)
				assert.NoError(t, tdb.Error, "expecting no error when updating using First method")
				assert.EqualValues(t, *model, *tm, "expecting models to match")
				expected := TestModel{
					ID:          model.ID,
					Description: "",
					Code:        0,
					Version:     0,
				}
				tm = &TestModel{
					ID: model.ID,
				}
				tdb = db.Model(tm).Update("description", "Test Update")
				assert.ErrorIs(t, tdb.Error, optimistic.ErrOptimisticLock, "expecting optimistic lock error")
				assert.EqualValues(t, 0, tdb.RowsAffected, "expecting one row updated")
				assert.EqualValues(t, expected, *tm, "expecting model to be unchanged")
			},
		},
		{
			name: "when Updates() with map",
			db:   setupDb(t),
			model: func(db *gorm.DB) *TestModel {
				tm := &TestModel{
					Description: "Test",
					Code:        1,
					Version:     1,
				}
				db.Create(tm)
				return tm
			},
			action: func(t *testing.T, db *gorm.DB, model *TestModel) {
				tdb := db.Model(model).Updates(map[string]any{
					"code":        2,
					"description": "Test Update",
				})
				assert.NoError(t, tdb.Error, "expecting no error when updating using Update method")
				assert.EqualValues(t, 1, tdb.RowsAffected, "expecting one row updated")
				assert.EqualValues(t, TestModel{
					ID:          model.ID,
					Description: "Test Update",
					Code:        2,
					Version:     2,
				}, *model, "expecting model to be changed")
				tm := &TestModel{
					ID: model.ID,
				}
				tdb = db.First(tm)
				assert.NoError(t, tdb.Error, "expecting no error when using First method")
				assert.EqualValues(t, *model, *tm, "expecting models to match")
				tm = &TestModel{
					ID: model.ID,
				}
				tdb = db.Model(tm).Updates(map[string]any{
					"code":        3,
					"description": "Test Update",
				})
				assert.ErrorIs(t, tdb.Error, optimistic.ErrOptimisticLock, "expecting optimistic lock error")
				assert.EqualValues(t, 0, tdb.RowsAffected, "expecting one row updated")

				tdb = db.Model(TestModel{ID: model.ID}).First(tm)
				assert.NoError(t, tdb.Error, "expecting no error when using First method")
				assert.EqualValues(t, *tm, *model, "expecting models to match")
			},
		},
		{
			name: "when Updates() with map and Unscoped()",
			db:   setupDb(t),
			model: func(db *gorm.DB) *TestModel {
				tm := &TestModel{
					Description: "Test",
					Code:        1,
					Version:     1,
				}
				db.Create(tm)
				return tm
			},
			action: func(t *testing.T, db *gorm.DB, model *TestModel) {
				expected := TestModel{
					ID:          model.ID,
					Code:        4,
					Description: "Test Update",
					Version:     99,
				}
				tdb := db.Unscoped().Model(model).Updates(map[string]any{
					"code":        4,
					"description": "Test Update",
					"version":     99,
				})
				assert.NoError(t, tdb.Error, "expecting no error")
				assert.EqualValues(t, 1, tdb.RowsAffected, "expecting one row updated")
				assert.EqualValues(t, expected, *model, "expecting model to be updated")
				tm := &TestModel{
					ID: model.ID,
				}
				tdb = db.Unscoped().Model(tm).Updates(map[string]any{
					"code":        4,
					"description": "Test Update",
					"version":     99,
				})
				assert.NoError(t, tdb.Error, "expecting no error")
				assert.EqualValues(t, 1, tdb.RowsAffected, "expecting one row updated")
				assert.EqualValues(t, expected, *tm, "expecting model to be updated")

			},
		},
		{
			name: "when Updates() with map and Conflict clause",
			db:   setupDb(t),
			model: func(db *gorm.DB) *TestModel {
				tm := &TestModel{
					Description: "Test",
					Code:        1,
					Version:     1,
				}
				db.Create(tm)
				return tm
			},
			action: func(t *testing.T, db *gorm.DB, model *TestModel) {
				tdb := db.Model(model).Updates(map[string]any{
					"code":        4,
					"description": "Test Update",
				})
				assert.NoError(t, tdb.Error, "expecting no error")
				assert.EqualValues(t, 1, tdb.RowsAffected, "expecting one row updated")
				assert.EqualValues(t, 4, model.Code, "expecting code to be 4")
				assert.EqualValues(t, "Test Update", model.Description, "expecting description to be 'Test Update")
				tm := &TestModel{
					ID: model.ID,
				}
				tdb = db.Model(tm).Updates(map[string]any{
					"code":        5,
					"description": "Test Reupdated",
				})
				assert.ErrorIs(t, tdb.Error, optimistic.ErrOptimisticLock, "expecting optimistic lock error")
				assert.EqualValues(t, 0, tdb.RowsAffected, "expecting zero rows updated")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(vt *testing.T) {
			m := tt.model(tt.db)
			if tt.action != nil {
				tt.action(vt, tt.db, m)
			}
			if vt.Failed() {
				t.Fail()
				t.Errorf("TestUpdateUsingUpdateMethod() = %s", tt.name)
				return
			}
		})
		if t.Failed() {
			break
		}
	}
}
