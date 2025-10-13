package optimistic_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	oracle "github.com/cmmoran/gorm-oracle"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	gmysql "gorm.io/driver/mysql"
	gpgx "gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	"github.com/cmmoran/optimistic"
)

const (
	testAll      = "all"
	testOracle   = "oracle"
	testSqlite   = "sqlite"
	testPostgres = "postgres"
	testMysql    = "mysql"
)

type TestModelPtr struct {
	ID          uint64  `gorm:"<-:create;autoIncrement;primaryKey"`
	Description *string `gorm:"type:text;"`
	Code        *uint64 `gorm:"type:numeric;"`
	Enabled     *bool   `gorm:"type:bool;"`
	Version     uint64  `gorm:"type:numeric;not null;version"`
}

func (TestModelPtr) TableName() string {
	return "test_models_ptr"
}

type TestModel struct {
	ID          uint64 `gorm:"<-:create;autoIncrement;primaryKey"`
	Description string `gorm:"type:text;"`
	Code        uint64 `gorm:"type:numeric;"`
	Enabled     bool   `gorm:"type:bool;"`
	Version     uint64 `gorm:"type:numeric;not null;version"`
}

func (TestModel) TableName() string {
	return "test_models"
}

type TestModelWithTime struct {
	ID          uint64    `gorm:"<-:create;autoIncrement;primaryKey"`
	Activation  time.Time `gorm:"autoUpdateTime"`
	Description string    `gorm:"type:text;"`
	Code        uint64    `gorm:"type:numeric;"`
	Enabled     bool      `gorm:"type:bool;"`
	Version     uint64    `gorm:"type:numeric;not null;version"`
}

func (TestModelWithTime) TableName() string {
	return "test_models_with_time"
}

type TestModelNoVersion struct {
	ID          uint64 `gorm:"<-:create;primaryKey"`
	Description string `gorm:"type:text;"`
	Code        uint64 `gorm:"type:numeric;"`
	Enabled     bool   `gorm:"type:bool;"`
}

func (TestModelNoVersion) TableName() string {
	return "test_models_no_version"
}

type TestModelUUIDVersion struct {
	ID          uint64    `gorm:"<-:create;primaryKey"`
	Description string    `gorm:"type:text;"`
	Code        uint64    `gorm:"type:numeric;"`
	Enabled     bool      `gorm:"type:bool;"`
	Version     uuid.UUID `gorm:"not null;version:uuid"`
}

func (TestModelUUIDVersion) TableName() string {
	return "test_models_uuid_version"
}

type TestOracleModelUUIDVersion struct {
	ID          uint64    `gorm:"<-:create;primaryKey"`
	Description string    `gorm:"type:text;"`
	Code        uint64    `gorm:"type:numeric;"`
	Enabled     bool      `gorm:"type:bool;"`
	Version     uuid.UUID `gorm:"not null;version:uuid"`
}

func (TestOracleModelUUIDVersion) TableName() string {
	return "test_models_uuid_version"
}

type TestModelULIDVersion struct {
	ID          uint64    `gorm:"<-:create;primaryKey"`
	Description string    `gorm:"type:text;"`
	Code        uint64    `gorm:"type:numeric;"`
	Enabled     bool      `gorm:"type:bool;"`
	Version     ulid.ULID `gorm:"not null;version"`
}

func (TestModelULIDVersion) TableName() string {
	return "test_models_ulid_version"
}

type TestOracleModelULIDVersion struct {
	ID          uint64    `gorm:"<-:create;primaryKey"`
	Description string    `gorm:"type:text;"`
	Code        uint64    `gorm:"type:numeric;"`
	Enabled     bool      `gorm:"type:bool;"`
	Version     ulid.ULID `gorm:"type:ulid;not null;version:ulid"`
}

func (TestOracleModelULIDVersion) TableName() string {
	return "test_models_ulid_version"
}

type TestModelTimeVersion struct {
	ID          uint64    `gorm:"<-:create;primaryKey"`
	Description string    `gorm:"type:text;"`
	Code        uint64    `gorm:"type:numeric;"`
	Enabled     bool      `gorm:"type:bool;"`
	StartTime   time.Time `gorm:"type:datetime"`
	Version     time.Time `gorm:"not null;version"`
}

func (TestModelTimeVersion) TableName() string {
	return "test_models_time_version"
}

type TestMysqlModelTimeVersion struct {
	ID          uint64    `gorm:"<-:create;primaryKey"`
	Description string    `gorm:"type:text;"`
	Code        uint64    `gorm:"type:numeric;"`
	Enabled     bool      `gorm:"type:bool;"`
	StartTime   time.Time `gorm:"type:timestamp(6)"`
	Version     time.Time `gorm:"type:timestamp(6);not null;version"`
}

func (TestMysqlModelTimeVersion) TableName() string {
	return "test_models_time_version"
}

type TestOracleModelTimeVersion struct {
	ID          uint64    `gorm:"<-:create;primaryKey"`
	Description string    `gorm:"type:text;"`
	Code        uint64    `gorm:"type:numeric;"`
	Enabled     bool      `gorm:"type:bool;"`
	StartTime   time.Time `gorm:"type:TIMESTAMP WITH TIME ZONE"`
	Version     time.Time `gorm:"type:TIMESTAMP WITH TIME ZONE;not null;version"`
}

func (TestOracleModelTimeVersion) TableName() string {
	return "test_models_time_version"
}

type TestPostgresModelTimeVersion struct {
	ID          uint64    `gorm:"<-:create;primaryKey"`
	Description string    `gorm:"type:text;"`
	Code        uint64    `gorm:"type:numeric;"`
	Enabled     bool      `gorm:"type:bool;"`
	StartTime   time.Time `gorm:"type:timestamp(6);"`
	Version     time.Time `gorm:"type:timestamp(6);not null;version"`
}

func (TestPostgresModelTimeVersion) TableName() string {
	return "test_models_time_version"
}

var baseTestModels = []interface{}{
	&TestModel{},
	&TestModelWithTime{},
	&TestModelPtr{},
	&TestModelNoVersion{},
	&TestModelUUIDVersion{},
	&TestModelULIDVersion{},
	&TestModelTimeVersion{},
}

var testModels = map[string][]interface{}{
	"sqlite": baseTestModels,
	"mysql": {
		&TestModel{},
		&TestModelWithTime{},
		&TestModelPtr{},
		&TestModelNoVersion{},
		&TestModelUUIDVersion{},
		&TestModelULIDVersion{},
		&TestMysqlModelTimeVersion{},
	},
	"oracle": {
		&TestModel{},
		&TestModelWithTime{},
		&TestModelPtr{},
		&TestModelNoVersion{},
		&TestOracleModelUUIDVersion{},
		&TestOracleModelULIDVersion{},
		&TestOracleModelTimeVersion{},
	},
	"postgres": {
		&TestModel{},
		&TestModelWithTime{},
		&TestModelPtr{},
		&TestModelNoVersion{},
		&TestModelUUIDVersion{},
		&TestModelULIDVersion{},
		&TestPostgresModelTimeVersion{},
	},
}

func findDbContextInfo(ctx context.Context) (dsn string, container tc.Container) {
	var (
		okContainer bool
		okDsn       bool
	)
	container, okContainer = ctx.Value("db").(tc.Container)
	dsn, okDsn = ctx.Value("dsn").(string)
	if !okContainer {
		container = nil
	}
	if !okDsn {
		panic("no dsn found")
	}
	return
}

func setupDatabase(t testingT, skip ...bool) (*gorm.DB, context.Context) {
	var (
		dok    bool
		dbOnce *sync.Once
	)
	if dbOnce, dok = dbOnces[t.DB()]; !dok {
		return nil, nil
	}
	switch t.DB() {
	case testAll:
		panic("doing it wrong")
	case testOracle:
		dbOnce.Do(func() {
			if dbHost, ok := os.LookupEnv("ORA_HOST"); !ok {
				dbHost = "127.0.0.1"
				_ = os.Setenv("ORA_HOST", dbHost)
			}
			if dbPass, ok := os.LookupEnv("ORA_PASS"); !ok {
				dbPass = "test"
				_ = os.Setenv("ORA_PASS", dbPass)
			}
			if dbUser, ok := os.LookupEnv("ORA_USER"); !ok {
				dbUser = "test"
				_ = os.Setenv("ORA_USER", dbUser)
			}
			if dbDriver, ok := os.LookupEnv("ORA_SERVICE"); !ok {
				dbDriver = "FREEPDB1"
				_ = os.Setenv("ORA_SERVICE", dbDriver)
			}
			testDbContexts[testOracle] = startOracleDatabase(t)
		})
		return setupOracleDatabase(t, skip...)
	case testSqlite:
		return setupSqliteDatabase(t, skip...)
	case testPostgres:
		dbOnce.Do(func() {
			if dbName, ok := os.LookupEnv("POSTGRES_DB"); !ok {
				dbName = "test"
				_ = os.Setenv("POSTGRES_DB", dbName)
			}
			if dbUser, ok := os.LookupEnv("POSTGRES_USER"); !ok {
				dbUser = "test"
				_ = os.Setenv("POSTGRES_USER", dbUser)
			}
			if dbPass, ok := os.LookupEnv("POSTGRES_PASS"); !ok {
				dbPass = "test"
				_ = os.Setenv("POSTGRES_PASS", dbPass)
			}
			if dbDriver, ok := os.LookupEnv("POSTGRES_DRIVER"); !ok {
				dbDriver = "pgx"
				_ = os.Setenv("POSTGRES_DRIVER", dbDriver)
			}
			testDbContexts[testPostgres] = startPostgresDatabase(t)
		})
		return setupPostgresDatabase(t, skip...)
	case testMysql:
		dbOnce.Do(func() {
			if dbName, ok := os.LookupEnv("MYSQL_DB"); !ok {
				dbName = "test"
				_ = os.Setenv("MYSQL_DB", dbName)
			}
			if dbUser, ok := os.LookupEnv("MYSQL_USER"); !ok {
				dbUser = "test"
				_ = os.Setenv("MYSQL_USER", dbUser)
			}
			if dbPass, ok := os.LookupEnv("MYSQL_PASS"); !ok {
				dbPass = "test"
				_ = os.Setenv("MYSQL_PASS", dbPass)
			}
			testDbContexts[testMysql] = startMysqlDatabase(t)
		})
		return setupMysqlDatabase(t, skip...)
	}

	return setupSqliteDatabase(t, skip...)
}

func startMysqlDatabase(t testingT) context.Context {
	var (
		err error
	)

	ctx := testDbContexts[testMysql]

	dbName := os.Getenv("MYSQL_DB")
	dbUser := os.Getenv("MYSQL_USER")
	dbPass := os.Getenv("MYSQL_PASS")
	dbHost := os.Getenv("OVERRIDE_HOST")
	if _, ok := os.LookupEnv("MYSQL_SKIP_CONTAINER"); !ok {
		container, cerr := mysql.Run(ctx,
			"mysql:8.0.42",
			mysql.WithDatabase(dbName),
			mysql.WithUsername(dbUser),
			mysql.WithPassword(dbPass),
		)
		require.NoError(t, cerr, "failed to start mysql container")
		dsn, derr := container.ConnectionString(ctx, "charset=utf8mb4", "parseTime=true", "loc=Local")
		if mysqlHost, mysqlHostErr := container.Host(ctx); mysqlHostErr == nil && mysqlHost == "localhost" {
			if len(dbHost) > 0 {
				dsn = strings.ReplaceAll(dsn, "localhost", dbHost)
			}
		}
		require.NoError(t, derr, "failed to get connection string")
		ctx = context.WithValue(ctx, "dsn", dsn)
		ctx = context.WithValue(ctx, "db", container)
		testDbContexts[testMysql] = ctx
	} else {
		dbHost = os.Getenv("MYSQL_HOST")
		if dbHost == "" {
			dbHost = "127.0.0.1"
		}
		dbPort := os.Getenv("MYSQL_PORT")
		if dbPort == "" {
			dbPort = "3306"
		}
		connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s%s", dbUser, dbPass, dbHost, dbPort, dbName, "?charset=utf8mb4&parseTime=true&loc=Local")
		ctx = context.WithValue(ctx, "dsn", connectionString)
		testDbContexts[testMysql] = ctx
	}

	db, _ := setupMysqlDatabase(t)
	require.NoError(t, err, "failed to migrate models")

	_ = db.Migrator().DropTable(testModels["mysql"]...)
	err = db.Migrator().AutoMigrate(testModels["mysql"]...)
	require.NoError(t, err, "failed to migrate models")

	sqlDb, _ := db.DB()
	if sqlDb != nil {
		_ = sqlDb.Close()
	}

	return testDbContexts[testMysql]
}

func setupMysqlDatabase(t testingT, skip ...bool) (*gorm.DB, context.Context) {
	var (
		db  *gorm.DB
		dsn string
		err error
	)
	dsn, _ = findDbContextInfo(testDbContexts[testMysql])

	l := gormlogger.New(&ow{testingT: t}, gormlogger.Config{
		SlowThreshold: time.Second,
		Colorful:      true,
		LogLevel:      gormlogger.Info,
	})
	gcfg := &gorm.Config{
		Logger: l,
		NowFunc: func() time.Time {
			return time.Now().UTC()
		},
	}

	db, err = gorm.Open(gmysql.New(gmysql.Config{
		DSN:               dsn,
		DefaultStringSize: 256, // add default size for string fields, by default, will use db type `longtext` for fields without size, not a primary key, no index defined and don't have default values
		DefaultDatetimePrecision: func() *int {
			val := 6
			return &val
		}(),
	}), gcfg)
	require.NoError(t, err, "failed to connect to mysql database")

	if len(skip) == 0 || !skip[0] {
		err = db.Use(optimistic.NewOptimisticLock())
		require.NoError(t, err)
	}

	require.NoError(t, err, "failed to set up optimistic locking")

	return db, testDbContexts[testMysql]
}

func startPostgresDatabase(t testingT) context.Context {
	var (
		err error
		ctx = testDbContexts[testPostgres]
		dsn string
	)

	dbName := os.Getenv("POSTGRES_DB")
	dbUser := os.Getenv("POSTGRES_USER")
	dbPass := os.Getenv("POSTGRES_PASS")
	dbDriver := os.Getenv("POSTGRES_DRIVER")
	dbHost := os.Getenv("OVERRIDE_HOST")
	if _, ok := os.LookupEnv("POSTGRES_SKIP_CONTAINER"); !ok {
		pgContainer, pgerr := postgres.Run(
			ctx,
			"postgres:16-alpine",
			postgres.WithDatabase(dbName),
			postgres.WithUsername(dbUser),
			postgres.WithPassword(dbPass),
			postgres.WithSQLDriver(dbDriver),
			postgres.BasicWaitStrategies(),
		)
		require.NoError(t, pgerr, "failed to startup postgres database")

		dsn, err = pgContainer.ConnectionString(ctx)
		if pgHost, pgHostOk := pgContainer.Host(ctx); pgHostOk == nil && pgHost == "localhost" {
			if len(dbHost) > 0 {
				dsn = strings.ReplaceAll(dsn, "localhost", dbHost)
			}
		}
		require.NoError(t, err, "failed to get connection string")
		ctx = context.WithValue(ctx, "dsn", dsn)
		ctx = context.WithValue(ctx, "db", pgContainer)
		testDbContexts[testPostgres] = ctx
	} else {
		dbHost = os.Getenv("POSTGRES_HOST")
		if dbHost == "" {
			dbHost = "127.0.0.1"
		}
		dbPort := os.Getenv("POSTGRES_PORT")
		if dbPort == "" {
			panic("POSTGRES_PORT is required")
		}

		dsn = fmt.Sprintf("postgres://%s:%s@%s/%s?%s", dbUser, dbPass, net.JoinHostPort(dbHost, dbPort), dbName, "sslmode=disable")
		ctx = context.WithValue(ctx, "dsn", dsn)
		testDbContexts[testPostgres] = ctx
	}

	db, _ := setupPostgresDatabase(t)
	err = db.Migrator().DropTable(testModels[testPostgres]...)
	require.NoError(t, err, "failed to drop test tables")
	err = db.Migrator().AutoMigrate(testModels[testPostgres]...)
	require.NoError(t, err, "failed to migrate models")
	sqlDb, _ := db.DB()
	if sqlDb != nil {
		_ = sqlDb.Close()
	}

	return testDbContexts[testPostgres]
}

func setupPostgresDatabase(t testingT, skip ...bool) (*gorm.DB, context.Context) {
	var (
		db     *gorm.DB
		err    error
		dsn, _ = findDbContextInfo(testDbContexts[testPostgres])
	)

	l := gormlogger.New(&ow{testingT: t}, gormlogger.Config{
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

	db, err = gorm.Open(gpgx.New(gpgx.Config{
		DSN:                  dsn,
		WithoutQuotingCheck:  false,
		PreferSimpleProtocol: false,
		WithoutReturning:     false,
	}), gcfg)
	require.NoError(t, err, "failed to connect to postgres database")

	if len(skip) == 0 || !skip[0] {
		err = db.Use(optimistic.NewOptimisticLock())
		require.NoError(t, err)
	}

	//err = db.Migrator().AutoMigrate(testModels["postgres"]...)
	//require.NoError(t, err, "failed to migrate models")

	//err = pgContainer.Snapshot(testDbContext)
	//require.NoError(t, err, "failed to snapshot container")

	return db, testDbContexts[testPostgres]
}

func startOracleDatabase(t testingT) context.Context {
	ctx := testDbContexts[testOracle]

	user := os.Getenv("ORA_USER")
	if user == "" {
		user = "test"
	}
	pass := os.Getenv("ORA_PASS")
	if pass == "" {
		pass = "test"
	}
	env := map[string]string{
		"ORACLE_PASSWORD":   pass,
		"APP_USER":          user,
		"APP_USER_PASSWORD": pass,
	}
	service := os.Getenv("ORA_SERVICE")
	if service != "" && service != "FREEPDB1" {
		service = strings.Split(service, ",")[0]
		if len(service) == 0 {
			service = "FREEPDB1"
		}
	}
	var err error
	if _, ok := os.LookupEnv("ORA_SKIP_CONTAINER"); !ok {
		req := tc.ContainerRequest{
			Image:        "gvenzl/oracle-free:slim",
			ExposedPorts: []string{"1521/tcp"},
			Env:          env,
			WaitingFor:   wait.ForLog("Completed: ALTER DATABASE OPEN").WithStartupTimeout(2 * time.Minute),
		}

		oraContainer, oraerr := tc.GenericContainer(ctx, tc.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
			Logger:           &ow{testingT: t},
		})
		require.NoError(t, oraerr, "failed to start container")
		var (
			host string
			port nat.Port
		)
		host, err = oraContainer.Host(ctx)
		require.NoError(t, err, "Failed to get container host")
		if envHost, envHostOk := os.LookupEnv("OVERRIDE_HOST"); envHostOk && host == "localhost" && len(envHost) > 0 {
			host = envHost
		}

		port, err = oraContainer.MappedPort(ctx, "1521")
		require.NoError(t, err, "Failed to get mapped port")
		slog.Default().With("host", host, "port", port.Port()).Debug("Oracle Free is running")
		connectionString := oracle.BuildUrl(
			host,
			port.Int(),
			service,
			user,
			pass,
			nil,
		)
		ctx = context.WithValue(ctx, "dsn", connectionString)
		ctx = context.WithValue(ctx, "db", oraContainer)
		testDbContexts[testOracle] = ctx
	} else {
		host := os.Getenv("ORA_HOST")
		if host == "" {
			host = "127.0.0.1"
		}
		port := os.Getenv("ORA_PORT")
		if port == "" {
			port = "1521"
		}
		var iport int
		iport, err = strconv.Atoi(port)
		require.NoError(t, err, "Failed to get env port")

		connectionString := oracle.BuildUrl(
			host,
			iport,
			service,
			user,
			pass,
			nil,
		)
		ctx = context.WithValue(ctx, "dsn", connectionString)
		testDbContexts[testOracle] = ctx
	}

	db, _ := setupOracleDatabase(t)
	err = db.Migrator().DropTable(testModels["oracle"]...)
	require.NoError(t, err, "failed to drop test tables")
	err = db.Migrator().AutoMigrate(testModels["oracle"]...)
	require.NoError(t, err, "failed to migrate models")
	sqlDb, _ := db.DB()
	if sqlDb != nil {
		_ = sqlDb.Close()
	}

	return testDbContexts[testOracle]
}

func setupOracleDatabase(t testingT, skip ...bool) (*gorm.DB, context.Context) {
	l := gormlogger.New(&ow{testingT: t}, gormlogger.Config{
		SlowThreshold: time.Second,
		Colorful:      true,
		LogLevel:      gormlogger.Info,
	})

	var (
		db  *gorm.DB
		dsn string
		err error
	)
	dsn, _ = findDbContextInfo(testDbContexts[testOracle])

	timeGranularity := -time.Microsecond
	//timeGranularity := time.Duration(0)
	if tgStr, ok := os.LookupEnv("ORA_TIME_GRANULARITY"); ok {
		timeGranularity, err = time.ParseDuration(tgStr)
		require.NoError(t, err, "Failed to parse ORA_TIME_GRANULARITY")
	}
	sessionTimezone := time.UTC
	if sessionTimezoneStr, ok := os.LookupEnv("GORM_ORA_TZ"); ok {
		sessionTimezone, err = time.LoadLocation(sessionTimezoneStr)
		require.NoError(t, err, "Failed to parse ORA_TZ")
	}
	db, err = gorm.Open(oracle.New(oracle.Config{
		DSN:                     dsn,
		VarcharSizeIsCharLength: true,
		IgnoreCase:              true,
		NamingCaseSensitive:     true,
		TimeGranularity:         timeGranularity,
		SessionTimezone:         sessionTimezone.String(),
	}), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			IdentifierMaxLength: 30,
		},
		Logger: l,
		NowFunc: func() time.Time {
			tt := time.Now()
			if timeGranularity < 0 {
				tt = tt.Truncate(-timeGranularity)
			} else if timeGranularity > 0 {
				tt = tt.Round(timeGranularity)
			}
			if sessionTimezone != time.Local {
				tt = tt.In(sessionTimezone)
			}
			return tt
		},
	})
	require.NoError(t, err)
	if len(skip) == 0 || !skip[0] {
		err = db.Use(optimistic.NewOptimisticLock())
		require.NoError(t, err)
	}

	return db, testDbContexts[testOracle]
}

func setupSqliteDatabase(t testingT, skip ...bool) (*gorm.DB, context.Context) {
	l := gormlogger.New(&ow{testingT: t}, gormlogger.Config{
		SlowThreshold: time.Second,
		Colorful:      true,
		LogLevel:      gormlogger.Info,
	})
	// Initialize a SQLite database in memory for testing
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:memdb-%s?mode=memory&cache=shared", uuid.New().String())), &gorm.Config{
		SkipDefaultTransaction: false,
		Logger:                 l,
		NowFunc: func() time.Time {
			return time.Now().UTC().Truncate(time.Microsecond)
		},
	})

	require.NoError(t, err)
	if len(skip) == 0 || !skip[0] {
		err = db.Use(optimistic.NewOptimisticLock())
		require.NoError(t, err)
	}

	// Migrate the schema for TestModel
	err = db.Migrator().DropTable(testModels["sqlite"]...)
	require.NoError(t, err, "failed to drop test tables")
	err = db.Migrator().AutoMigrate(testModels["sqlite"]...)
	require.NoError(t, err)

	return db, testDbContexts[testSqlite]
}
