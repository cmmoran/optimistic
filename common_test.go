package optimistic_test

import (
	"context"
	"fmt"
	oracle "github.com/cmmoran/gorm-oracle"
	"github.com/cmmoran/optimistic"
	"github.com/google/uuid"
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
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	testAll      = "all"
	testOracle   = "oracle"
	testSqlite   = "sqlite"
	testPostgres = "postgres"
	testMysql    = "mysql"
)

func setupDatabase(t testingT) (*gorm.DB, context.Context) {
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
		return setupOracleDatabase(t)
	case testSqlite:
		return setupSqliteDatabase(t)
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
		return setupPostgresDatabase(t)
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
		return setupMysqlDatabase(t)
	}

	return setupSqliteDatabase(t)
}

func startMysqlDatabase(t testingT) context.Context {
	var err error

	ctx := context.Background()

	dbName := os.Getenv("MYSQL_DB")
	dbUser := os.Getenv("MYSQL_USER")
	dbPass := os.Getenv("MYSQL_PASS")
	mysqlContainer, err = mysql.Run(ctx,
		"mysql:8.0.42",
		mysql.WithDatabase(dbName),
		mysql.WithUsername(dbUser),
		mysql.WithPassword(dbPass),
	)
	require.NoError(t, err, "failed to start mysql container")

	db, _ := setupMysqlDatabase(t)
	require.NoError(t, err, "failed to migrate models")

	err = db.Migrator().AutoMigrate(&TestModel{}, &TestModelPtr{}, &TestModelNoVersion{})
	require.NoError(t, err, "failed to migrate models")

	sqlDb, _ := db.DB()
	if sqlDb != nil {
		_ = sqlDb.Close()
	}

	return ctx
}

func setupMysqlDatabase(t testingT) (*gorm.DB, context.Context) {
	var (
		db       *gorm.DB
		dsn, err = mysqlContainer.ConnectionString(testDbContexts[testMysql])
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

	db, err = gorm.Open(gmysql.New(gmysql.Config{
		DSN:               dsn,
		DefaultStringSize: 256, // add default size for string fields, by default, will use db type `longtext` for fields without size, not a primary key, no index defined and don't have default values
	}), gcfg)
	require.NoError(t, err, "failed to connect to mysql database")
	err = db.Use(optimistic.NewOptimisticLock())
	require.NoError(t, err, "failed to set up optimistic locking")

	return db, testDbContexts[testMysql]
}

func startPostgresDatabase(t testingT) context.Context {
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
	require.NoError(t, err, "failed to startup postgres database")

	db, _ := setupPostgresDatabase(t)
	require.NoError(t, err, "failed to connect to postgres database")
	err = db.Migrator().AutoMigrate(&TestModel{}, &TestModelPtr{}, &TestModelNoVersion{})
	require.NoError(t, err, "failed to migrate models")

	sqlDb, _ := db.DB()
	if sqlDb != nil {
		_ = sqlDb.Close()
	}

	return dbCtx
}

func setupPostgresDatabase(t testingT) (*gorm.DB, context.Context) {
	var (
		db       *gorm.DB
		dsn, err = pgContainer.ConnectionString(testDbContexts[testPostgres])
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

	db, err = gorm.Open(gpgx.Open(dsn), gcfg)
	require.NoError(t, err, "failed to connect to postgres database")
	err = db.Use(optimistic.NewOptimisticLock())
	require.NoError(t, err, "failed to set up optimistic locking")

	err = db.Migrator().AutoMigrate(&TestModel{}, &TestModelPtr{}, &TestModelNoVersion{})
	require.NoError(t, err, "failed to migrate models")

	//err = pgContainer.Snapshot(testDbContext)
	//require.NoError(t, err, "failed to snapshot container")

	return db, testDbContexts[testPostgres]
}

func startOracleDatabase(t testingT) context.Context {
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
		Logger:           &ow{testingT: t},
	})
	require.NoError(t, err, "failed to start container")

	ip, err := oraContainer.Host(ctx)
	require.NoError(t, err, "Failed to get container host")

	port, err := oraContainer.MappedPort(ctx, "1521")
	require.NoError(t, err, "Failed to get mapped port")
	db, _ := setupOracleDatabase(t)
	err = db.Migrator().AutoMigrate(&TestModel{}, &TestModelPtr{}, &TestModelNoVersion{})
	require.NoError(t, err, "failed to migrate models")
	sqlDb, _ := db.DB()
	if sqlDb != nil {
		_ = sqlDb.Close()
	}
	slog.Default().With("ip", ip, "port", port.Port()).Debug("Oracle Free is running")

	return ctx
}

func setupOracleDatabase(t testingT) (*gorm.DB, context.Context) {
	l := gormlogger.New(&ow{testingT: t}, gormlogger.Config{
		SlowThreshold: time.Second,
		Colorful:      true,
		LogLevel:      gormlogger.Info,
	})

	var db *gorm.DB
	host, err := oraContainer.Host(testDbContexts[testOracle])
	require.NoError(t, err, "Failed to get container host")

	port, err := oraContainer.MappedPort(testDbContexts[testOracle], "1521")
	require.NoError(t, err, "Failed to get mapped port")

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
	require.NoError(t, err)
	err = db.Use(optimistic.NewOptimisticLock())
	require.NoError(t, err)

	return db, testDbContexts[testOracle]
}

func setupSqliteDatabase(t testingT) (*gorm.DB, context.Context) {
	l := gormlogger.New(&ow{testingT: t}, gormlogger.Config{
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
	require.NoError(t, err)
	err = db.Use(optimistic.NewOptimisticLock())
	require.NoError(t, err)

	// Migrate the schema for TestModel
	err = db.AutoMigrate(&TestModel{}, &TestModelPtr{}, &TestModelNoVersion{})
	require.NoError(t, err)

	return db, testDbContexts[testSqlite]
}
