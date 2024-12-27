package optimistic_test

import (
	"fmt"
	"github.com/cmmoran/optimistic"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"log/slog"
	"os"
	"testing"
	"time"
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

type TestModel struct {
	ID          uint64             `gorm:"<-:create;column:id;primaryKey"`
	Description string             `gorm:"column:description"`
	Code        uint64             `gorm:"column:code"`
	Version     optimistic.Version `gorm:"column:version"`
}

type TestModelNoVersion struct {
	ID          uint64 `gorm:"<-:create;column:id;primaryKey"`
	Description string `gorm:"column:description"`
	Code        uint64 `gorm:"column:code"`
}

type ow struct{}

func (ow) Printf(s string, i ...interface{}) {
	fmt.Printf(fmt.Sprintf("%s\n", s), i...)
}

func setupDatabase(t *testing.T) *gorm.DB {
	o := ow{}
	// Initialize a SQLite database in memory for testing
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		SkipDefaultTransaction: false,
		Logger: gormlogger.New(o, gormlogger.Config{
			SlowThreshold: time.Second * 2,
			Colorful:      true,
			LogLevel:      gormlogger.Info,
		}),
		NowFunc: time.Now().UTC,
	})
	assert.NoError(t, err)
	err = db.Use(optimistic.NewOptimisticLock())
	assert.NoError(t, err)

	// Migrate the schema for TestModel
	err = db.AutoMigrate(&TestModel{}, &TestModelNoVersion{})
	assert.NoError(t, err)

	return db
}

func TestCreateNewRecordNoVersion(t *testing.T) {
	db := setupDatabase(t)

	testModel := &TestModelNoVersion{Description: "Test"}

	result := db.Create(testModel)
	assert.NoError(t, result.Error, "expecting no error when creating a new record")
	assert.EqualValues(t, 1, result.RowsAffected, "expecting 1 affected record")
}

func TestCreateNewRecord(t *testing.T) {
	db := setupDatabase(t)

	testModel := &TestModel{Description: "Test"}
	result := db.Create(testModel)
	assert.NoError(t, result.Error, "expecting no error when creating a new record")
	assert.EqualValues(t, 1, testModel.Version, "expecting version to be 1")
}

func TestConcurrentUpdate(t *testing.T) {
	db := setupDatabase(t)

	// Insert a new record
	testModel := &TestModel{Description: "Test"}
	db.Create(testModel)
	concurrentModel := &TestModel{}
	result := db.First(concurrentModel)
	assert.NoError(t, result.Error, "expecting no error when fetching the concurrent record")

	testModel.Description = "Updated Test"
	result = db.Updates(testModel)
	assert.NoError(t, result.Error, "expecting no error when updating test record")
	assert.EqualValues(t, 2, testModel.Version, "expecting version to be 2")
	assert.EqualValues(t, "Updated Test", testModel.Description, "expecting description to be updated")

	// Simulate a concurrent update
	concurrentModel.Description = "Concurrent Test"
	result = db.Updates(concurrentModel)
	assert.ErrorIs(t, result.Error, optimistic.ErrOptimisticLock, "expecting error when updating the concurrent record")
	assert.EqualValues(t, 1, concurrentModel.Version, "expecting version to be 1")
	assert.EqualValues(t, "Concurrent Test", concurrentModel.Description, "expecting description to be 'Test'")

	testModelNoVersion := &TestModelNoVersion{Description: "Test"}
	db.Create(testModelNoVersion)
	concurrentModelNoVersion := &TestModelNoVersion{}
	result = db.First(concurrentModelNoVersion)
	assert.NoError(t, result.Error, "expecting no error when fetching the concurrent record")

	testModelNoVersion.Description = "Updated Test"
	result = db.Updates(testModelNoVersion)
	assert.NoError(t, result.Error, "expecting no error when updating test record")

	concurrentModelNoVersion.Description = "Concurrent Test"
	result = db.Updates(concurrentModelNoVersion)
	assert.NoError(t, result.Error, "expecting no error when fetching the concurrent record")
	assert.EqualValues(t, "Concurrent Test", concurrentModelNoVersion.Description, "expecting description to be 'Test'")
}

func TestTransactionUpdate(t *testing.T) {
	dbx := setupDatabase(t)

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

func TestDryRunUpdate(t *testing.T) {
	db := setupDatabase(t)

	testModel := &TestModel{Description: "Test"}
	db.Create(testModel)

	testModel.Description = "DryRun Test"
	result := db.Session(&gorm.Session{DryRun: true}).Updates(testModel)
	assert.NoError(t, result.Error, "expecting no error with DryRun = true")
	assert.EqualValues(t, "DryRun Test", testModel.Description, "expecting description to be 'DryRun Test'")
	assert.EqualValues(t, 1, testModel.Version, "expecting version to be unchanged")

	checkModel := &TestModel{ID: testModel.ID}
	result = db.First(checkModel)
	assert.NoError(t, result.Error, "expecting no error with DryRun = true")
	assert.EqualValues(t, "Test", checkModel.Description, "expecting description to be 'Test'")
	assert.EqualValues(t, 1, checkModel.Version, "expecting version to be unchanged")
}

func TestUpdateWithZeroValues(t *testing.T) {
	db := setupDatabase(t)

	testModel := &TestModel{Description: "Test"}
	db.Create(testModel)

	testModel.Description = ""
	result := db.Updates(testModel)
	assert.NoError(t, result.Error, "expecting no error when setting values to zero values")
	assert.EqualValues(t, 1, testModel.Version, "expecting version to be unchanged")
}

func TestUnscopedUpdate(t *testing.T) {
	db := setupDatabase(t)

	testModel := &TestModel{Description: "Test"}
	db.Create(testModel)

	testModel.Description = "foo"
	testModel.Code = 1
	result := db.Unscoped().Updates(testModel)
	assert.NoError(t, result.Error, "expecting no error when updating with unscoped")
	assert.EqualValues(t, "foo", testModel.Description, "expecting description to be changed")
	assert.EqualValues(t, 2, testModel.Version, "expecting version to increment")
}

func TestForceZeroValueUpdate(t *testing.T) {
	db := setupDatabase(t)

	testModel := &TestModel{Description: "Test"}
	db.Create(testModel)

	testModel.Description = ""
	result := db.Select("description").Updates(testModel)
	assert.NoError(t, result.Error, "expecting no error when updating to force zero value")
	assert.EqualValues(t, 2, testModel.Version, "expecting version to increment")
}

func TestUpdateSelectStar(t *testing.T) {
	db := setupDatabase(t)

	testModel := &TestModel{Description: "Test", Code: 1}
	db.Create(testModel)

	testModel.Description = "Updated Test"
	testModel.Code = 2
	result := db.Select("*").Updates(testModel)
	assert.NoError(t, result.Error, "expecting no error when updating all fields")
	assert.EqualValues(t, 2, testModel.Version, "expecting version to increment")
	assert.EqualValues(t, "Updated Test", testModel.Description, "expecting description to be updated")
	assert.EqualValues(t, 2, testModel.Code, "expecting code to be updated")
}

func TestMultipleFieldUpdate(t *testing.T) {
	db := setupDatabase(t)

	testModel := &TestModel{Description: "Test"}
	db.Create(testModel)

	testModel.Description = "Updated Test multiple fields"
	testModel.Code = 123
	result := db.Updates(testModel)
	assert.NoError(t, result.Error, "expecting no error when updating multiple fields")
	assert.EqualValues(t, 2, testModel.Version, "expecting version to increment")
}

func anyFalse(values ...bool) bool {
	for _, value := range values {
		if !value {
			return true
		}
	}
	return false
}

func TestUpdateUsingUpdateMethod(t *testing.T) {

	tests := []struct {
		name   string
		db     *gorm.DB
		model  func(*gorm.DB) *TestModel
		action func(*gorm.DB, *TestModel) (*gorm.DB, *TestModel)
		check  func(*testing.T, *gorm.DB, *TestModel) []bool
	}{
		{
			name: "+simple update using update method",
			db:   setupDatabase(t),
			model: func(db *gorm.DB) *TestModel {
				tm := &TestModel{
					ID:          1,
					Description: "Test",
					Code:        1,
					Version:     1,
				}
				db.Create(tm)
				return tm
			},
			action: func(db *gorm.DB, model *TestModel) (*gorm.DB, *TestModel) {
				return db.Model(model).Update("description", "Test Update"), model
			},
			check: func(t *testing.T, result *gorm.DB, testModel *TestModel) []bool {
				return []bool{
					assert.NoError(t, result.Error, "expecting no error when updating using Update method"),
					assert.EqualValues(t, 1, result.RowsAffected, "expecting one row updated"),
					assert.EqualValues(t, TestModel{
						ID:          1,
						Description: "Test Update",
						Code:        1,
						Version:     2,
					}, *testModel, "expecting testModel description to be updated"),
				}
			},
		},
		{
			name: "+update using update method with map of values to change",
			db:   setupDatabase(t),
			model: func(db *gorm.DB) *TestModel {
				tm := &TestModel{
					ID:          1,
					Description: "Test",
					Code:        1,
					Version:     1,
				}
				db.Create(tm)
				return tm
			},
			action: func(db *gorm.DB, model *TestModel) (*gorm.DB, *TestModel) {
				tm := &TestModel{
					Version: model.Version,
				}
				return db.Model(tm).Where("id = ?", model.ID).Updates(map[string]any{
					"code":        2,
					"description": "Test Update",
				}), tm
			},
			check: func(t *testing.T, result *gorm.DB, testModel *TestModel) []bool {
				return []bool{
					assert.NoError(t, result.Error, "expecting no error when updating using Update method"),
					assert.EqualValues(t, 1, result.RowsAffected, "expecting one row updated"),
					assert.EqualValues(t, TestModel{
						ID:          1,
						Description: "Test Update",
						Code:        2,
						Version:     2,
					}, *testModel, "expecting testModel to be updated"),
				}
			},
		},
		{
			name: "-update using update method with map of values to change but no version field",
			db:   setupDatabase(t),
			model: func(db *gorm.DB) *TestModel {
				tm := &TestModel{
					ID:          1,
					Description: "Test",
					Code:        1,
					Version:     1,
				}
				db.Create(tm)
				return tm
			},
			action: func(db *gorm.DB, model *TestModel) (*gorm.DB, *TestModel) {
				tm := &TestModel{}
				return db.Model(tm).Where("id = ?", model.ID).Updates(map[string]any{
					"code":        3,
					"description": "Test Update",
				}), tm
			},
			check: func(t *testing.T, result *gorm.DB, testModel *TestModel) []bool {
				return []bool{
					assert.ErrorIs(t, result.Error, optimistic.ErrOptimisticLock, "expecting optimistic lock error"),
					assert.EqualValues(t, 0, result.RowsAffected, "expecting one row updated"),
					assert.EqualValues(t, TestModel{
						ID:          0,
						Code:        3,
						Description: "Test Update",
						Version:     0,
					}, *testModel, "expecting testModel to be updated"),
				}
			},
		},
		{
			name: "+update using update method with unscoped and with map of values to change",
			db:   setupDatabase(t),
			model: func(db *gorm.DB) *TestModel {
				tm := &TestModel{
					ID:          1,
					Description: "Test",
					Code:        1,
					Version:     1,
				}
				db.Create(tm)
				return tm
			},
			action: func(db *gorm.DB, model *TestModel) (*gorm.DB, *TestModel) {
				tm := &TestModel{}
				return db.Unscoped().Model(tm).Where("id = ?", model.ID).Updates(map[string]any{
					"code":        4,
					"description": "Test Update",
					"version":     99,
				}), tm
			},
			check: func(t *testing.T, result *gorm.DB, testModel *TestModel) []bool {

				return []bool{
					assert.NoError(t, result.Error, "expecting optimistic lock error"),
					assert.EqualValues(t, 1, result.RowsAffected, "expecting one row updated"),
					assert.EqualValues(t, TestModel{
						ID:          1,
						Code:        4,
						Description: "Test Update",
						Version:     99,
					}, *testModel, "expecting testModel to be updated"),
				}
			},
		},
		{
			name: "-update using update method with empty model placeholder but with primary key in where clause (should fail due to no version)",
			db:   setupDatabase(t),
			model: func(db *gorm.DB) *TestModel {
				tm := &TestModel{
					ID:          1,
					Description: "Test",
					Code:        1,
					Version:     1,
				}
				db.Create(tm)
				return tm
			},
			action: func(db *gorm.DB, model *TestModel) (*gorm.DB, *TestModel) {
				tm := &TestModel{}
				return db.Model(tm).Where("id = ?", model.ID).Update("description", "Test Update"), tm
			},
			check: func(t *testing.T, result *gorm.DB, testModel *TestModel) []bool {
				return []bool{
					assert.ErrorIs(t, result.Error, optimistic.ErrOptimisticLock, "expecting optimistic lock error"),
					assert.EqualValues(t, 0, result.RowsAffected, "expecting one row updated"),
					assert.EqualValues(t, TestModel{
						ID:          0,
						Description: "Test Update",
						Code:        0,
						Version:     0,
					}, *testModel, "expecting testModel to be updated"),
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.model(tt.db)
			if tt.action != nil {
				tt.db, m = tt.action(tt.db, m)
			}
			check := tt.check(t, tt.db, m)
			if anyFalse(check...) {
				t.Errorf("TestUpdateUsingUpdateMethod() = %s, (%v)", tt.name, check)
			}
		})
	}
}

func TestUnscopedVersionUpdate(t *testing.T) {
	db := setupDatabase(t)

	testModel := &TestModel{Description: "Test"}
	db.Create(testModel)

	result := db.Unscoped().Model(testModel).Update("version", 5)
	assert.NoError(t, result.Error, "expecting no error when unscoped updating version field")
	assert.EqualValues(t, 5, testModel.Version, "expecting version to update directly")
}

func TestOptimisticLockOnConcurrentUpdate(t *testing.T) {
	db := setupDatabase(t)

	testModel := &TestModel{Description: "Test"}
	db.Create(testModel)

	concurrentModel := &TestModel{}
	db.First(concurrentModel)
	testModel.Description = "Updated Test"
	db.Updates(testModel)

	concurrentModel.Description = "Concurrent Update"
	result := db.Updates(concurrentModel)
	assert.EqualValues(t, 1, concurrentModel.Version, "expecting version to remain unchanged")
	assert.ErrorIs(t, result.Error, optimistic.ErrOptimisticLock, "expecting optimistic lock error")
	assert.EqualValues(t, 0, result.RowsAffected, "expecting no rows affected")
}

func TestVersionCreateClauses(t *testing.T) {
	db := setupDatabase(t)

	// Insert a new record
	testModel := TestModel{Description: "Test"}
	result := db.Create(&testModel)
	assert.NoError(t, result.Error)

	// Check if the version field is initialized correctly
	assert.True(t, testModel.Version > 0)
	assert.True(t, testModel.Version.Equal(1))
}

func TestVersionUpdateClauses(t *testing.T) {
	db := setupDatabase(t)

	// Insert a new record
	testModel := TestModel{Description: "Test"}
	result := db.Create(&testModel)
	id := testModel.ID
	assert.NoError(t, result.Error)

	// Update the record
	testModel.Description = "Updated Test"
	result = db.Save(&testModel)
	assert.NoError(t, result.Error)
	assert.EqualValues(t, TestModel{
		ID:          id,
		Description: "Updated Test",
		Version:     2,
	}, testModel, "expecting one row updated")

	// Reload the record and check version increment
	var updatedModel TestModel
	result = db.First(&updatedModel, testModel.ID)
	assert.NoError(t, result.Error)
	assert.True(t, updatedModel.Version.Equal(2))
}

func TestUnmarshalAndMarshalJSON(t *testing.T) {
	// Test JSON marshalling and unmarshalling of Version
	var (
		version    optimistic.Version
		marshalled []byte
		err        error
	)
	jsonData := []byte(`1`)

	err = version.UnmarshalJSON(jsonData)
	assert.NoError(t, err, "expecting no error when unmarshalling JSON")
	assert.EqualValues(t, 1, version, "expecting version to be 1")

	marshalled, err = version.MarshalJSON()
	assert.NotNil(t, marshalled, "expecting marshalled JSON to be non-nil")
	assert.NoError(t, err, "expecting no error when marshalling JSON")
	assert.Equal(t, []byte(`1`), marshalled, "expecting JSON to be `1`")
}

func TestNullVersionHandling(t *testing.T) {
	// Test handling of null versions
	var version optimistic.Version
	err := version.UnmarshalJSON([]byte(""))
	assert.NoError(t, err)
	assert.False(t, version > 0)

	err = version.UnmarshalJSON([]byte("101"))
	assert.NoError(t, err)
	assert.Equal(t, optimistic.Version(101), version)

	marshalled, err := version.MarshalJSON()
	assert.NoError(t, err)
	assert.Equal(t, `101`, string(marshalled))
}
