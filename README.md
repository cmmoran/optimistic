# GORM Optimistic Locking Plugin

[![Go Reference](https://pkg.go.dev/badge/github.com/username/optimistic.svg)](https://pkg.go.dev/github.com/username/optimistic)
[![Go Report Card](https://goreportcard.com/badge/github.com/username/optimistic)](https://goreportcard.com/report/github.com/username/optimistic)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Introduction

This plugin adds optimistic locking support to [GORM](https://gorm.io). It works by tracking version changes in your
models using a version field, preventing concurrent updates from overwriting each other's changes. The plugin supports
both UUID and ULID version field types.

## Installation

```go
    var db *gorm.DB

    // Use default options: TagName = `Version`
    db.Use(optimistic.NewOptimisticLock())
```

## Model Integration

This plugin works by inspecting your model `gorm` tags. When it finds a struct field with a `gorm` tag matching `Config.TagName`, that field will be marked as the `Version` field for that model.

Any non-unscoped, non-dryrun, _targeted_ modifications of the model will require a valid version value in order for the change to persist to the underlying database. A _targeted_ modification is one where the primary key(s) are included in the modification. This means that multi-row updates (for example `UPDATE "users" SET "active" = false WHERE "active" = true AND "idle_time" > 300`) where the `ID` of the model is not specified in the request.

### Examples

#### Number-based versioning

This model will be configured with a `uint64` flavor of versioning. This means every optimistic lock supported update to the model will increment the version by `+ 1`.

Example model:

```go
    type User struct {
        ID          uint64  `gorm:"<-:create;autoIncrement;primaryKey"`
        Name        string  `gorm:"type:text;"`
        Code        uint64  `gorm:"type:numeric;"`
        Enabled     bool    `gorm:"type:bool;"`
        Version     uint64  `gorm:"type:numeric;not null;version"`
    }
```

#### UUID-based versioning

Example model:
```go
    type User struct {
        ID          uint64      `gorm:"<-:create;autoIncrement;primaryKey"`
        Name        string      `gorm:"type:text;"`
        Code        uint64      `gorm:"type:numeric;"`
        Enabled     bool        `gorm:"type:bool;"`
        Version     uuid.UUID   `gorm:"not null;version"`
    }
```

This model will be configured with a `UUID` flavor of versioning. This means every optimistic lock supported update to the model will set the version to a new `UUID`. Note: this versioning strategy will work with _any_ uuid-type that is a type alias to `[16]byte`. Under the hood, `github.com/google/uuid` is used to generate a new `uuidv4` but the value persisted to the database is either `[]byte` or `string` depending on the underlying database driver.

#### ULID-based versioning

Example model:
```go
    type User struct {
        ID          uint64      `gorm:"<-:create;autoIncrement;primaryKey"`
        Name        string      `gorm:"type:text;"`
        Code        uint64      `gorm:"type:numeric;"`
        Enabled     bool        `gorm:"type:bool;"`
        Version     ulid.ULID   `gorm:"not null;version"`
    }
```

This model will be configured with a `ULID` flavor of versioning. This means every optimistic lock supported update to the model will set the version to a new `ULID`. Note: this versioning strategy will work with _any_ ulid-type that is a type alias to `[16]byte`. Under the hood, `github.com/oklog/ulid/v2` is used to generate a new `ulid` but the value persisted to the database is either `[]byte` or `string` depending on the underlying database driver.

#### Time-based versioning

Example model:
```go
    type User struct {
        ID          uint64      `gorm:"<-:create;autoIncrement;primaryKey"`
        Name        string      `gorm:"type:text;"`
        Code        uint64      `gorm:"type:numeric;"`
        Enabled     bool        `gorm:"type:bool;"`
        Version     time.Time   `gorm:"not null;version"`
    }
```

This model will be configured with a `Timestamp` flavor of versioning. This means every optimistic lock supported update to the model will set the version to a new `Timestamp`. Your mileage may vary with this particular version type. Different databases have different mappings for `time.Time`. Some are more coarse-grained than others and may not yield desirable optimistic locking results.

### Issues

If you have issues please open a PR
