## Another optimistic locking plugin for [GORM](https://gorm.io)

Provides a `Version` type alias to `uint64` with useful helper functions for enabling optimistic locking when using GORM.

This package also provides a `gorm:after_update` callback to ensure updates are handled appropriately. If not, `ErrOptimisticLock` is set to the `db.Error`.

Effort has been taken to ensure the logic is sound but I make no claims that this code is perfect and free from bugs. If you find any issues please raise a PR.

