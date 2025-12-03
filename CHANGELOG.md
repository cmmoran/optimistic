## [0.3.8](https://github.com/cmmoran/optimistic/compare/v0.3.7...v0.3.8) (2025-12-03)


### Bug Fixes

* bump gorm-oracle to v0.5.0 for compatibility and enhancements ([fd65dfd](https://github.com/cmmoran/optimistic/commit/fd65dfd2ce8e5681d3db32b27d70a16ef72e6408))



## [0.3.7](https://github.com/cmmoran/optimistic/compare/v0.3.6...v0.3.7) (2025-11-20)


### Bug Fixes

* bump gorm-oracle to v0.4.1 to fix an issue where RETURNING values were not being set to properly initialized values in go_ora.Out.Dest. ([25896dd](https://github.com/cmmoran/optimistic/commit/25896dd64020c5e7a580237963acfd1b23a4f88b))



## [0.3.6](https://github.com/cmmoran/optimistic/compare/v0.3.5...v0.3.6) (2025-11-19)


### Bug Fixes

* **optimistic:** ensure collectAssignments excludes non-updatable schema fields and add versioning test updates ([745d791](https://github.com/cmmoran/optimistic/commit/745d7916b6e944f2740ed9eb8e57a02cfc7b6ff0))



## [0.3.5](https://github.com/cmmoran/optimistic/compare/v0.3.4...v0.3.5) (2025-10-28)


### Bug Fixes

* **optimistic:** collectAssignments should not include non-updatable fields ([7e4e266](https://github.com/cmmoran/optimistic/commit/7e4e266701b28227c6290420d6e0f8fbac74f217))



## [0.3.4](https://github.com/cmmoran/optimistic/compare/v0.3.3...v0.3.4) (2025-10-13)


### Bug Fixes

* **optimistic:** optimistic locking checks need to take into account specific dialector naming strategies ([156a6c8](https://github.com/cmmoran/optimistic/commit/156a6c87619cc46d3e87e74892f5decf48633e61))



