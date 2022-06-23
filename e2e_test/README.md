# e2e test

This folder contains sqllogictest source files for e2e. It is running on CI for integration.

e2e test should drop table if created one to avoid table exist conflict (Currently all tests are running in a single database).
## How to write e2e tests
Refer to Sqllogictest [Doc](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki)
