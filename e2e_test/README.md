# e2e test

This folder contains sqllogictest soruce files for e2e. It is running on CI for integration.

Scripts in folder `distributed` is able to pass both 3-nodes and single node e2e tests. Scripts in `single` is not able to be executed in distributed mode (possibly miss some distributed exchange executor). 

Authors of files in `single` should have a clear plan for when to move files to `distributed`. One proposal is to include a issue links for each files in `single` to illustrate why it is not ready for distributed and how to enable.

## How to write e2e tests
Refer to Sqllogictest [Doc](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki)