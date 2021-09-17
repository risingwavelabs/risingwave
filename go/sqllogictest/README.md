# SQLLogicTest-Go

SQLLogicTest-Go implements SQLLogicTest in Golang.
As a brief introduction, SQLLogicTest is a testing script that simplifies
the steps of writing SQL queries and verification.
For the details, please read the SQLite documentation here:
<https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki>.

## Installation

Please choose a suitable binary https://github.com/singularity-data/sqllogictest-go/releases
to download for your platform.

## Building from source

```sh
make
```

If success, the result will be compiled into `bin/sqllogictest`.

## Usage

```
./bin/sqllogictest -port 5432 -file "testdata/basic.slt" -pguser "" -pgpass ""
```

**NOTE:** For mac users, please be aware that macOS's security policy may prevent you from executing this binary. Go Settings->Security&Privacy->General and click "Allow anyway" to be able to run sqllogictest.
