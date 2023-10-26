module github.com/my_account/my_udf

go 1.20

require github.com/apache/arrow/go/v13 v13.0.0-20230712165359-085a0baf7868

require (
	github.com/goccy/go-json v0.10.0 // indirect
	github.com/google/flatbuffers v23.5.26+incompatible // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/klauspost/cpuid/v2 v2.2.3 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/mod v0.8.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/tools v0.6.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
)

replace github.com/apache/arrow/go/v13 => github.com/xxchan/arrow/go/v13 v13.0.0-20230713134335-45002b4934f9
