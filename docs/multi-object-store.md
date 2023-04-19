# Build RisingWave with Multiple Object Storage Backend


<!-- Created by https://github.com/ekalinin/github-markdown-toc -->

## Overview
As a cloud-neutral database, Risingwave supports deployment on different object stores. The storage products currently supported include [S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage), [COS](https://cloud.tencent.com/product/cos), [OSS](https://www.aliyun.com/product/oss), [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_user_guide.html), [LyveCloud Storage](https://help.lyvecloud.seagate.com/en/s3-storage.html), [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/). This doc first briefly introduces how RisingWave supports these storage products, then give a guidance about how to build RisingWave with these object stores quickly and easily through risedev.

## How RisingWave supports multiple object store
The object storage that RisingWave first supported was s3. Afterwards, for other object storage, RisingWave supports in two ways: via s3 compatible mode or via OpenDAL.
### S3 compatible object store
If an object store declares that it is s3-compatible, it means that it can be directly accessed through the interface provided by the s3 protocol. As RisingWave already implemented S3 object store, we can reuse the interface of s3 to access this kind of object storage.

Currently for COS and Lyvecloud Storage, we use s3 compatible mode. To use these two object storage products, you need to configure the access key, secret key, region and bueket name.
### OpenDAL object store
For those (object) storage products that are not compatible with s3 (or compatible but some interfaces are unstable), we use [OpenDAL](https://github.com/apache/incubator-opendal) to access them. OpenDAL is the Open Data Access Layer to freely access data, which supports several different storage backends. We implemented a `OpenDALObjectStore` to support the interface for accessing object store in RisingWave.
## How to build RisingWave with multiple object store
### COS
### Lyvecloud Storage
### GCS
### OSS
### HDFS
### Azure Blob