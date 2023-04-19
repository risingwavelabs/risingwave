# Build RisingWave with Multiple Object Storage Backends


<!-- Created by https://github.com/ekalinin/github-markdown-toc -->

## Overview
As a cloud-neutral database, RisingWave supports running on different object stores. Currently, these storage products include [S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage), [COS](https://cloud.tencent.com/product/cos), [OSS](https://www.aliyun.com/product/oss), [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_user_guide.html), [LyveCloud Storage](https://help.lyvecloud.seagate.com/en/s3-storage.html), [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/). 

This doc first briefly introduces how RisingWave supports these storage products, then give a guidance about how to build RisingWave with these object stores quickly and easily through risedev.

## How RisingWave supports multiple object store
The first supported object storage was s3. Afterwards, for other object storage, RisingWave supports them in two ways: via s3 compatible mode or via OpenDAL.
### S3 compatible object store
If an object store declares that it is s3-compatible, it means that it can be directly accessed through the interface provided by the s3 protocol. As RisingWave already implemented [`S3ObjectStore`](https://github.com/risingwavelabs/risingwave/blob/1fd0394980fd713459df8076283bb1a1f46fef9a/src/object_store/src/object/s3.rs#L288), we can reuse the interface of s3 to access this kind of object storage.

Currently for COS and Lyvecloud Storage, we use s3 compatible mode. To use these two object storage products, you need to configure the `access_key`, `secret_key`, `region` and `bueket_name`.
### OpenDAL object store
For those (object) storage products that are not compatible with s3 (or compatible but some interfaces are unstable), we use [OpenDAL](https://github.com/apache/incubator-opendal) to access them. OpenDAL is the Open Data Access Layer to freely access data, which supports several different storage backends. We implemented a [`OpenDALObjectStore`](https://github.com/risingwavelabs/risingwave/blob/1fd0394980fd713459df8076283bb1a1f46fef9a/src/object_store/src/object/opendal_engine/opendal_object_store.rs#L61) to support the interface for accessing object store in RisingWave. 

All of these object stores are supported in risedev, you can use the risedev command to start RisingWave on these storage backends.
## How to build RisingWave with multiple object store
### COS and Lyvecloud Storage
To use COS or Lyvecloud Storage, you need to 
```shell
export S3_COMPATIBLE_REGION = "your_bucket_region"
export S3_COMPATIBLE_ENDPOINT = "endpoint"
export S3_COMPATIBLE_ACCESS_KEY_ID = "access_key_id"
export S3_COMPATIBLE_SECRET_ACCESS_KEY ="access_key_secret"
```

then in `risedev.yml`, [enable using aws s3](https://github.com/risingwavelabs/risingwave/blob/5993d24a4ccadf6dc9fc93ce0fd7e175a2810b71/risedev.yml#L25-L29) and set the bucket name and `s3-compatible = true`. Finally, start RisingWave with ridedev, then you can successfully run RisingWave on these two storage backends.

### GCS

To use GCS, you need to [enable OpenDAL](https://github.com/risingwavelabs/risingwave/blob/1fd0394980fd713459df8076283bb1a1f46fef9a/risedev.yml#L152-L154) in `risedev.yml`, set `engine = gcs`,  `bucket_name` and `root` as well. For authentication, you just need to config credential by `gcloud`, and then OpenDAL gcs backend will automatically find the token in application to complete the verification.

Once these configurations are set, run `./risedev d gcs` and then you can run RisingWave on GCS.
### OSS
To use OSS, you need to [enable OpenDAL](https://github.com/risingwavelabs/risingwave/blob/1fd0394980fd713459df8076283bb1a1f46fef9a/risedev.yml#L167-L170) in `risedev.yml`, set `engine = oss`,  `bucket_name` and `root` as well. 

For authentication, set the identity information in the environment variable:
```shell
export OSS_ENDPOINT="endpoint"
export OSS_ACCESS_KEY_ID="oss_access_key"
export OSS_ACCESS_KEY_SECRET="oss_secret_key"
```


Once these configurations are set, run `./risedev d oss` and then you can run RisingWave on OSS.

### Azure Blob
To use Azure Blob, you need to [enable OpenDAL](https://github.com/risingwavelabs/risingwave/blob/1fd0394980fd713459df8076283bb1a1f46fef9a/risedev.yml#L182-L185) in `risedev.yml`, set `engine = azblob`,  `bucket_name` and `root` as well. For azure blob storage, `bucket_name` is actually the `container_name`.

For authentication, set the identity information in the environment variable:
```shell
export AZBLOB_ENDPOINT="endpoint"
export AZBLOB_ACCOUNT_NAME="your_account_name"
export AZBLOB_ACCOUNT_KEY="your_account_key"
```


Once these configurations are set, run `./risedev d azblob` and then you can run RisingWave on Azure Blob Storage.

### HDFS
To use HDFS, you need to [enable OpenDAL](https://github.com/risingwavelabs/risingwave/blob/1fd0394980fd713459df8076283bb1a1f46fef9a/risedev.yml#L123-L126) in `risedev.yml`, set `engine = hdfs`,  `namenode` and `root` as well. 

You can also use WebHDFS as a lightweight alternative to HDFS. Hdfs is powered by HDFS’s native java client. Users need to setup the hdfs services correctly. But webhdfs can access from HTTP API and no extra setup needed. The way to start webhdfs is basically the same as hdfs, but its default name_node is `127.0.0.1:9870`.