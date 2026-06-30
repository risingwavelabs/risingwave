# Docker sccache cache

This Terraform configuration sketches the AWS resources needed for an
S3-backed `sccache` cache used by ad-hoc Docker image builds.

The cache is intended to live in a shared build-cache bucket and use a scoped
prefix, instead of creating a new bucket for each cache type. By default, Docker
compiler objects are stored under `sccache/docker/`.

It intentionally does not create or store any long-lived access keys. The
recommended path is:

1. Create or select one shared build-cache bucket.
2. Create the scoped IAM role with this configuration.
3. Set `DOCKER_SCCACHE_BUCKET`, `DOCKER_SCCACHE_REGION`, and
   `DOCKER_SCCACHE_ROLE_ARN` in the Buildkite docker pipeline environment.
   Optionally set `DOCKER_SCCACHE_PREFIX_ROOT` if you changed the default
   prefix.
4. Trigger a Docker image build with `ENABLE_DOCKER_SCCACHE=true`.

Example that creates the shared build-cache bucket:

```bash
terraform init
terraform plan \
  -var='bucket_name=<shared-build-cache-bucket>' \
  -var='buildkite_agent_role_arns=["arn:aws:iam::<account-id>:role/<buildkite-agent-role>"]'
```

Example that reuses an existing shared build-cache bucket:

```bash
terraform plan \
  -var='create_bucket=false' \
  -var='bucket_name=<shared-build-cache-bucket>' \
  -var='manage_lifecycle_rules=false' \
  -var='buildkite_agent_role_arns=["arn:aws:iam::<account-id>:role/<buildkite-agent-role>"]'
```

`manage_lifecycle_rules=false` is recommended when the existing bucket's
lifecycle configuration is owned by another Terraform module, because the AWS
provider manages the whole bucket lifecycle configuration as one resource.
When unset, `manage_lifecycle_rules` defaults to `create_bucket`, so existing
buckets are not modified unless this is explicitly enabled.

The provider defaults to the `rwc-cicd` AWS profile.
