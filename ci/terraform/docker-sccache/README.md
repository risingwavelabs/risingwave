# Docker sccache cache

This Terraform configuration sketches the AWS resources needed for an
S3-backed `sccache` cache used by ad-hoc Docker image builds.

It intentionally does not create or store any long-lived access keys. The
recommended path is:

1. Create the S3 bucket and the scoped IAM role with this configuration.
2. Allow the Buildkite agent role to assume the generated role.
3. Set `DOCKER_SCCACHE_BUCKET`, `DOCKER_SCCACHE_REGION`, and
   `DOCKER_SCCACHE_ROLE_ARN` in the Buildkite docker pipeline environment.
4. Trigger a Docker image build with `ENABLE_DOCKER_SCCACHE=true`.

Example:

```bash
terraform init
terraform plan \
  -var='buildkite_agent_role_arns=["arn:aws:iam::<account-id>:role/<buildkite-agent-role>"]'
```

The provider defaults to the `rwc-cicd` AWS profile.
