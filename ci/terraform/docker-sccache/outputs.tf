output "bucket_name" {
  description = "S3 bucket name for Docker sccache."
  value       = aws_s3_bucket.docker_sccache.bucket
}

output "bucket_region" {
  description = "S3 bucket region."
  value       = var.aws_region
}

output "iam_policy_arn" {
  description = "IAM policy ARN with scoped Docker sccache S3 permissions."
  value       = aws_iam_policy.docker_sccache.arn
}

output "role_arn" {
  description = "Role ARN for Buildkite agents to assume. Null when no agent roles were provided."
  value       = try(aws_iam_role.docker_sccache[0].arn, null)
}

output "buildkite_environment" {
  description = "Non-secret Buildkite environment values to set when enabling Docker sccache."
  value = {
    DOCKER_SCCACHE_BUCKET   = aws_s3_bucket.docker_sccache.bucket
    DOCKER_SCCACHE_REGION   = var.aws_region
    DOCKER_SCCACHE_ROLE_ARN = try(aws_iam_role.docker_sccache[0].arn, null)
  }
}
