variable "aws_profile" {
  description = "AWS profile used for planning and applying these resources."
  type        = string
  default     = "rwc-cicd"
}

variable "aws_region" {
  description = "AWS region for the Docker sccache bucket."
  type        = string
  default     = "us-east-2"
}

variable "bucket_name" {
  description = "Optional fixed bucket name. Leave null to use bucket_name_prefix."
  type        = string
  default     = null
}

variable "bucket_name_prefix" {
  description = "Bucket prefix used when bucket_name is null."
  type        = string
  default     = "rw-docker-sccache-"
}

variable "cache_retention_days" {
  description = "How long to retain cached compiler objects under the docker/ prefix."
  type        = number
  default     = 60
}

variable "buildkite_agent_role_arns" {
  description = "IAM role ARNs used by Buildkite agents that may assume the Docker sccache role."
  type        = list(string)
  default     = []
}

variable "max_session_duration_seconds" {
  description = "Maximum STS session duration for the Docker sccache role."
  type        = number
  default     = 14400
}

variable "tags" {
  description = "Tags to apply to created resources."
  type        = map(string)
  default = {
    Service = "buildkite"
    Purpose = "docker-sccache"
  }
}
