# AGENTS.md - AWS Docker Configuration

## 1. Scope

Policies for the docker/aws directory, covering AWS-specific Docker configurations and build scripts for RisingWave deployment on AWS infrastructure.

## 2. Purpose

The aws directory provides Docker configurations optimized for AWS deployment scenarios. It includes minimal Ubuntu-based images and build scripts for creating RisingWave containers suitable for AWS ECS, EKS, and EC2 deployments.

## 3. Structure

```
aws/
├── Dockerfile                 # AWS-optimized RisingWave image
├── aws-build.sh              # Build script for AWS images
├── .gitignore                # Git ignore rules
└── AGENTS.md                 # This file
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Minimal Ubuntu 24.04 based RisingWave image |
| `aws-build.sh` | Build automation script with AWS-specific options |
| `.gitignore` | Excludes build artifacts and credentials |

## 5. Edit Rules (Must)

- Keep image minimal with only required dependencies
- Use specific Ubuntu version tags (not `latest`)
- Document all installed packages and their purposes
- Follow AWS container best practices
- Include health checks in Dockerfile
- Use multi-stage builds where appropriate
- Test image in AWS environment before releasing
- Document required IAM roles and permissions

## 6. Forbidden Changes (Must Not)

- Hardcode AWS credentials or secrets
- Use `latest` tag for base images
- Include unnecessary packages that increase image size
- Expose sensitive ports without authentication
- Commit AWS account IDs or ARNs
- Remove CA certificates package (required for TLS)

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build image | `docker build -f Dockerfile .` |
| Validate image | `docker run --rm <image> --version` |
| Security scan | `docker scan <image>` or Trivy |
| Size check | `docker images <image>` |
| AWS test | Deploy to ECS/EKS test environment |

## 8. Dependencies & Contracts

- Docker Engine 20.10+
- Ubuntu 24.04 base image
- AWS CLI (for build script)
- ECR registry access
- RisingWave binary (copied at build time)
- CA certificates for TLS verification

## 9. Overrides

Inherits from `/home/k11/risingwave/docker/AGENTS.md`:
- Override: Edit Rules - AWS-specific minimal image requirements
- Override: Test Entry - AWS deployment testing required

## 10. Update Triggers

Regenerate this file when:
- Base image changes (Ubuntu version updates)
- AWS deployment patterns change
- New AWS services are supported
- Build script functionality changes
- Security requirements for AWS change

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: /home/k11/risingwave/docker/AGENTS.md
