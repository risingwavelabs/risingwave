use std::collections::HashMap;

use aws_config::retry::RetryConfig;
use aws_sdk_ec2::model::{Filter, VpcEndpointType};
use risingwave_pb::catalog::connection::PrivateLinkService;

use crate::MetaResult;

#[derive(Clone)]
pub struct AwsEc2Client {
    client: aws_sdk_ec2::Client,
    vpc_id: String,
    security_group_id: String,
}

impl AwsEc2Client {
    pub async fn new(vpc_id: &str, security_group_id: &str) -> Self {
        let sdk_config = aws_config::from_env()
            .retry_config(RetryConfig::standard().with_max_attempts(4))
            .load()
            .await;
        let client = aws_sdk_ec2::Client::new(&sdk_config);

        Self {
            client,
            vpc_id: vpc_id.to_string(),
            security_group_id: security_group_id.to_string(),
        }
    }

    /// vpc_id: The VPC of the running RisingWave instance
    /// service_name: The name of the endpoint service we want to access
    pub async fn create_aws_private_link(
        &self,
        service_name: &str,
        availability_zones: &Vec<String>,
    ) -> MetaResult<PrivateLinkService> {
        let subnet_ids = self
            .describe_subnets(&self.vpc_id, &availability_zones)
            .await?;

        let (endpoint_id, dns_names) = self
            .create_vpc_endpoint(
                &self.vpc_id,
                service_name,
                &self.security_group_id,
                &subnet_ids,
            )
            .await?;

        let mut az_to_dns_map = HashMap::new();
        for dns_name in dns_names.iter() {
            for az in availability_zones {
                if dns_name.contains(az) {
                    az_to_dns_map.insert(az.clone(), dns_name.clone());
                    break;
                }
            }
        }
        debug_assert!(az_to_dns_map.len() == availability_zones.len());
        Ok(PrivateLinkService {
            endpoint_id,
            dns_entries: az_to_dns_map,
        })
    }

    async fn describe_subnets(
        &self,
        vpc_id: &str,
        availability_zones: &[String],
    ) -> MetaResult<Vec<String>> {
        let vpc_filter = Filter::builder().name("vpc-id").values(vpc_id).build();
        let az_filter = Filter::builder()
            .name("availability-zone")
            .set_values(Some(Vec::from(availability_zones)))
            .build();
        let output = self
            .client
            .describe_subnets()
            .set_filters(Some(vec![vpc_filter, az_filter]))
            .send()
            .await?;

        let subnets = output
            .subnets
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.subnet_id.unwrap_or_default())
            .collect();
        Ok(subnets)
    }

    async fn create_vpc_endpoint(
        &self,
        vpc_id: &str,
        service_name: &str,
        security_group_id: &str,
        subnet_ids: &Vec<String>,
    ) -> MetaResult<(String, Vec<String>)> {
        let output = self
            .client
            .create_vpc_endpoint()
            .vpc_endpoint_type(VpcEndpointType::Interface)
            .vpc_id(vpc_id)
            .security_group_ids(security_group_id)
            .service_name(service_name)
            .set_subnet_ids(Some(subnet_ids.clone()))
            .send()
            .await?;

        let endpoint = output.vpc_endpoint().unwrap();
        let mut dns_names = Vec::new();
        if let Some(dns_entries) = endpoint.dns_entries() {
            dns_entries.iter().for_each(|e| {
                if let Some(dns_name) = e.dns_name() {
                    dns_names.push(dns_name.to_string());
                }
            });
        }

        Ok((
            endpoint.vpc_endpoint_id().unwrap_or_default().to_string(),
            dns_names,
        ))
    }
}
