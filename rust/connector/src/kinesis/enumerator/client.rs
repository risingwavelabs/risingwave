use anyhow::Result;
use aws_sdk_kinesis::model::Shard;
use aws_sdk_kinesis::Client as kinesis_client;

async fn list_shards(client: &kinesis_client, stream_name: &str) -> Result<Vec<Shard>> {
    let mut next_token: Option<String> = None;
    let mut shard_collect: Vec<Shard> = Vec::new();

    loop {
        let list_shard_output = client
            .list_shards()
            .set_next_token(next_token)
            .stream_name(stream_name)
            .send()
            .await?;
        match list_shard_output.shards {
            Some(shard) => shard_collect.extend(shard),
            None => {
                return Err(anyhow::Error::msg(format!(
                    "no shards in stream {}",
                    stream_name
                )));
            }
        }

        match list_shard_output.next_token {
            Some(token) => next_token = Some(token),
            None => break,
        }
    }
    Ok(shard_collect)
}
