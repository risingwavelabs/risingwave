use std::collections::BTreeMap;
use std::fs;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;

use anyhow::{Context, bail};
use apache_avro::Schema;
use clap::Parser;
use risingwave_connector::schema::ConfluentSchemaLoader;
use risingwave_connector::schema::schema_registry::Client;
use risingwave_connector_codec::decoder::avro::{MapHandling, avro_schema_to_fields};

mod check;

/// Validate Avro schemas against RisingWave's type system, reproducing what
/// `ENCODE AVRO` does when a source is created.
///
/// A schema comes from either a Confluent schema registry (`--all` / `--subject`
/// / `--topic`) or a local file (`--file`). Registry options mirror the
/// `ENCODE AVRO (...)` clause and are passed as `key=value` pairs, e.g.
/// `schema.registry=http://localhost:8081`.
#[derive(Parser)]
#[command(name = "risingwave_avro_validator")]
struct Args {
    /// Validate every subject in the registry.
    #[arg(long, conflicts_with_all = ["subject", "topic"])]
    all: bool,

    /// Validate a single subject by name.
    #[arg(long, conflicts_with_all = ["all", "topic"])]
    subject: Option<String>,

    /// Validate the `<topic>-value` subject for a Kafka topic.
    #[arg(long, conflicts_with_all = ["all", "subject"])]
    topic: Option<String>,

    /// Validate a local Avro schema file, without a registry.
    #[arg(long, conflicts_with_all = ["all", "subject", "topic"])]
    file: Option<PathBuf>,

    /// In `--all` mode, validate without prompting even past `--limit`.
    #[arg(long)]
    yes: bool,

    /// In `--all` mode, prompt for confirmation when more than this many subjects are found.
    #[arg(long, default_value_t = 20)]
    limit: usize,

    /// `ENCODE AVRO` options as `key=value` (at least `schema.registry=<url>`).
    #[arg(value_name = "KEY=VALUE")]
    options: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let options = parse_options(&args.options)?;
    let map_handling = MapHandling::from_options(&options)?;

    // Local file: no registry needed; a single schema, so print its columns.
    if let Some(path) = &args.file {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let (findings, result) =
            validate_schema(&path.display().to_string(), &content, map_handling, true);
        let groups = findings
            .into_iter()
            .map(|f| (f.path.clone(), vec![f]))
            .collect::<Vec<_>>();
        render_rule_sections(&groups);
        return result;
    }

    // The topic only matters for subject-name *strategies*, which these modes
    // bypass by resolving concrete subjects directly; pass it through anyway.
    let loader = ConfluentSchemaLoader::from_format_options(
        args.topic.as_deref().unwrap_or_default(),
        &options,
    )
    .context("failed to build schema registry client from options")?;
    let client = &loader.client;

    let subjects = match (args.all, &args.subject, &args.topic) {
        (true, _, _) => {
            let mut subjects = client
                .list_subjects()
                .await
                .context("failed to list subjects")?;
            subjects.sort();
            if !confirm_scan(subjects.len(), args.limit, args.yes)? {
                eprintln!("aborted.");
                return Ok(());
            }
            subjects
        }
        (false, Some(subject), _) => vec![subject.clone()],
        (false, _, Some(topic)) => vec![format!("{topic}-value")],
        (false, None, None) => {
            bail!("specify one of --all, --subject <name>, --topic <name>, or --file <path>");
        }
    };

    // Single-subject lookups print the full column mapping; a bulk scan is concise.
    let details = !args.all;
    let mut failures = 0usize;
    let mut groups: Vec<(String, Vec<check::Finding>)> = Vec::new();
    for subject in &subjects {
        let (findings, result) = validate_subject(client, subject, map_handling, details).await;
        if let Err(e) = result {
            failures += 1;
            // In `--all` the per-subject status is the report, so it goes to stdout
            // alongside the `OK` lines; a single lookup's failure is a diagnostic.
            if args.all {
                println!("{subject}: error: {e:#}");
            } else {
                eprintln!("{subject}: error: {e:#}");
            }
        }
        // Checks are additive: collect matches into per-rule sections printed after
        // the heading — keyed by subject for a bulk scan, by field path for a single
        // lookup.
        if args.all {
            groups.push((subject.clone(), findings));
        } else {
            for f in findings {
                groups.push((f.path.clone(), vec![f]));
            }
        }
    }

    render_rule_sections(&groups);

    if failures > 0 {
        bail!(
            "{failures} of {} subject(s) failed validation",
            subjects.len()
        );
    }
    Ok(())
}

/// Fetch one subject's latest schema and validate it (full columns, or a concise
/// `OK` line in a bulk scan), warning if the schema declares references.
async fn validate_subject(
    client: &Client,
    subject: &str,
    map_handling: Option<MapHandling>,
    details: bool,
) -> (Vec<check::Finding>, anyhow::Result<()>) {
    let fetched = client
        .get_subject_and_references(subject)
        .await
        .context("failed to fetch schema");
    let (primary, references) = match fetched {
        Ok(pair) => pair,
        Err(e) => return (vec![], Err(e)),
    };

    if !references.is_empty() {
        eprintln!(
            "{subject}: warning: schema declares {} reference(s); RisingWave resolves \
             only the primary subject, so referenced named types are unavailable (validation \
             below will fail if the schema actually uses them).",
            references.len()
        );
    }

    validate_schema(subject, &primary.schema.content, map_handling, details)
}

/// Parse an Avro schema and report the RisingWave columns it maps to: the full
/// per-column listing when `details`, otherwise a single concise `<label>: OK`.
///
/// Output and error text are identical to a check-free run; the structural check
/// findings are returned (computed even when the mapping fails) for the caller to
/// report additively.
fn validate_schema(
    label: &str,
    content: &str,
    map_handling: Option<MapHandling>,
    details: bool,
) -> (Vec<check::Finding>, anyhow::Result<()>) {
    let schema = match Schema::parse_str(content).context("failed to parse Avro schema") {
        Ok(schema) => schema,
        Err(e) => return (vec![], Err(e)),
    };
    // Checks walk the raw JSON, not the parsed `Schema`, so they see what it hides
    // (e.g. the physical type under a logical type). apache_avro already accepted the
    // content, so this should parse; if it somehow doesn't, report it rather than let
    // a schema we couldn't check look clean.
    let value: serde_json::Value = match content
        .parse()
        .context("schema is valid Avro but not valid JSON; structural checks not run")
    {
        Ok(value) => value,
        Err(e) => return (vec![], Err(e)),
    };
    let findings = check::run_default(&value);
    let fields = match avro_schema_to_fields(&schema, map_handling) {
        Ok(fields) => fields,
        Err(e) => return (findings, Err(e)),
    };
    if details {
        for field in &fields {
            println!("{}: {}", field.name, field.data_type);
        }
    } else {
        println!("{label}: OK");
    }
    (findings, Ok(()))
}

/// Additive per-rule sections printed after the heading: every rule prints its own
/// section listing `locator: detail` for its matches (identical matches collapsed
/// with a `(×N)` count), or `(no matches)` when nothing matched. Each group is a
/// `(locator, findings)` pair — the locator is the subject for a bulk scan and the
/// field path for a single schema.
fn render_rule_sections(groups: &[(String, Vec<check::Finding>)]) {
    for rule in check::rule_ids() {
        let mut lines: Vec<(&str, &str, usize)> = Vec::new();
        for (locator, findings) in groups {
            for f in findings.iter().filter(|f| f.rule == rule) {
                match lines.iter_mut().find(|l| l.0 == locator && l.1 == f.detail) {
                    Some(slot) => slot.2 += 1,
                    None => lines.push((locator, &f.detail, 1)),
                }
            }
        }
        println!("\n[{rule}]");
        if lines.is_empty() {
            println!("  (no matches)");
            continue;
        }
        for (locator, detail, count) in lines {
            if count > 1 {
                println!("  {locator}: {detail} (×{count})");
            } else {
                println!("  {locator}: {detail}");
            }
        }
    }
}

/// Split `key=value` arguments into the option map consumed by the loader.
fn parse_options(raw: &[String]) -> anyhow::Result<BTreeMap<String, String>> {
    let mut options = BTreeMap::new();
    for item in raw {
        let (key, value) = item
            .split_once('=')
            .with_context(|| format!("option {item:?} is not in key=value form"))?;
        options.insert(key.to_owned(), value.to_owned());
    }
    Ok(options)
}

/// Decide whether to proceed with an `--all` scan, prompting on a terminal when
/// the subject count exceeds `limit`.
fn confirm_scan(count: usize, limit: usize, yes: bool) -> anyhow::Result<bool> {
    if yes || count <= limit {
        return Ok(true);
    }
    if !std::io::stdin().is_terminal() {
        bail!(
            "{count} subjects exceed --limit {limit}; re-run with --yes to scan non-interactively"
        );
    }
    eprint!("Found {count} subjects (over --limit {limit}). Validate all? [y/N] ");
    std::io::stderr().flush()?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    Ok(matches!(answer.trim(), "y" | "Y" | "yes" | "Yes"))
}
