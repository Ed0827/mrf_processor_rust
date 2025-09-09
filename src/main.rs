//! # High-Performance MRF Processor in Rust
//!
//! Streams massive, gzipped JSON MRFs in two passes with low memory:
//! 1) provider_references  2) in_network
//!
//! Key design points:
//! - **Streaming root-object arrays** via Serde `Visitor`/`DeserializeSeed` (no full JSON load)
//! - **MPMC** work queue with `async_channel` for multi-consumer workers
//! - **Per-code file writers** using `tokio::mpsc`
//! - **Cloudflare R2** upload via AWS SDK (concurrent, bounded)
//!
//! Usage:
//!   mrf_processor_rust <input_file.json.gz> <output_dir> <r2_prefix>

use anyhow::{anyhow, Context, Result};
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client as S3Client};
use dashmap::DashMap;
use flate2::read::GzDecoder;
use futures::stream::{self, StreamExt};
use serde::{de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor}, Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Semaphore};
use tokio::task;

// --- Data Structures (matching the JSON) ---

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Tin {
    r#type: String,
    value: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ProviderGroup {
    npi: Vec<serde_json::Value>,
    tin: Tin,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ProviderReference {
    provider_group_id: u64,
    provider_groups: Vec<ProviderGroup>,
}

#[derive(Debug, Deserialize, Clone)]
struct RemoteProviderReference {
    provider_group_id: u64,
    location: String,
}

/// An enum to handle both local and remote reference types seamlessly.
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
enum ReferenceType {
    Local(ProviderReference),
    Remote(RemoteProviderReference),
}

#[derive(Debug, Deserialize)]
struct NegotiatedPrice {
    negotiated_type: String,
    negotiated_rate: f64,
    billing_class: String,
    #[serde(default)]
    billing_code_modifier: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct NegotiatedRate {
    provider_references: Vec<u64>,
    negotiated_prices: Vec<NegotiatedPrice>,
}

#[derive(Debug, Deserialize)]
struct InNetworkItem {
    negotiation_arrangement: String,
    billing_code_type: String,
    billing_code: String,
    negotiated_rates: Vec<NegotiatedRate>,
}

// --- Serde streaming seeds/visitors for array-by-key at root ---

/// Streams every element of an array, invoking `cb` per element.
struct ArrayStreamSeed<'a, F> {
    cb: &'a mut F,
}

impl<'de, 'a, F> DeserializeSeed<'de> for ArrayStreamSeed<'a, F>
where
    F: FnMut(Value) -> Result<()>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ArrayStreamVisitor { cb: self.cb })
    }
}

struct ArrayStreamVisitor<'a, F> {
    cb: &'a mut F,
}

impl<'de, 'a, F> Visitor<'de> for ArrayStreamVisitor<'a, F>
where
    F: FnMut(Value) -> Result<()>,
{
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "an array to stream")
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(elem) = seq.next_element::<Value>()? {
            (self.cb)(elem).map_err(serde::de::Error::custom)?;
        }
        Ok(())
    }
}

/// Looks for a specific key at the root object and streams its array value via `cb`.
fn stream_root_array_by_key<R: std::io::Read, F: FnMut(Value) -> Result<()>>(
    reader: R,
    key: &str,
    mut cb: F,
) -> Result<()> {
    struct RootKeySeed<'a, F> {
        key: &'a str,
        cb: &'a mut F,
    }

    impl<'de, 'a, F> DeserializeSeed<'de> for RootKeySeed<'a, F>
    where
        F: FnMut(Value) -> Result<()>,
    {
        type Value = ();

        fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_map(RootKeyVisitor { key: self.key, cb: self.cb })
        }
    }

    struct RootKeyVisitor<'a, F> {
        key: &'a str,
        cb: &'a mut F,
    }

    impl<'de, 'a, F> Visitor<'de> for RootKeyVisitor<'a, F>
    where
        F: FnMut(Value) -> Result<()>,
    {
        type Value = ();

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "a JSON object at the root")
        }

        fn visit_map<M>(self, mut map: M) -> std::result::Result<(), M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut found = false;
            while let Some(k) = map.next_key::<String>()? {
                if k == self.key {
                    found = true;
                    // Stream the array value element-by-element
                    map.next_value_seed(ArrayStreamSeed { cb: self.cb })?;
                } else {
                    // Skip any other value efficiently
                    let _ = map.next_value::<IgnoredAny>()?;
                }
            }
            if !found {
                return Err(serde::de::Error::custom(format!(
                    "Key '{}' not found at root",
                    self.key
                )));
            }
            Ok(())
        }
    }

    let mut de = serde_json::Deserializer::from_reader(reader);
    let seed = RootKeySeed { key, cb: &mut cb };
    seed.deserialize(&mut de).map_err(|e| anyhow!(e))?;
    Ok(())
}

// --- Main Application Logic ---

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} <input_file.json.gz> <output_dir> <r2_prefix>", args[0]);
        std::process::exit(1);
    }

    let input_path = PathBuf::from(&args[1]);
    let output_dir = PathBuf::from(&args[2]);
    let r2_prefix = &args[3];

    let start_time = Instant::now();
    println!("🚀 Starting processing for: {}", input_path.display());

    // Ensure output directory exists and is clean
    if output_dir.exists() {
        fs::remove_dir_all(&output_dir)?;
    }
    fs::create_dir_all(&output_dir)?;

    // --- Pass 1: Extract, Fetch, and Write Provider References ---
    println!("\n===== Pass 1: Processing Provider References =====");
    let pass1_start = Instant::now();
    let all_references = extract_and_fetch_references(&input_path, &output_dir).await?;
    println!(
        "✅ Pass 1 finished in {:.2?}. Found {} total provider references.",
        pass1_start.elapsed(),
        all_references.len()
    );

    // --- Pass 2: Stream and Process In-Network Items ---
    println!("\n===== Pass 2: Processing In-Network Items =====");
    let pass2_start = Instant::now();
    process_in_network_items(&input_path, &output_dir).await?;
    println!("✅ Pass 2 finished in {:.2?}", pass2_start.elapsed());

    // --- R2 Upload ---
    println!("\n===== Uploading to R2 =====");
    let upload_start = Instant::now();
    let s3_client = setup_r2_client().await?;
    upload_to_r2(s3_client, &output_dir, r2_prefix).await?;
    println!("✅ Upload finished in {:.2?}", upload_start.elapsed());

    // --- Cleanup ---
    println!("\n🧹 Cleaning up local directory...");
    fs::remove_dir_all(&output_dir)?;

    println!(
        "\n🎉 Total processing time: {:.2?}",
        start_time.elapsed()
    );

    Ok(())
}

/// Pass 1: Extracts local references, fetches remote ones, and writes consolidated results.
async fn extract_and_fetch_references(
    input_path: &Path,
    output_dir: &Path,
) -> Result<Vec<ProviderReference>> {
    let file = File::open(input_path).context("Failed to open input file for Pass 1")?;
    let gz = GzDecoder::new(file);
    let buf_reader = BufReader::new(gz);

    println!("🔍 Streaming 'provider_references' ...");

    let mut local_refs: Vec<ProviderReference> = Vec::new();
    let mut remote_refs: Vec<RemoteProviderReference> = Vec::new();

    // Stream elements of the provider_references array, pushing into lists
    stream_root_array_by_key(buf_reader, "provider_references", |val| {
        let r: ReferenceType = serde_json::from_value(val)?;
        match r {
            ReferenceType::Local(pr) => local_refs.push(pr),
            ReferenceType::Remote(rr) => remote_refs.push(rr),
        }
        Ok(())
    })?;

    println!(
        "Found {} local and {} remote references.",
        local_refs.len(),
        remote_refs.len()
    );

    // Fetch remote references concurrently
    if !remote_refs.is_empty() {
        println!("📡 Fetching {} remote references...", remote_refs.len());
        let client = reqwest::Client::new();
        let bodies = stream::iter(remote_refs)
            .map(|remote_ref| {
                let client = client.clone();
                async move {
                    match client.get(&remote_ref.location).send().await {
                        Ok(resp) => match resp.json::<ProviderReference>().await {
                            Ok(mut provider_ref) => {
                                provider_ref.provider_group_id = remote_ref.provider_group_id;
                                Ok(provider_ref)
                            }
                            Err(e) => Err(anyhow!(
                                "Failed to parse JSON from {}: {}",
                                remote_ref.location,
                                e
                            )),
                        },
                        Err(e) => Err(anyhow!("Failed to fetch {}: {}", remote_ref.location, e)),
                    }
                }
            })
            .buffer_unordered(100);

        let fetched_refs = bodies
            .filter_map(|res| async {
                match res {
                    Ok(r) => Some(r),
                    Err(e) => {
                        eprintln!("Warning: {}", e);
                        None
                    }
                }
            })
            .collect::<Vec<_>>()
            .await;

        println!("Successfully fetched {} remote references.", fetched_refs.len());
        local_refs.extend(fetched_refs);
    }

    // Write consolidated references to output files
    write_provider_references_files(&local_refs, output_dir)?;

    Ok(local_refs)
}

/// Writes the provider references to both a JSON and a CSV file.
fn write_provider_references_files(refs: &[ProviderReference], output_dir: &Path) -> Result<()> {
    let refs_dir = output_dir.join("provider_references");
    fs::create_dir_all(&refs_dir)?;

    // Write JSON file
    let json_path = refs_dir.join("provider_references.json");
    let json_file = File::create(&json_path)?;
    serde_json::to_writer_pretty(BufWriter::new(json_file), refs)?;
    println!("Wrote consolidated references to {}", json_path.display());

    // Write CSV file
    let csv_path = refs_dir.join("provider_groups.csv");
    let mut csv_writer = csv::Writer::from_path(&csv_path)?;
    csv_writer.write_record(&["provider_group_id", "npi", "tin_type", "tin_value"])?;

    for pref in refs {
        for group in &pref.provider_groups {
            for npi_val in &group.npi {
                let npi_str = match npi_val {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    _ => continue, // Skip null or other types
                };
                csv_writer.write_record(&[
                    pref.provider_group_id.to_string(),
                    npi_str,
                    group.tin.r#type.clone(),
                    group.tin.value.clone(),
                ])?;
            }
        }
    }
    csv_writer.flush()?;
    println!("Wrote provider groups to {}", csv_path.display());

    Ok(())
}

/// Pass 2: Streams the in_network array and processes items using a pool of worker tasks.
async fn process_in_network_items(input_path: &Path, output_dir: &Path) -> Result<()> {
    let in_network_dir = output_dir.join("in_network");
    fs::create_dir_all(&in_network_dir)?;

    let writer_map: Arc<DashMap<String, mpsc::Sender<String>>> = Arc::new(DashMap::new());

    // MPMC channel (async_channel) so multiple workers can receive.
    let (tx, rx) = async_channel::bounded::<InNetworkItem>(2048);

    let item_count = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();
    let total_workers = num_cpus::get_physical();
    println!("⚙️  Starting {} worker tasks for in-network processing.", total_workers);

    // --- Spawn Worker Tasks ---
    let mut worker_handles = Vec::new();
    for _ in 0..total_workers {
        let rx = rx.clone(); // Clone the receiver for each worker
        let writer_map = writer_map.clone();
        let in_network_dir = in_network_dir.clone();
        let item_count = item_count.clone();

        let handle = task::spawn(async move {
            while let Ok(item) = rx.recv().await {
                // Aggregate CSV lines per billing code to minimize writer contention
                let mut records_by_code: HashMap<String, String> = HashMap::new();
                for rate in &item.negotiated_rates {
                    for price in &rate.negotiated_prices {
                        for provider_ref in &rate.provider_references {
                            let modifiers = price.billing_code_modifier.join("|");
                            let record = format!(
                                "{},{},{},{},{},{},{},{}\n",
                                provider_ref,
                                price.negotiated_rate,
                                item.billing_code,
                                item.billing_code_type,
                                item.negotiation_arrangement,
                                price.negotiated_type,
                                price.billing_class,
                                modifiers
                            );
                            records_by_code
                                .entry(item.billing_code.clone())
                                .or_default()
                                .push_str(&record);
                        }
                    }
                }

                for (code, records) in records_by_code {
                    if let Some(writer_tx) = writer_map.get(&code) {
                        if writer_tx.send(records).await.is_err() {
                            eprintln!("Error: File writer for code {} has closed.", code);
                        }
                    } else {
                        // Potential race: double-insert guard
                        let (writer_tx, mut writer_rx) = mpsc::channel::<String>(128);
                        let file_path = in_network_dir.join(format!("in_network_{}.csv", code));

                        if let Some(existing) = writer_map.insert(code.clone(), writer_tx.clone()) {
                            // Someone beat us; use existing and drop ours
                            if existing.send(records).await.is_err() {
                                eprintln!("Error: File writer for code {} has closed.", code);
                            }
                        } else {
                            // Spawn writer task for this code
                            task::spawn(async move {
                                let mut file =
                                    BufWriter::new(File::create(file_path).expect("create file"));
                                file.write_all(b"provider_reference,negotiated_rate,billing_code,billing_code_type,negotiation_arrangement,negotiated_type,billing_class,billing_code_modifier\n")
                                    .expect("write header");
                                while let Some(data) = writer_rx.recv().await {
                                    if file.write_all(data.as_bytes()).is_err() {
                                        eprintln!("Error writing to file for code {}", code);
                                        break;
                                    }
                                }
                                let _ = file.flush();
                            });
                            let _ = writer_tx.send(records).await;
                        }
                    }
                }

                let count = item_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count % 10_000 == 0 {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    println!("Processed {} items... ({:.2} items/sec)", count, count as f64 / elapsed);
                }
            }
        });
        worker_handles.push(handle);
    }

    // --- Spawn JSON Reader Task (blocking thread, owns its PathBuf) ---
    let input_path_owned = input_path.to_path_buf();
    let reader_handle = task::spawn_blocking(move || -> Result<()> {
        let file = File::open(input_path_owned).context("Failed to open input file for Pass 2")?;
        let gz = GzDecoder::new(file);
        let buf_reader = BufReader::new(gz);

        println!("🔍 Streaming 'in_network' ...");

        stream_root_array_by_key(buf_reader, "in_network", |val| {
            let item: InNetworkItem = serde_json::from_value(val)?;
            // Send each item to workers; if the channel closed, stop
            if tx.send_blocking(item).is_err() {
                return Ok(());
            }
            Ok(())
        })?;

        Ok(())
    });

    // Wait for the reader; channel closes when tx is dropped at end of closure
    reader_handle.await??;

    // Wait for all worker tasks to complete
    for handle in worker_handles {
        handle.await?;
    }

    // Drop all writer senders to let writer tasks exit (on channel close)
    writer_map.clear();

    println!("Total items processed: {}", item_count.load(Ordering::Relaxed));
    Ok(())
}

/// Sets up the S3 client to connect to an R2 bucket using the modern AWS SDK config.
async fn setup_r2_client() -> Result<S3Client> {
    let account_id = env::var("R2_ACCOUNT_ID").context("R2_ACCOUNT_ID not set")?;
    let access_key_id = env::var("R2_ACCESS_KEY_ID").context("R2_ACCESS_KEY_ID not set")?;
    let secret_access_key =
        env::var("R2_SECRET_ACCESS_KEY").context("R2_SECRET_ACCESS_KEY not set")?;

    let endpoint_url = format!("https://{}.r2.cloudflarestorage.com", account_id);

    let credentials = Credentials::new(access_key_id, secret_access_key, None, None, "Static");

    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(RegionProviderChain::first_try(Region::new("auto")))
        .credentials_provider(credentials)
        .endpoint_url(endpoint_url)
        .load()
        .await;

    Ok(S3Client::new(&config))
}

/// Walks the output directory and uploads all files to R2 concurrently.
async fn upload_to_r2(client: S3Client, output_dir: &Path, r2_prefix: &str) -> Result<()> {
    let bucket_name = env::var("R2_BUCKET_NAME").context("R2_BUCKET_NAME not set")?;

    let mut files_to_upload = vec![];
    for entry in walkdir::WalkDir::new(output_dir) {
        let entry = entry?;
        if entry.file_type().is_file() {
            files_to_upload.push(entry.into_path());
        }
    }

    println!(
        "Found {} files to upload to R2 bucket '{}'.",
        files_to_upload.len(),
        bucket_name
    );

    let sem = Arc::new(Semaphore::new(20)); // Limit concurrent uploads
    let output_dir_owned = output_dir.to_path_buf();

    let mut tasks = Vec::new();
    for path in files_to_upload {
        let sem = Arc::clone(&sem);
        let client = client.clone();
        let bucket_name = bucket_name.clone();
        let r2_prefix = r2_prefix.to_string();
        let output_dir = output_dir_owned.clone();

        let task = task::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let rel = path
                .strip_prefix(&output_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            let key = if r2_prefix.is_empty() {
                rel
            } else {
                format!("{}/{}", r2_prefix, rel)
            };

            println!("Uploading {} to s3://{}/{}", path.display(), bucket_name, key);

            let body = ByteStream::from_path(&path)
                .await
                .map_err(|e| anyhow!("Failed to read file {}: {}", path.display(), e))?;

            client
                .put_object()
                .bucket(&bucket_name)
                .key(&key)
                .body(body)
                .send()
                .await
                .map_err(|e| anyhow!("Failed to upload {}: {:?}", path.display(), e))?;

            Ok::<(), anyhow::Error>(())
        });
        tasks.push(task);
    }

    for task in tasks {
        task.await??;
    }

    Ok(())
}
