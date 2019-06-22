#[macro_use] extern crate log;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use clap::{App, Arg};
use tokio::prelude::*;
use tokio::timer::Interval;
use std::time::{Duration, Instant};
use env_logger;
use std::fs::File;
use reqwest::r#async::Client;
use std::io::copy;
use reqwest::r#async::Chunk;
use std::error::Error;

fn produce(brokers: &str, topic_name: &str, snapshot_url: &str) {
    let url = snapshot_url.to_string();
    let client = Client::new();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let task = Interval::new(Instant::now(), Duration::from_secs(60))
        //.take(10)
        .for_each( move |instant| {
            info!("GET {}", url);
            client.get(&url)
                .send()
                .and_then(|resp| {
                    let mut body = resp.into_body().concat2().wait();

                    match body {
                        Ok(chunk) => {
                            match save_chunk(chunk, "image.png") {
                                Ok(_) => {
                                    info!("Produce success");
                                },
                                Err(e) => {
                                    error!("Produce failure: {:?}", e);
                                },
                            }
                        },
                        Err(e) => {
                            error!("Unable to get response body: {:?}", e);
                        },
                    }

                    Ok(())
                }).map_err(|err| panic!("request error, err: {:?}", err))
        })
        .map_err(|e| panic!("interval errored; err={:?}", e));

    tokio::run(task);
}

fn save_chunk(chunk: Chunk, filename: &str) -> Result<(), Box<Error>> {
    let mut dest = File::create(filename)?;
    let payload = chunk.to_vec();
    dest.write_all(&payload[..])?;
    Ok(())
}

fn main() {
    env_logger::init();
    let matches = App::new("producer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple camera snapshot producer")
        .arg(Arg::with_name("brokers")
            .short("b")
            .long("brokers")
            .help("Broker list in kafka format")
            .takes_value(true)
            .default_value("localhost:9092"))
        .arg(Arg::with_name("topic")
            .short("t")
            .long("topic")
            .help("Destination topic")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("snapshot_url")
            .short("s")
            .long("snapshot_url")
            .help("Camera snapshot url")
            .takes_value(true)
            .required(true))
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topic = matches.value_of("topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let snapshot_url = matches.value_of("snapshot_url").unwrap();

    produce(brokers, topic, snapshot_url);
}
