extern crate core;

use std::{process, thread};
use std::time::Duration;
use paho_mqtt as mqtt;
use paho_mqtt::Error::ReasonCode;
use paho_mqtt::{Client, ConnectOptions, CreateOptions, DisconnectOptions, PersistenceType};
use tokio::time::sleep;

fn main() {
    loop {
        let subs_handle = thread::spawn(move || {
            spawn_subscribers()
        });
        let pub_handle = thread::spawn(move || {
            spawn_publishers()
        });
        let subs_handle1 = thread::spawn(move || {
            spawn_subscribers()
        });
        let pub_handle2 = thread::spawn(move || {
            spawn_publishers()
        });
        thread::sleep(Duration::from_secs(3));
    }



    // subs_handle.join().expect("panic in process");
    // pub_handle.join().expect("panic in process");
    // subs_handle1.join().expect("panic in process");
    // pub_handle2.join().expect("panic in process");
    // println!("Test done");
}

//#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
#[tokio::main(flavor = "current_thread")]
async fn spawn_subscribers() {
    let mut handles = vec![];
    for i in 0..100 {
        let handle = tokio::spawn(async move {
            println!("Spawning subscriber {}", i);
            let cli = create_client();
            let rx = cli.start_consuming();
            cli.subscribe("test", 2).expect("panic subscribe");
            for msg in rx {
                sleep(Duration::from_millis(25)).await;
            }
        });
        handles.push(handle);
        //sleep(Duration::from_millis(500)).await;
    }
    for handle in handles {
        println!("Subscriber handle done");
        handle.await.expect("panic in process");
    }
}

//#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
#[tokio::main(flavor = "current_thread")]
async fn spawn_publishers() {
    let mut handles = vec![];
    for i in 0..100 {
        let handle = tokio::spawn(async move {
            println!("Spawning publisher {}", i);
            let cli = create_client();
            for i in 0..100000 {
                let msg = mqtt::Message::new("test", "Hello world!", 2);
                cli.publish(msg).expect("panic publish");
            }
        });
        handles.push(handle);
        //sleep(Duration::from_millis(500)).await;
    }
    for handle in handles {
        handle.await.expect("panic in process");
        println!("Publisher handle done");

    }
}


fn disconnect_opts() -> DisconnectOptions {
    mqtt::DisconnectOptionsBuilder::new()
        .reason_code(mqtt::ReasonCode::Success)
        .finalize()
}

fn create_opts() -> CreateOptions {
    mqtt::CreateOptionsBuilder::new()
        .mqtt_version(5)
        .persistence(PersistenceType::None)
        .server_uri("tcp://localhost:1883")
        .finalize()
}

fn connect_opts() -> ConnectOptions {
    mqtt::ConnectOptionsBuilder::new()
        .mqtt_version(5)
        .keep_alive_interval(Duration::from_secs(20))
        .clean_session(true)
        .finalize()
}

fn create_client() -> Client {
    // Create a client & define connect options
    let cli = mqtt::Client::new(create_opts()).unwrap_or_else(|err| {
        println!("Error creating the client: {:?}", err);
        process::exit(1);
    });

    // Connect and wait for it to complete or fail
    if let Err(e) = cli.connect(connect_opts()) {
        println!("Unable to connect:\n\t{:?}", e);
        process::exit(1);
    }
    cli
}


