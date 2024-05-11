use bytes::Bytes;
use mini_redis::client;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
enum Command {
    Set {
        key: String,
        val: Bytes,
        responder: Responder<()>,
    },
    Get {
        key: String,
        responder: Responder<Option<Bytes>>,
    },
}

type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[tokio::main]
async fn main() {
    let (sender, mut receiver) = mpsc::channel::<Command>(32);
    let sender2 = sender.clone();

    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = receiver.recv().await {
            match cmd {
                Command::Get { key, responder } => {
                    responder.send(client.get(&key).await).unwrap();
                }
                Command::Set {
                    key,
                    val,
                    responder,
                } => {
                    responder.send(client.set(&key, val).await).unwrap();
                }
            }
        }
    });

    tokio::spawn(async move {
        let (one_sender, one_receiver) = oneshot::channel::<mini_redis::Result<()>>();
        sender2
            .send(Command::Set {
                key: "Hello".to_string(),
                val: "World".into(),
                responder: one_sender,
            })
            .await
            .unwrap();

        let res = one_receiver.await.unwrap().unwrap();
        println!("Set : {res:?}");
    });

    tokio::spawn(async move {
        let (one_sender, one_receiver) = oneshot::channel::<mini_redis::Result<Option<Bytes>>>();
        sender
            .send(Command::Get {
                key: "Hello".to_string(),
                responder: one_sender,
            })
            .await
            .unwrap();

        let res = one_receiver.await.unwrap().unwrap().unwrap();

        println!("Get : {res:?}");
    });

    manager.await.unwrap();
}
