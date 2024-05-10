use bytes::Bytes;
use core::panic;
use mini_redis::{
    Command::{self, Get, Set},
    Connection, Frame,
};
use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, Mutex},
};
use tokio::net::{TcpListener, TcpStream};

type Db = Mutex<HashMap<String, Bytes>>;
type ShardedDb = Arc<Vec<Db>>;
const NO_OF_SHARDS: usize = 10;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening");

    let db = get_sharded_db();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let db = db.clone();
        tokio::spawn(async {
            process(stream, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: ShardedDb) {
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db[get_index_for_key(cmd.key().to_string()) % db.len()]
                    .lock()
                    .unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db = db[get_index_for_key(cmd.key().to_string()) % db.len()]
                    .lock()
                    .unwrap();
                if let Some(val) = db.get(cmd.key()) {
                    Frame::Bulk(val.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("Unimplemented! {cmd:?}"),
        };
        connection.write_frame(&response).await.unwrap();
    }
}

fn get_sharded_db() -> ShardedDb {
    let mut db_vec: Vec<Db> = Vec::with_capacity(NO_OF_SHARDS);
    for _ in 0..NO_OF_SHARDS {
        db_vec.push(Mutex::new(HashMap::new()));
    }

    Arc::new(db_vec)
}

fn get_index_for_key(key: String) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish().try_into().unwrap()
}
