use core::panic;
use mini_redis::{
    Command::{self, Get, Set},
    Connection, Frame,
};
use std::collections::HashMap;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening");

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async {
            process(stream).await;
        });
    }
}

async fn process(socket: TcpStream) {
    let mut connection = Connection::new(socket);
    let mut db: HashMap<String, Vec<u8>> = HashMap::new();

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                db.insert(cmd.key().to_string(), cmd.value().to_vec());
                Frame::Simple("OK".to_string())
            }

            Get(cmd) => {
                if let Some(val) = db.get(cmd.key()) {
                    Frame::Bulk(val.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("Unimplemented! {cmd:?}"),
        };


        println!("response : {response:?}");


        connection.write_frame(&response).await.unwrap();
    }
}
