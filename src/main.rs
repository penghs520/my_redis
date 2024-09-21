use std::{
    net::TcpListener,
    io::Write,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Redis Server Started at 6379!");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                stream.write(b"+PONG\r\n").unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

