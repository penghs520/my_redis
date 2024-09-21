use std::net::TcpListener;
fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Redis Server Started at 6379!");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}


