use std::{
    net::TcpListener,
    io::Write,
};
use std::io::Read;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("Redis Server Started at 6379!");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let mut buf = [0; 1024];
                //使用循环等待新的命令
                loop {
                    ////这个方法会阻塞，直到客户端输入命令
                    let n = stream.read(&mut buf).unwrap();
                    //直到收到 bye命令是才关闭连接
                    if String::from_utf8_lossy(&buf[..n]).contains("bye") {
                        break;
                    }
                    stream.write(b"+PONG\r\n").unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}