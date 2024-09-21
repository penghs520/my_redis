use crate::Command::*;
use std::error::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

//Tokio提供了执行异步任务的运行时。大多数应用程序可以使用#[tokio::main]宏在tokio运行时运行它们的代码。
//但是，这个宏只提供基本的配置选项。作为替代方案，tokio::runtime模块为配置和管理运行时提供了更强大的api。如果#[tokio::main]宏不提供所需的功能，则应该使用该模块。
#[tokio::main]
//async fn main() -> std::io::Result<()> {
async fn main() -> Result<(), Box<dyn Error>> {
    //std::net::TcpListener是阻塞的，而tokio::net::TcpListener是异步的
    let listener = TcpListener::bind("127.0.0.1:6379").await?; //使用?时，函数的返回值不能是()
    println!("Redis Server Started at 6379!");
    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            println!("Accept connection from {:?}", socket.peer_addr().unwrap());
            loop {
                //一定要在外面控制退出循环的条件，否则会死循环
                match extract_cmd(&mut socket).await {
                    Ok(cmd) => {
                        match handle_cmd(cmd, &mut socket).await {
                            Ok(n) if n == 0 => {
                                println!("Connection terminated");
                                return;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                socket.write(format!("-{}\r\n", &e).as_bytes()).await.expect("Failed to write to client");
                            }
                        }
                    }
                    Err(err) => {
                        socket.write(format!("-{}\r\n", &err).as_bytes()).await.expect("Failed to write to client");
                    }
                }
            }
        });
    }
}

async fn extract_cmd(socket: &mut TcpStream) -> Result<Command, String> {
    let mut reader = BufReader::new(socket);
    let mut line = String::new();
    if let Ok(size) = reader.read_line(&mut line).await {
        if size == 0 {
            println!("Empty line");
            return Ok(Exit);
        }
        if !line.starts_with("*") {
            return Err(String::from("Input format error"));
        }
        let elements = line[1..].trim().parse().unwrap();
        let mut cmd_with_options = Vec::with_capacity(elements);
        //每一个元素有长度和实际内容组成，要先读取长度，再按长度读取实际内容
        for _ in 0..elements {
            line.clear();
            reader.read_line(&mut line).await.unwrap();
            let len = line[1..].trim().parse().unwrap();
            line.clear();
            reader.read_line(&mut line).await.unwrap();
            cmd_with_options.push(line[..len].trim().to_lowercase())
        }
        if cmd_with_options.len() > 0 {
            let cmd = &cmd_with_options[0];
            match cmd.as_str() {
                "ping" => {
                    return Ok(Ping);
                }
                "echo" => {
                    let options = &cmd_with_options[1..];
                    if options.len() == 1 {
                        return Ok(Echo(options[0].to_string()));
                    }
                    return Err("ERR wrong number of arguments for 'echo' command".to_string());
                }
                _ => {
                    return Err(format!("Unknown command: {}", cmd));
                }
            }
        }
    };
    Ok(Exit)
}

async fn handle_cmd(cmd: Command, socket: &mut TcpStream) -> Result<(u8), String> {
    match cmd {
        Exit => {
            Ok(0)
        }
        Ping => {
            //这里用if let有问题，连续的命令中第二次会失败，所以改成match语法
            // if let Ok(_) = socket.write(b"+PONG\r\n").await.unwrap() {
            //     return Err(String::from("Wrote response failed2"));
            // }
            // Ok(())
            match socket.write(b"+PONG\r\n").await {
                Ok(_) => Ok((1)),
                Err(r) => {
                    Err(r.to_string())
                }
            }
        }
        Echo(msg) => {
            match socket.write(format!("+{}\r\n", msg).as_bytes()).await {
                Ok(_) => Ok((1)),
                Err(r) => {
                    Err(r.to_string())
                }
            }
        }
        _ => {
            Err(String::from("暂不支持的命令!"))
        }
    }
}


#[derive(Debug)]
enum Command {
    Ping,
    Exit,
    Echo(String),
    Get(String),
    Set(String, String),
    MultiGet(String, Vec<String>),
    Config(String, Vec<KvPair>),
}

#[derive(Debug)]
struct KvPair {
    key: String,
    value: String,
}