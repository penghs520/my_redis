use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

//Tokio提供了执行异步任务的运行时。大多数应用程序可以使用#[tokio::main]宏在tokio运行时运行它们的代码。
//但是，这个宏只提供基本的配置选项。作为替代方案，tokio::runtime模块为配置和管理运行时提供了更强大的api。如果#[tokio::main]宏不提供所需的功能，则应该使用该模块。
#[tokio::main]
//async fn main() -> std::io::Result<()> {
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //std::net::TcpListener是阻塞的，而tokio::net::TcpListener是异步的
    let listener = TcpListener::bind("127.0.0.1:6379").await?; //使用?时，函数的返回值不能是()
    println!("Redis Server Started at 6379!");
    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            println!("Accept connection from {:?}", socket.peer_addr().unwrap());
            let mut buf = [0; 1024];
            loop {
                match socket.read(&mut buf).await {
                    Ok(n) if n == 0 => {
                        println!("Connection from {} closed", socket.peer_addr().unwrap());
                        return;
                    }
                    Ok(n) => {
                        if let Err(e) = socket.write(b"+PONG\r\n").await {
                            eprintln!("Failed to send PONG!");
                        }
                    }
                    Err(_) => {}
                }
            }
        });
    }
}