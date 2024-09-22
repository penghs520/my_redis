use std::collections::HashMap;
use crate::Command::*;
use std::error::Error;
use std::sync::RwLock;
use std::time::SystemTime;
use once_cell::sync::Lazy;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

//主键空间
//这里借住once_cell库来定义全局变量，也可以使用核心库的lazy_static!宏，目的都是延迟初始化
static DB: Lazy<RwLock<HashMap<String, String>>> = Lazy::new(|| RwLock::new(HashMap::new()));

//过期键空间：存放设置了过期时间的键以及对应的过期时间（毫秒）
static EXPIRE_KEY_SET: Lazy<RwLock<HashMap<String, u128>>> = Lazy::new(|| RwLock::new(HashMap::new()));

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
        return construct_cmd(cmd_with_options);
    };
    Ok(Exit)
}

fn construct_cmd(cmd_with_options: Vec<String>) -> Result<Command, String> {
    if cmd_with_options.is_empty() {
        return Ok(Exit);
    }
    let cmd = &cmd_with_options[0];
    match cmd.as_str() {
        "ping" => {
            Ok(Ping)
        }
        "echo" => {
            let options = &cmd_with_options[1..];
            if options.len() == 1 {
                return Ok(Echo(options[0].to_string()));
            }
            Err("ERR wrong number of arguments for 'echo' command".to_string())
        }
        "set" => {
            let options = &cmd_with_options[1..];
            if options.len() == 2 {
                return Ok(Set(SetCmd::new(options[0].to_string(), options[1].to_string())));
            } else if options.len() > 2 {
                let mut expire_set: Option<ExpireSet> = None;
                let mut condition_set: Option<ConditionSet> = None;
                //选项的参数
                let mut option_value = String::new();
                for i in 2..options.len() {
                    let option = &options[i];
                    //如果当前是一个选项的参数，则跳过，因为选项参数上一轮已经被提前使用
                    if &option_value == option {
                        option_value.clear();
                        continue;
                    }
                    match option.as_str() {
                        "ex" | "px" => {
                            //ex px紧接着的下一个是它的参数，下一轮需要跳过
                            option_value.push_str(options[i + 1].as_str());
                            match expire_set {
                                None => {
                                    if "ex" == option.as_str() {
                                        expire_set = Some(ExpireSet::EX(option_value.parse().unwrap()));
                                    } else {
                                        expire_set = Some(ExpireSet::PX(option_value.parse().unwrap()));
                                    }
                                }
                                Some(_) => {
                                    return Err(String::from("Expire set duplicate"));
                                }
                            }
                        }
                        "nx" | "xx" => {
                            match condition_set {
                                None => {
                                    if "nx" == option.as_str() {
                                        condition_set = Some(ConditionSet::NX);
                                    } else {
                                        condition_set = Some(ConditionSet::XX);
                                    }
                                }
                                Some(_) => {
                                    return Err(String::from("Expire set duplicate"));
                                }
                            }
                        }
                        _ => {
                            return Err(format!("Invalid Option: {}", option));
                        }
                    }
                }
                return Ok(Set(SetCmd::new_with_options(options[0].to_string(), options[1].to_string(), expire_set, condition_set)));
            }
            Err("ERR wrong number of arguments for 'set' command".to_string())
        }
        "get" => {
            let options = &cmd_with_options[1..];
            if options.len() == 1 {
                return Ok(Get(options[0].to_string()));
            }
            Err("ERR wrong number of arguments for 'get' command".to_string())
        }
        _ => {
            Err(format!("Unknown command: {}", cmd))
        }
    }
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
                Ok(_) => Ok(1),
                Err(r) => {
                    Err(r.to_string())
                }
            }
        }
        Set(set_cmd) => {
            match set_cmd.condition {
                None => {}
                Some(ConditionSet::NX) => {
                    //当键不存在时设置，否则直接返回
                    if DB.read().unwrap().get(set_cmd.key.as_str()).is_some() {
                        return match socket.write(b"$-1\r\n").await {
                            Ok(_) => Ok(1),
                            Err(r) => {
                                Err(r.to_string())
                            }
                        };
                    }
                }
                Some(ConditionSet::XX) => {
                    //当键存在时设置，否则直接返回
                    if DB.read().unwrap().get(set_cmd.key.as_str()).is_none() {
                        return match socket.write(b"$-1\r\n").await {
                            Ok(_) => Ok(1),
                            Err(r) => {
                                Err(r.to_string())
                            }
                        };
                    }
                }
            }
            match set_cmd.expired {
                None => {}
                Some(ExpireSet::PX(millis)) => {
                    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
                    EXPIRE_KEY_SET.write().unwrap().insert(set_cmd.key.clone(), millis + now);
                }
                Some(ExpireSet::EX(sec)) => {
                    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
                    let millis: u128 = (sec * 1000) as u128;
                    EXPIRE_KEY_SET.write().unwrap().insert(set_cmd.key.clone(), millis + now);
                }
            }
            DB.write().unwrap().insert(set_cmd.key, set_cmd.val.to_string());
            match socket.write(b"+OK\r\n").await {
                Ok(_) => Ok(1),
                Err(r) => {
                    Err(r.to_string())
                }
            }
        }
        Get(key) => {
            let resp = match DB.read().unwrap().get(&key).cloned() {
                Some(val) => {
                    //主键空间存在时，再去检查过期键空间是否存在，如果存在则继续判断键是否过期
                    match EXPIRE_KEY_SET.read().unwrap().get(&key).cloned() {
                        //没有过期设置，直接返回值
                        None => {
                            format!("+{}\r\n", val)
                        }
                        Some(millis) => {
                            let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
                            //已过期
                            if now > millis {
                                //清理过期键
                                //todo 这里会阻塞，因为我们获取的读锁还未释放  rust这个读写锁不是很好，没实现可重入
                                //EXPIRE_KEY_SET.write().unwrap().remove(&key).unwrap();
                                //DB.write().unwrap().remove(&key).unwrap();
                                //返回不存在
                                "$-1\r\n".to_string()
                            } else {
                                //未过期
                                format!("+{}\r\n", val)
                            }
                        }
                    }
                }
                None => "$-1\r\n".to_string(),
            };
            match socket.write(resp.as_bytes()).await {
                Ok(_) => Ok(1),
                Err(r) => {
                    Err(r.to_string())
                }
            }
        }
        Echo(msg) => {
            match socket.write(format!("+{}\r\n", msg).as_bytes()).await {
                Ok(_) => Ok(1),
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
    Set(SetCmd),
    MultiGet(String, Vec<String>),
    Config(String, Vec<KvPair>),
}

#[derive(Debug)]
struct SetCmd {
    key: String,
    val: String,
    expired: Option<ExpireSet>,
    condition: Option<ConditionSet>,
}

impl SetCmd {
    pub fn new(key: String,
               val: String) -> Self {
        SetCmd {
            key,
            val,
            expired: None,
            condition: None,
        }
    }

    pub fn new_with_options(key: String,
                            val: String,
                            expired: Option<ExpireSet>,
                            condition: Option<ConditionSet>) -> Self {
        SetCmd {
            key,
            val,
            expired,
            condition,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ExpireSet {
    EX(u32),
    PX(u128),
}

#[derive(Debug, PartialEq, Eq)]
enum ConditionSet {
    NX, //仅当键不存在时设置键的值
    XX, //仅当键已经存在时设置键的值
}

#[derive(Debug)]
struct KvPair {
    key: String,
    value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construct_cmd() {
        //set key val
        {
            //todo 现在有一个问题，输入全部被转成小写了，无法区分大小写
            let cmd_with_options = vec![String::from("set"), String::from("name"), String::from("tom")];
            let result = construct_cmd(cmd_with_options);
            assert!(result.is_ok());
            let cmd = result.unwrap();
            assert!(matches!(&cmd, Set(..)));
            if let Set(SetCmd) = cmd {
                assert_eq!(SetCmd.key, "name".to_string());
                assert_eq!(SetCmd.val, "tom".to_string());
            }
        }

        //set key val nx
        {
            let cmd_with_options = vec![String::from("set"), String::from("name"), String::from("tom"), String::from("nx")];
            let result = construct_cmd(cmd_with_options);
            assert!(result.is_ok());
            if let Set(SetCmd) = result.unwrap() {
                assert_eq!(SetCmd.key, "name".to_string());
                assert_eq!(SetCmd.val, "tom".to_string());
                assert_eq!(Some(ConditionSet::NX), SetCmd.condition);
            }
        }

        //set key val xx
        {
            let cmd_with_options = vec![String::from("set"), String::from("name"), String::from("tom"), String::from("xx")];
            let result = construct_cmd(cmd_with_options);
            assert!(result.is_ok());
            if let Set(SetCmd) = result.unwrap() {
                assert_eq!(SetCmd.key, "name".to_string());
                assert_eq!(SetCmd.val, "tom".to_string());
                assert_eq!(Some(ConditionSet::XX), SetCmd.condition);
            }
        }

        //set key val px 3000
        {
            let cmd_with_options = vec![String::from("set"), String::from("name"), String::from("tom"), String::from("px"), String::from("3000")];
            let result = construct_cmd(cmd_with_options);
            assert!(result.is_ok());
            if let Set(SetCmd) = result.unwrap() {
                assert_eq!(SetCmd.key, "name".to_string());
                assert_eq!(SetCmd.val, "tom".to_string());
                assert_eq!(Some(ExpireSet::PX(3000)), SetCmd.expired);
            }
        }

        //set key val ex 10
        {
            let cmd_with_options = vec![String::from("set"), String::from("name"), String::from("tom"), String::from("ex"), String::from("10")];
            let result = construct_cmd(cmd_with_options);
            assert!(result.is_ok());
            if let Set(SetCmd) = result.unwrap() {
                assert_eq!(SetCmd.key, "name".to_string());
                assert_eq!(SetCmd.val, "tom".to_string());
                assert_eq!(Some(ExpireSet::EX(10)), SetCmd.expired);
            }
        }

        //测试多选项组合
        //set key val nx ex 10
        {
            let cmd_with_options = vec![String::from("set"), String::from("name"), String::from("tom"), String::from("nx"), String::from("ex"), String::from("10")];
            let result = construct_cmd(cmd_with_options);
            assert!(result.is_ok());
            if let Set(SetCmd) = result.unwrap() {
                assert_eq!(SetCmd.key, "name".to_string());
                assert_eq!(SetCmd.val, "tom".to_string());
                assert_eq!(Some(ConditionSet::NX), SetCmd.condition);
                assert_eq!(Some(ExpireSet::EX(10)), SetCmd.expired);
            }
        }

        //set key val px 3000 xx
        {
            let cmd_with_options = vec![String::from("set"), String::from("name"), String::from("tom"), String::from("px"), String::from("3000"), String::from("xx")];
            let result = construct_cmd(cmd_with_options);
            assert!(result.is_ok());
            if let Set(SetCmd) = result.unwrap() {
                assert_eq!(SetCmd.key, "name".to_string());
                assert_eq!(SetCmd.val, "tom".to_string());
                assert_eq!(Some(ConditionSet::XX), SetCmd.condition);
                assert_eq!(Some(ExpireSet::PX(3000)), SetCmd.expired);
            }
        }
    }
}
