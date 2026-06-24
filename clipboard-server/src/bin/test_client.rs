use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:8080";
    println!("Connecting to clipboard server at {}...", addr);
    
    let stream = TcpStream::connect(addr).await?;
    println!("Connected successfully!");

    let (mut reader, mut writer) = stream.into_split();

    tokio::spawn(async move {
        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes).await {
                Ok(_) => {
                    let len = u32::from_be_bytes(len_bytes) as usize;
                    let mut buf = vec![0u8; len];
                    if let Err(e) = reader.read_exact(&mut buf).await {
                        eprintln!("[Client] Failed to read packet body: {}", e);
                        break;
                    }
                    match String::from_utf8(buf) {
                        Ok(text) => {
                            println!("[Client Received] {:?}", text);
                        }
                        Err(e) => {
                            eprintln!("[Client] Invalid UTF-8 received: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("[Client] Connection closed by server: {}", e);
                    break;
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_secs(2)).await;
    
    let test_message = "Hello from Mock Android Client!";
    println!("[Client Sending] {:?}", test_message);
    
    let bytes = test_message.as_bytes();
    let len = bytes.len() as u32;
    let mut payload = Vec::with_capacity(4 + bytes.len());
    payload.extend_from_slice(&len.to_be_bytes());
    payload.extend_from_slice(bytes);

    writer.write_all(&payload).await?;
    println!("[Client] Mock clipboard update sent successfully!");

    tokio::time::sleep(Duration::from_secs(12)).await;
    println!("[Client] Exiting.");
    Ok(())
}
