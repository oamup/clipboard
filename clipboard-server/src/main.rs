#![windows_subsystem = "windows"]

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::TcpListener;
use mdns_sd::{ServiceDaemon, ServiceInfo};
use std::collections::HashMap;
use std::time::Duration;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct ClipboardPayload {
    id: String,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mdns = ServiceDaemon::new()?;

    let service_type = "_localclip._tcp.local.";
    let instance_name = "clipboard_server";
    let host_name = "clipboard-server.local.";
    let port = 8080;
    
    let service_info = ServiceInfo::new(
        service_type,
        instance_name,
        host_name,
        "",
        port,
        None::<HashMap<String, String>>,
    )?.enable_addr_auto();

    mdns.register(service_info)?;
    println!("Registered mDNS service '{}' on port {}", service_type, port);

    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    println!("TCP Listener bound to 0.0.0.0:{}", port);

    let connections: Arc<Mutex<HashMap<std::net::SocketAddr, tokio::net::tcp::OwnedWriteHalf>>> = Arc::new(Mutex::new(HashMap::new()));

    let (inbound_tx, inbound_rx) = std::sync::mpsc::channel::<String>();
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    std::thread::spawn(move || {
        let mut clipboard = match arboard::Clipboard::new() {
            Ok(c) => c,
            Err(e) => { eprintln!("Failed to initialize clipboard: {}", e); return; }
        };

        let mut last_cached_value = clipboard.get_text().unwrap_or_default();

        loop {
            while let Ok(new_text) = inbound_rx.try_recv() {
                
                let normalized_new = new_text.replace("\r", "");
                let normalized_cached = last_cached_value.replace("\r", "");

                if normalized_cached != normalized_new {
                    println!("Inbound sync received. Updating local clipboard...");
                    last_cached_value = new_text.clone(); 
                    if let Err(e) = clipboard.set_text(new_text) {
                        eprintln!("Failed to set clipboard text: {}", e);
                    }
                }
            }

            if let Ok(current_text) = clipboard.get_text() {
                
                let normalized_current = current_text.replace("\r", "");
                let normalized_cached = last_cached_value.replace("\r", "");

                if normalized_current != normalized_cached {
                    println!("Local OS clipboard changed! Broadcasting to Android...");
                    last_cached_value = current_text.clone();
                    
                    if let Err(e) = outbound_tx.send(current_text) {
                        eprintln!("Failed to send update: {}", e);
                    }
                }
            }

            std::thread::sleep(Duration::from_millis(400));
        }
    });

    let conns_for_outbound = Arc::clone(&connections);
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        while let Some(text) = outbound_rx.recv().await {
            
            let payload_obj = ClipboardPayload {
                id: uuid::Uuid::new_v4().to_string(),
                text,
            };
            
            if let Ok(mut json_string) = serde_json::to_string(&payload_obj) {
                json_string.push('\n');
                let payload = json_string.into_bytes();

                let mut guard = conns_for_outbound.lock().await;
                let mut to_remove = Vec::new();

                for (addr, writer) in guard.iter_mut() {
                    match tokio::time::timeout(Duration::from_secs(3), writer.write_all(&payload)).await {
                        Ok(Ok(())) => println!("Successfully sent update to: {}", addr),
                        Ok(Err(e)) => {
                            eprintln!("Error writing to {}: {}", addr, e);
                            to_remove.push(*addr);
                        }
                        Err(_) => {
                            eprintln!("Write timeout for {}", addr);
                            to_remove.push(*addr);
                        }
                    }
                }

                for addr in to_remove { guard.remove(&addr); }
            }
        }
    });

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("New connection accepted from: {}", addr);
                let (reader, writer) = stream.into_split();

                {
                    let mut guard = connections.lock().await;
                    guard.insert(addr, writer);
                }

                let conns_clone = Arc::clone(&connections);
                let inbound_tx_clone = inbound_tx.clone();
                
                tokio::spawn(async move {
                    use tokio::io::{AsyncBufReadExt, BufReader};
                    
                    let mut buf_reader = BufReader::new(reader);
                    let mut line = String::new();
                    
                    loop {
                        line.clear();
                        match buf_reader.read_line(&mut line).await {
                            Ok(0) => {
                                println!("Connection closed normally by {}", addr);
                                break;
                            }
                            Ok(_) => {
                                match serde_json::from_str::<ClipboardPayload>(&line) {
                                    Ok(payload) => {
                                        if let Err(e) = inbound_tx_clone.send(payload.text) {
                                            eprintln!("Failed to send to clipboard thread: {}", e);
                                            break;
                                        }
                                    }
                                    Err(e) => eprintln!("Failed to parse JSON from {}: {}", addr, e),
                                }
                            }
                            Err(e) => {
                                println!("Connection read error {}: {}", addr, e);
                                break;
                            }
                        }
                    }

                    let mut guard = conns_clone.lock().await;
                    guard.remove(&addr);
                    println!("Cleaned up client connection: {}", addr);
                });
            }
            Err(e) => eprintln!("Failed to accept incoming connection: {}", e),
        }
    }
}