import asyncio
import json
import queue
import socket
import threading
import time
import uuid
import pyperclip
from zeroconf import IPVersion, ServiceInfo
from zeroconf.asyncio import AsyncZeroconf

SERVICE_TYPE = "_localclip._tcp.local."
INSTANCE_NAME = "clipboard_server._localclip._tcp.local."
HOST_NAME = "clipboard-server.local."
PORT = 8080


def get_lan_ip() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except OSError:
            return "127.0.0.1"
def normalize(text: str) -> str:
    return text.replace("\r", "")

def clipboard_worker(
    inbound_queue: queue.Queue,
    outbound_queue: asyncio.Queue,
    loop: asyncio.AbstractEventLoop,
) -> None:
    try:
        last_cached_value = pyperclip.paste() or ""
    except pyperclip.PyperclipException as e:
        print(f"Failed to initialize clipboard: {e}")
        return

    while True:
        while True:
            try:
                new_text = inbound_queue.get_nowait()
            except queue.Empty:
                break

            if normalize(last_cached_value) != normalize(new_text):
                print("Updating local clipboard...")
                last_cached_value = new_text
                try:
                    pyperclip.copy(new_text)
                except pyperclip.PyperclipException as e:
                    print(f"Failed to set clipboard text: {e}")

        try:
            current_text = pyperclip.paste()
            if current_text is not None:
                if normalize(current_text) != normalize(last_cached_value):
                    print("Broadcasting to Android...")
                    last_cached_value = current_text
                    loop.call_soon_threadsafe(outbound_queue.put_nowait, current_text)
        except pyperclip.PyperclipException:
            pass

        time.sleep(0.4)


async def outbound_broadcaster(
    outbound_queue: asyncio.Queue,
    connections: dict[tuple, asyncio.StreamWriter],
    connections_lock: asyncio.Lock,
) -> None:
    while True:
        text = await outbound_queue.get()
        payload_obj = {
            "id": str(uuid.uuid4()),
            "text": text,
        }
        payload_bytes = (json.dumps(payload_obj) + "\n").encode("utf-8")

        async with connections_lock:
            to_remove = []
            for addr, writer in connections.items():
                try:
                    writer.write(payload_bytes)
                    await asyncio.wait_for(writer.drain(), timeout=3.0)
                    print(f"Successfully sent update to: {addr}")
                except asyncio.TimeoutError:
                    print(f"Write timeout for {addr}")
                    to_remove.append(addr)
                except OSError as e:
                    print(f"Error writing to {addr}: {e}")
                    to_remove.append(addr)

            for addr in to_remove:
                writer = connections.pop(addr, None)
                if writer:
                    writer.close()


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    inbound_queue: queue.Queue,
    connections: dict[tuple, asyncio.StreamWriter],
    connections_lock: asyncio.Lock,
) -> None:
    addr = writer.get_extra_info("peername")
    print(f"New connection accepted from: {addr}")

    async with connections_lock:
        connections[addr] = writer

    try:
        while True:
            line = await reader.readline()
            if not line:
                print(f"Connection closed normally by {addr}")
                break

            try:
                payload = json.loads(line.decode("utf-8"))
                if isinstance(payload, dict) and "text" in payload:
                    inbound_queue.put_nowait(str(payload["text"]))
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                print(f"Failed to parse JSON from {addr}: {e}")
    except OSError as e:
        print(f"Connection read error {addr}: {e}")
    finally:
        async with connections_lock:
            connections.pop(addr, None)
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
        print(f"Cleaned up client connection: {addr}")


async def main() -> None:
    lan_ip = get_lan_ip()
    service_info = ServiceInfo(
        type_=SERVICE_TYPE,
        name=INSTANCE_NAME,
        addresses=[socket.inet_aton(lan_ip)],
        port=PORT,
        properties={},
        server=HOST_NAME,
    )

    aiozc = AsyncZeroconf(ip_version=IPVersion.V4Only)
    await aiozc.async_register_service(service_info)
    print(f"Registered mDNS service '{SERVICE_TYPE}' on {lan_ip}:{PORT}")
    connections: dict[tuple, asyncio.StreamWriter] = {}
    connections_lock = asyncio.Lock()
    inbound_queue: queue.Queue[str] = queue.Queue()
    outbound_queue: asyncio.Queue[str] = asyncio.Queue()
    loop = asyncio.get_running_loop()

    clip_thread = threading.Thread(
        target=clipboard_worker,
        args=(inbound_queue, outbound_queue, loop),
        daemon=True,
    )
    clip_thread.start()
    broadcaster_task = asyncio.create_task(
        outbound_broadcaster(outbound_queue, connections, connections_lock)
    )
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, inbound_queue, connections, connections_lock),
        host="0.0.0.0",
        port=PORT,
    )
    print(f"TCP Listener bound to 0.0.0.0:{PORT}")

    try:
        async with server:
            await server.serve_forever()
    finally:
        broadcaster_task.cancel()
        await aiozc.async_unregister_service(service_info)
        await aiozc.async_close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass