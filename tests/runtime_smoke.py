#!/usr/bin/env python3
"""Exercise built binaries and zenohd plugin over TCP/WebSocket (Python stdlib only).
Run after `cargo build --bins --lib --examples`; requires zenohd 1.10.1 on PATH.
"""
import base64
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / (sys.argv[1] if len(sys.argv) > 1 else "target/debug")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def read_exact(sock, length):
    data = b""
    while len(data) < length:
        part = sock.recv(length - len(data))
        if not part:
            raise RuntimeError("WebSocket closed unexpectedly")
        data += part
    return data


def receive(sock):
    header = read_exact(sock, 2)
    opcode, length = header[0] & 15, header[1] & 127
    if length == 126:
        length = struct.unpack("!H", read_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", read_exact(sock, 8))[0]
    assert not header[1] & 128, "server frames must not be masked"
    assert length < 1024 * 1024
    return opcode, read_exact(sock, length)


def send(sock, opcode, payload):
    mask = os.urandom(4)
    header = bytes([128 | opcode])
    if len(payload) < 126:
        header += bytes([128 | len(payload)])
    else:
        header += bytes([128 | 126]) + struct.pack("!H", len(payload))
    sock.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))


def run(mode):
    with tempfile.TemporaryDirectory(prefix="zenoh-foxglove-smoke-") as temp:
        temp = Path(temp)
        processes = []
        log = open(temp / "process.log", "w+")
        ws_port, zenoh_port = free_port(), free_port()
        while ws_port == zenoh_port:
            zenoh_port = free_port()

        def launch(*args):
            process = subprocess.Popen(args, cwd=ROOT, stdout=log, stderr=log)
            processes.append(process)
            return process

        def config(name, value):
            path = temp / name
            path.write_text(json.dumps(value))
            return str(path)

        settings = {"bind": "127.0.0.1", "port": ws_port}
        router = {
            "mode": "router",
            "listen": {"endpoints": [f"tcp/127.0.0.1:{zenoh_port}"]},
            "scouting": {"multicast": {"enabled": False}},
        }
        if mode == "plugin":
            router["plugins"] = {"foxglove": {
                "__required__": True,
                "__path__": str(BUILD / "libzenoh_plugin_foxglove.so"),
                **settings,
            }}
        client = config("client.json", {
            "mode": "client", "connect": {"endpoints": [f"tcp/127.0.0.1:{zenoh_port}"]},
            "scouting": {"multicast": {"enabled": False}},
        })
        try:
            launch("zenohd", "-c", config("router.json", router))
            if mode == "standalone":
                launch(str(BUILD / "zenoh-foxglove-bridge"), "--config",
                       config("bridge.json", settings), "--zenoh-config", client)
            deadline = time.monotonic() + 20
            while True:
                for process in processes:
                    if process.poll() is not None:
                        raise RuntimeError(
                            f"{mode}: {process.args[0]} exited with code {process.returncode}; "
                            "see process log above for startup or plugin compatibility errors"
                        )
                try:
                    sock = socket.create_connection(("127.0.0.1", ws_port), timeout=1)
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise RuntimeError("WebSocket listener did not start")
                    time.sleep(0.05)
            with sock:
                sock.settimeout(10)
                key = base64.b64encode(os.urandom(16)).decode()
                sock.sendall((f"GET / HTTP/1.1\r\nHost: 127.0.0.1:{ws_port}\r\n"
                              "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                              f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
                              "Sec-WebSocket-Protocol: foxglove.sdk.v1\r\n\r\n").encode())
                header = b""
                while not header.endswith(b"\r\n\r\n"):
                    header += read_exact(sock, 1)
                assert b"101 Switching Protocols" in header
                assert b"foxglove.sdk.v1" in header
                launch(str(BUILD / "examples/protobuf_publisher"), client)
                channels, received = {}, set()
                while len(received) < 2:
                    assert time.monotonic() < deadline, "timed out waiting for both topics"
                    opcode, payload = receive(sock)
                    if opcode == 9:
                        send(sock, 10, payload)
                    elif opcode == 1:
                        message = json.loads(payload)
                        if message["op"] == "advertise":
                            subscriptions = []
                            for channel in message["channels"]:
                                assert channel["topic"] in ("proto_topic", "proto_topic_2")
                                assert channel["encoding"] == "protobuf"
                                assert channel["schemaName"] == "demo.Reading"
                                assert base64.b64decode(channel["schema"])
                                ident = len(channels) + 1
                                channels[ident] = channel["topic"]
                                subscriptions.append({"id": ident, "channelId": channel["id"]})
                            send(sock, 1, json.dumps({"op": "subscribe", "subscriptions": subscriptions}).encode())
                    elif opcode == 2:
                        assert payload[0] == 1
                        ident, timestamp = struct.unpack("<IQ", payload[1:13])
                        assert timestamp > 0
                        assert payload[13] == 9  # Reading.value: protobuf fixed64 field 1.
                        assert channels[ident].encode() in payload[13:]
                        received.add(channels[ident])
                    elif opcode == 8:
                        raise RuntimeError("server closed WebSocket")
                send(sock, 8, struct.pack("!H", 1000))
            print(f"{mode}: descriptor discovery and both Protobuf streams passed")
        except Exception:
            log.flush()
            log.seek(0)
            print(log.read(), file=sys.stderr)
            raise
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            log.close()


if __name__ == "__main__":
    for mode in ("standalone", "plugin"):
        run(mode)
