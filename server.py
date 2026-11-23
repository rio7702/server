"""Railway TCP group chat: threaded JSON-lines, SQLite chat history."""
import argparse
import json
import os
import socket
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone

MAX_PACKET = 16384

@dataclass(eq=False)
class Peer:
    sock: socket.socket
    name: str = ""
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    def send(self, packet):
        with self.send_lock:
            self.sock.sendall((json.dumps(packet, ensure_ascii=False) + "\n").encode("utf-8"))


class ChatServer:
    def __init__(self, host, port, db_path):
        self.host, self.port = host, port
        self.lock = threading.RLock()
        self.peers = set()
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL)")
        self.db.commit()
        print("DB:", db_path, flush=True)

    def broadcast(self, packet, exclude=None):
        # Caller may hold self.lock, ensuring that history and live messages don't cross.
        with self.lock:
            for peer in list(self.peers):
                if peer is exclude:
                    continue
                try:
                    peer.send(packet)
                except OSError:
                    self.remove(peer)

    def user_list(self):
        with self.lock:
            self.broadcast({"type": "users", "names": sorted(p.name for p in self.peers if p.name)})

    def remove(self, peer):
        with self.lock:
            if peer not in self.peers:
                return
            self.peers.remove(peer)
            try:
                peer.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            peer.sock.close()
            if peer.name:
                self.broadcast({"type": "typing", "name": peer.name, "active": False})
                self.broadcast({"type": "notice", "text": f"{peer.name}님이 퇴장했습니다."})
                self.user_list()

    def handle(self, sock, addr):
        peer = Peer(sock)
        try:
            with sock.makefile("r", encoding="utf-8", newline="\n") as stream:
                line = stream.readline(MAX_PACKET + 1)
                if not line or len(line) > MAX_PACKET:
                    return
                hello = json.loads(line)
                if not isinstance(hello, dict) or hello.get("type") != "join":
                    peer.send({"type": "error", "text": "입장 메시지가 필요합니다."})
                    return
                name = str(hello.get("name", "")).strip()[:20]
                if not name or any(ord(ch) < 32 for ch in name):
                    peer.send({"type": "error", "text": "올바른 닉네임이 필요합니다."})
                    return
                with self.lock:
                    if any(p.name == name for p in self.peers):
                        peer.send({"type": "error", "text": "현재 접속 중인 닉네임입니다."})
                        return
                    peer.name = name
                    peer.send({"type": "welcome", "name": name})
                    peer.send({"type": "history_start"})
                    # History is sent before joining the broadcast set, under the
                    # same lock as writes so there is no gap or duplicate.
                    cursor = self.db.execute("SELECT id,name,body,created_at FROM messages ORDER BY id")
                    for mid, sender, body, created in cursor:
                        peer.send({"type": "chat", "id": mid, "name": sender, "text": body, "time": created, "history": True})
                    peer.send({"type": "history_end"})
                    self.peers.add(peer)
                    self.broadcast({"type": "notice", "text": f"{name}님이 입장했습니다."})
                    self.user_list()
                print("JOIN", name, addr[0], flush=True)
                while True:
                    line = stream.readline(MAX_PACKET + 1)
                    if not line or len(line) > MAX_PACKET:
                        break
                    try:
                        msg = json.loads(line)
                    except ValueError:
                        continue
                    if not isinstance(msg, dict):
                        continue
                    kind = msg.get("type")
                    if kind == "chat":
                        body = msg.get("text", "")
                        if not isinstance(body, str):
                            continue
                        body = body.strip()[:2000]
                        if body:
                            stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
                            with self.lock:
                                cursor = self.db.execute("INSERT INTO messages(name,body,created_at) VALUES (?,?,?)", (name, body, stamp))
                                self.db.commit()
                                packet = {"type": "chat", "id": cursor.lastrowid, "name": name, "text": body, "time": stamp}
                                self.broadcast(packet)
                                self.broadcast({"type": "typing", "name": name, "active": False})
                    elif kind == "typing":
                        self.broadcast({"type": "typing", "name": name, "active": msg.get("active") is True}, exclude=peer)
                    elif kind == "leave":
                        break
        except (OSError, UnicodeError, ValueError, sqlite3.Error) as exc:
            print("CLIENT ERROR", addr, exc, flush=True)
        finally:
            self.remove(peer)
            try:
                sock.close()
            except OSError:
                pass

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.port))
            srv.listen()
            print(f"Listening on {self.host}:{self.port}", flush=True)
            while True:
                try:
                    sock, addr = srv.accept()
                except KeyboardInterrupt:
                    break
                threading.Thread(target=self.handle, args=(sock, addr), daemon=True).start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.getenv("RAILWAY_TCP_APPLICATION_PORT") or os.getenv("PORT") or "12550"))
    parser.add_argument("--db", default=os.getenv("DB_PATH") or os.path.join(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/app/data"), "chat.sqlite3"))
    args = parser.parse_args()
    ChatServer(args.host, args.port, args.db).run()
