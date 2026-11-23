"""다중 사용자 Tkinter 채팅용 TCP 서버 (Python 표준 라이브러리만 사용)."""
import argparse
import json
import os
import socket
import threading
from dataclasses import dataclass, field

MAX_PACKET = 16384

@dataclass(eq=False)
class Peer:
    sock: socket.socket
    name: str = ""
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    def send(self, message: dict) -> None:
        raw = (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")
        with self.send_lock:
            self.sock.sendall(raw)


class ChatServer:
    def __init__(self, host: str, port: int):
        self.host, self.port = host, port
        self.peers: set[Peer] = set()
        self.lock = threading.RLock()

    def broadcast(self, packet: dict, exclude: Peer | None = None) -> None:
        with self.lock:
            targets = [p for p in self.peers if p is not exclude]
        for peer in targets:
            try:
                peer.send(packet)
            except OSError:
                self.remove(peer)

    def user_list(self) -> None:
        with self.lock:
            names = sorted(p.name for p in self.peers if p.name)
        self.broadcast({"type": "users", "names": names})

    def remove(self, peer: Peer) -> None:
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
            print(f"퇴장: {peer.name}")
            self.broadcast({"type": "typing", "name": peer.name, "active": False})
            self.broadcast({"type": "notice", "text": f"{peer.name}님이 퇴장했습니다."})
            self.user_list()

    def handle(self, sock: socket.socket, addr) -> None:
        peer = Peer(sock)
        try:
            with sock.makefile("r", encoding="utf-8", newline="\n") as stream:
                first = stream.readline(MAX_PACKET + 1)
                if not first or len(first) > MAX_PACKET:
                    return
                hello = json.loads(first)
                if hello.get("type") != "join":
                    peer.send({"type": "error", "text": "입장 메시지가 필요합니다."})
                    return
                name = str(hello.get("name", "")).strip()[:20]
                if not name or any(ord(ch) < 32 for ch in name):
                    peer.send({"type": "error", "text": "올바른 닉네임을 입력하세요."})
                    return
                with self.lock:
                    if any(p.name == name for p in self.peers):
                        peer.send({"type": "error", "text": "이미 사용 중인 닉네임입니다."})
                        return
                    peer.name = name
                    self.peers.add(peer)
                peer.send({"type": "welcome", "name": name})
                print(f"입장: {name} ({addr[0]})")
                self.broadcast({"type": "notice", "text": f"{name}님이 입장했습니다."})
                self.user_list()
                while True:
                    line = stream.readline(MAX_PACKET + 1)
                    if not line or len(line) > MAX_PACKET:
                        break
                    try:
                        msg = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    typ = msg.get("type")
                    if typ == "chat":
                        body = str(msg.get("text", "")).strip()[:2000]
                        if body:
                            self.broadcast({"type": "chat", "name": name, "text": body})
                            self.broadcast({"type": "typing", "name": name, "active": False})
                    elif typ == "typing":
                        self.broadcast({"type": "typing", "name": name,
                                        "active": msg.get("active") is True}, exclude=peer)
                    elif typ == "leave":
                        break
        except (OSError, UnicodeError, ValueError) as exc:
            print(f"접속 종료 ({addr}): {exc}")
        finally:
            self.remove(peer)
            try:
                sock.close()
            except OSError:
                pass

    def run(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()
            print(f"서버 실행 중: {self.host}:{self.port} (Ctrl+C로 종료)")
            try:
                while True:
                    client, addr = server.accept()
                    threading.Thread(target=self.handle, args=(client, addr), daemon=True).start()
            except KeyboardInterrupt:
                print("\n서버 종료")
            finally:
                with self.lock:
                    peers = list(self.peers)
                for peer in peers:
                    self.remove(peer)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="다중 사용자 채팅 서버")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.getenv("RAILWAY_TCP_APPLICATION_PORT") or os.getenv("PORT") or "12550"))
    args = parser.parse_args()
    ChatServer(args.host, args.port).run()
