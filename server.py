import socket
import threading

# 서버 설정 (클라우드 환경에 맞게 모든 IP 접속 허용으로 변경)
HOST = '0.0.0.0'
PORT = 9999

# 접속한 클라이언트 소켓들을 저장할 리스트
clients = []

# 모든 클라이언트에게 메시지를 전송하는 함수 (브로드캐스트)
def broadcast(message, sender_socket):
    for client in clients:
        # 메시지를 보낸 본인을 제외하고 나머지 사람들에게만 전송
        if client != sender_socket:
            try:
                client.sendall(message)
            except:
                # 연결이 끊어진 소켓은 리스트에서 제거
                if client in clients:
                    clients.remove(client)

# 개별 클라이언트의 메시지를 지속적으로 수신하는 함수 (스레드로 작동)
def handle_client(client_socket, addr):
    print(f"[+] 새 연결: {addr}")
    clients.append(client_socket)
    
    try:
        while True:
            # 클라이언트로부터 메시지 수신
            data = client_socket.recv(1024)
            if not data:
                break
                
            # 받은 메시지를 다른 모든 사람에게 공유
            print(f"[{addr}] {data.decode('utf-8')}")
            broadcast(data, client_socket)
            
    except ConnectionResetError:
        # 클라이언트가 강제 종료했을 때 처리
        pass
    finally:
        # 연결 종료 시 청소
        if client_socket in clients:
            clients.remove(client_socket)
        client_socket.close()
        print(f"[-] 연결 종료: {addr}")

# 메인 서버 실행 루프
def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen()
    print(f"[*] 메신저 서버가 {PORT}번 포트에서 시작되었습니다...")

    try:
        while True:
            # 새로운 클라이언트 접속 접수
            client_socket, addr = server_socket.accept()
            
            # 각 클라이언트 전용 개별 스레드 생성 및 실행 (동시 처리 핵심)
            client_thread = threading.Thread(target=handle_client, args=(client_socket, addr))
            client_thread.daemon = True # 서버 종료 시 스레드도 같이 종료되도록 설정
            client_thread.start()
            
    except KeyboardInterrupt:
        print("\n[*] 서버를 종료합니다.")
    finally:
        server_socket.close()

if __name__ == "__main__":
    start_server()
