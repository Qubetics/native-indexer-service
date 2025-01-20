import { Socket } from "socket.io";

type GlobalWithSocket = {
  socket: Socket;
};

declare const global: GlobalWithSocket;
class SocketEventEmitter {
  emitMessage<T>(eventName: string, data: T) {
    const globalVar: GlobalWithSocket = global;
    globalVar.socket.emit(eventName, data);
  }
}

export default new SocketEventEmitter();
