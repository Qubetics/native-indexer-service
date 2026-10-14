import WebSocket from "ws";
import { log } from "../Utility/common";
import { COLORS } from "../constant";
import { RabbitMqService } from "../service";

export class CustomWebSocket {
  url: string;

  wsQuery: {
    jsonrpc: string;
    method: string;
    id: string;
    params: {
      query: string;
    };
  };

  websocket: null | WebSocket;

  queueName: string | null = null;

  reconnectInterval = 5000;

  constructor(
    url: string,
    wsFor: string,
    wsQuery: {
      jsonrpc: string;
      method: string;
      id: string;
      params: {
        query: string;
      };
    },
  ) {
    this.url = url;
    this.wsQuery = wsQuery;
    this.queueName = wsFor;
    // this.connect(); // Connect immediately upon initialization
  }

  onOpen() {
    log(
      "WebSocket connection opened for ",
      COLORS.yellow,
      this.queueName,
      COLORS.green,
    );
    if (this.websocket) this.websocket.send(JSON.stringify(this.wsQuery));
  }

  onMessage(event: any) {
    const message = JSON.parse(event);
    if (this.queueName !== "NATIVE_LATEST_BLOCKS") {
      console.log(this.queueName, "this.queue", "message ", message);
    }
    if (this.queueName) {
      if (this.queueName !== "NATIVE_LATEST_BLOCKS") {
        console.log(this.queueName, "this.queue", "inside the if condition");
      }
      RabbitMqService.inQueueData(this.queueName, JSON.stringify(message));
    }
  }

  onError(error: any) {
    console.error("Block events webSocket error:", error);
    this.reconnect();
  }

  onClose(code: number, reason: Buffer) {
    console.warn(
      "WebSocket connection closed due to\n",
      reason.toString("utf-8"),
      `closing code ${code}`,
    );
    this.reconnect();
  }

  connect() {
    try {
      this.websocket = new WebSocket(this.url);

      this.websocket.on("open", () => {
        this.onOpen();
      });

      this.websocket.on("message", (event) => {
        this.onMessage(event);
      });

      this.websocket.on("error", (error) => {
        this.onError(error);
      });

      this.websocket.on("close", (code: number, reason: Buffer) => {
        console.warn(
          "WebSocket connection closed due to\n",
          reason.toString("utf-8"),
          `closing code ${code}`,
        );
        this.onClose(code, reason);
      });
    } catch (err) {
      console.error(err);
      this.reconnect();
    }
  }

  reconnect() {
    if (this.websocket) {
      this.websocket.removeAllListeners(); // Remove all existing event listeners
      this.websocket = null;
    }
    setTimeout(() => {
      this.connect();
    }, this.reconnectInterval);
  }

  disconnect() {
    if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) return;
    this.websocket.send(
      JSON.stringify({ ...this.wsQuery, method: "unsubscribe" }),
    );
    this.websocket.close();
    this.websocket = null;
  }
}
