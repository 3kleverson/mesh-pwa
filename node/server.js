"use strict";

const http = require("node:http");
const { randomUUID } = require("node:crypto");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = Number(process.env.PORT || 8080);
const MAX_CLIENTS = Number(process.env.MAX_CLIENTS || 500);
const MAX_ROOM_PEERS = Number(process.env.MAX_ROOM_PEERS || 12);
const MAX_MESSAGE_BYTES = 16 * 1024;

const allowedOrigins = new Set(
  (process.env.ALLOWED_ORIGINS || "")
    .split(",")
    .map(value => value.trim())
    .filter(Boolean)
);

const rooms = new Map();
const clients = new Set();

const server = http.createServer((req, res) => {
  if (req.url === "/health" && req.method === "GET") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({
      ok: true,
      clients: clients.size,
      rooms: rooms.size
    }));
  }

  res.writeHead(404, { "content-type": "text/plain" });
  res.end("Not found");
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: MAX_MESSAGE_BYTES,
  perMessageDeflate: false
});

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://localhost");
  const origin = req.headers.origin;

  if (url.pathname !== "/signal") {
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    return socket.destroy();
  }

  if (!origin || !allowedOrigins.has(origin)) {
    socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
    return socket.destroy();
  }

  if (clients.size >= MAX_CLIENTS) {
    socket.write("HTTP/1.1 503 Service Unavailable\r\n\r\n");
    return socket.destroy();
  }

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit("connection", ws, req);
  });
});

function send(ws, message) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function leaveRoom(ws) {
  if (!ws.room) return;

  const room = rooms.get(ws.room);
  if (room) {
    room.delete(ws.id);

    for (const peer of room.values()) {
      send(peer, { type: "peer-left", peerId: ws.id });
    }

    if (room.size === 0) rooms.delete(ws.room);
  }

  ws.room = null;
}

wss.on("connection", ws => {
  ws.id = randomUUID();
  ws.room = null;
  ws.messageCount = 0;
  ws.rateWindow = Date.now();
  ws.isAlive = true;
  clients.add(ws);

  send(ws, { type: "connected", peerId: ws.id });

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", raw => {
    const now = Date.now();

    if (now - ws.rateWindow > 10_000) {
      ws.rateWindow = now;
      ws.messageCount = 0;
    }

    if (++ws.messageCount > 120) {
      ws.close(1008, "Rate limit");
      return;
    }

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      ws.close(1007, "Invalid JSON");
      return;
    }

    if (!msg || typeof msg.type !== "string") {
      send(ws, { type: "error", error: "Invalid message" });
      return;
    }

    if (msg.type === "join") {
      if (ws.room) {
        send(ws, { type: "error", error: "Already joined a room" });
        return;
      }

      if (
        typeof msg.room !== "string" ||
        !/^[a-zA-Z0-9_-]{8,64}$/.test(msg.room)
      ) {
        send(ws, { type: "error", error: "Invalid room ID" });
        return;
      }

      let room = rooms.get(msg.room);
      if (!room) {
        room = new Map();
        rooms.set(msg.room, room);
      }

      if (room.size >= MAX_ROOM_PEERS) {
        send(ws, { type: "error", error: "Room full" });
        return;
      }

      ws.room = msg.room;
      room.set(ws.id, ws);

      send(ws, {
        type: "joined",
        room: msg.room,
        peers: [...room.keys()].filter(id => id !== ws.id)
      });

      for (const [id, peer] of room) {
        if (id !== ws.id) {
          send(peer, { type: "peer-joined", peerId: ws.id });
        }
      }
      return;
    }

    const allowedSignalTypes = new Set(["offer", "answer", "ice"]);

    if (!allowedSignalTypes.has(msg.type) || !ws.room) {
      send(ws, { type: "error", error: "Unexpected message" });
      return;
    }

    const room = rooms.get(ws.room);
    const target = room?.get(msg.to);

    if (!target || target === ws) {
      send(ws, { type: "error", error: "Peer not found" });
      return;
    }

    send(target, {
      type: msg.type,
      from: ws.id,
      data: msg.data
    });
  });

  ws.on("close", () => {
    leaveRoom(ws);
    clients.delete(ws);
  });

  ws.on("error", () => {
    leaveRoom(ws);
    clients.delete(ws);
  });
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }

    ws.isAlive = false;
    ws.ping();
  }
}, 30_000);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Mesh signaling node listening on ${PORT}`);
});

function shutdown() {
  clearInterval(heartbeat);
  for (const ws of wss.clients) ws.close(1001, "Server shutting down");
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10_000).unref();
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
