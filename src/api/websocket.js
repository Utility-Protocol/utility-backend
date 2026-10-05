import { WebSocketServer } from 'ws';

let wss = null;

/**
 * Attaches a WebSocket server to the HTTP server and keeps a client set so
 * broadcasts can fan out to every connected listener.
 */
export function initWebSocket(httpServer) {
  wss = new WebSocketServer({ server: httpServer, path: '/stream/ticks' });

  wss.on('connection', (socket, req) => {
    socket.isAlive = true;
    socket.on('pong', () => {
      socket.isAlive = true;
    });
    socket.on('error', (err) => console.error('[ws] socket error:', err.message));
    socket.send(
      JSON.stringify({
        type: 'CONNECTED',
        data: { remote: req.socket.remoteAddress, at: Math.floor(Date.now() / 1000) },
      })
    );
  });

  // Drop half-open connections so the client set cannot grow unbounded.
  const heartbeat = setInterval(() => {
    for (const socket of wss.clients) {
      if (socket.isAlive === false) {
        socket.terminate();
        continue;
      }
      socket.isAlive = false;
      socket.ping();
    }
  }, 30_000);

  // Do not let the heartbeat alone hold the event loop open; otherwise test
  // runs never exit.
  heartbeat.unref?.();

  wss.on('close', () => clearInterval(heartbeat));

  return wss;
}

export function broadcastEvent(frame) {
  if (!wss) return 0;
  const payload = JSON.stringify(frame);
  let sent = 0;
  for (const socket of wss.clients) {
    if (socket.readyState === socket.OPEN) {
      socket.send(payload);
      sent += 1;
    }
  }
  return sent;
}

export function clientCount() {
  return wss ? wss.clients.size : 0;
}

export function closeWebSocket() {
  if (wss) {
    for (const socket of wss.clients) socket.terminate();
    wss.close();
    wss = null;
  }
}