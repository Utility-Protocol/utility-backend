import { WebSocket, WebSocketServer } from 'ws';

const WELCOME_MESSAGE = JSON.stringify({
  type: 'WELCOME',
  data: { message: 'Connected to Utility Protocol telemetry stream' },
});

export function attachWebSocket(httpServer) {
  const wss = new WebSocketServer({ server: httpServer, path: '/stream' });

  wss.on('connection', (socket) => {
    socket.on('error', (err) => {
      console.error('[ws] client error:', err instanceof Error ? err.message : err);
    });
    try {
      socket.send(WELCOME_MESSAGE);
    } catch (err) {
      console.error('[ws] failed to send welcome:', err instanceof Error ? err.message : err);
    }
  });

  wss.on('error', (err) => {
    console.error('[ws] server error:', err instanceof Error ? err.message : err);
  });

  return wss;
}

export function broadcast(wss, payload) {
  if (!wss) return;
  const message = JSON.stringify(payload);
  for (const client of wss.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;
    try {
      client.send(message);
    } catch (err) {
      console.error('[ws] send failed:', err instanceof Error ? err.message : err);
    }
  }
}