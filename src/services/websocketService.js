import { WebSocketServer, WebSocket } from 'ws';
import dotenv from 'dotenv';

dotenv.config();

const { WS_PORT } = process.env;
// Tạo WebSocket Server
const wss = new WebSocketServer({ port: WS_PORT }); 

wss.on('connection', (ws) => {
    console.log('Client connected');
    ws.on('close', () => console.log('Client disconnected'));
});

// Hàm phát thông báo tới tất cả các client
const broadcast = (message) => {
    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(message));
        }
    });
};

export { broadcast };