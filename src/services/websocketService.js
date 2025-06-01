import { WebSocketServer, WebSocket } from 'ws';
import dotenv from 'dotenv';

dotenv.config();

const { WS_PORT } = process.env;
let wss = null;

const initializeWebSocket = () => {
    try {
        if (wss) {
            console.log('WebSocket server already running');
            return;
        }

        wss = new WebSocketServer({ port: WS_PORT }); 

        wss.on('connection', (ws) => {
            console.log('Client connected');
            ws.on('close', () => console.log('Client disconnected'));
        });

        wss.on('error', (error) => {
            console.error('WebSocket server error:', error);
            if (error.code === 'EADDRINUSE') {
                console.log(`Port ${WS_PORT} is already in use. Please try a different port.`);
            }
        });

        console.log(`WebSocket server is running on port ${WS_PORT}`);
    } catch (error) {
        console.error('Failed to initialize WebSocket server:', error);
    }
};

// Hàm phát thông báo tới tất cả các client
const broadcast = (message) => {
    if (!wss) {
        console.error('WebSocket server is not initialized');
        return;
    }

    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(message));
        }
    });
};

// Cleanup function
const cleanup = () => {
    if (wss) {
        wss.close(() => {
            console.log('WebSocket server closed');
            wss = null;
        });
    }
};

// Initialize WebSocket server
initializeWebSocket();

// Handle process termination
process.on('SIGTERM', cleanup);
process.on('SIGINT', cleanup);

export { broadcast, cleanup };