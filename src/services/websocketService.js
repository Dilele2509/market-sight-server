const WebSocket = require('ws');

// Tạo WebSocket Server
const wss = new WebSocket.Server({ port: 3002 }); 

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

module.exports = { broadcast };