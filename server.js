const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

app.use(express.static(path.join(__dirname, 'public')));

let users = {};
let lastSeenMap = {};
let messagesHistory = [];

io.on('connection', (socket) => {

    socket.on('register_user', (data) => {
        const username = typeof data === 'object' ? data.username : data;
        const clientTime = typeof data === 'object' ? data.time : '';
        if (!username) return;
        
        const normalized = username.trim().toLowerCase();
        socket.username = username.trim();
        users[normalized] = socket.id;

        io.emit('contacts_update', {
            contacts: Object.keys(users),
            online: Object.keys(users),
            lastSeen: lastSeenMap
        });
    });

    socket.on('update_last_seen', (data) => {
        if (!socket.username || !data.time) return;
        const normalized = socket.username.toLowerCase();
        lastSeenMap[normalized] = data.time;
        
        io.emit('contacts_update', {
            contacts: Object.keys(users),
            online: Object.keys(users),
            lastSeen: lastSeenMap
        });
    });

    socket.on('load_private_chat', ({ user1, user2 }) => {
        if (!user1 || !user2) return;
        const filtered = messagesHistory.filter(msg => 
            (msg.sender.toLowerCase() === user1.toLowerCase() && msg.receiver.toLowerCase() === user2.toLowerCase()) ||
            (msg.sender.toLowerCase() === user2.toLowerCase() && msg.receiver.toLowerCase() === user1.toLowerCase())
        );
        socket.emit('load_history', filtered);
    });

    socket.on('private_message', (data) => {
        const timeStr = data.time;
        const receiverSocketId = users[data.receiver.trim().toLowerCase()];
        const status = receiverSocketId ? 'delivered' : 'sent';

        const msgObj = {
            id: Date.now(),
            sender: data.sender,
            receiver: data.receiver,
            text: data.text,
            type: data.type || 'text',
            time: timeStr,
            status: status
        };

        messagesHistory.push(msgObj);

        socket.emit('chat_message', msgObj);
        if (receiverSocketId) {
            io.to(receiverSocketId).emit('chat_message', msgObj);
        }
    });

    socket.on('disconnect', () => {
        if (socket.username) {
            const normalized = socket.username.toLowerCase();
            delete users[normalized];
            
            io.emit('contacts_update', {
                contacts: Object.keys(users),
                online: Object.keys(users),
                lastSeen: lastSeenMap
            });
        }
    });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
