const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    maxHttpBufferSize: 1e8 // 100MB for image/audio transfers
});

app.use(express.static(path.join(__dirname, 'public')));

const users = new Map(); // socket.id -> username
const userLastSeen = new Map(); // username -> timestamp string
let chatHistory = []; // All messages

function getFormattedTime() {
    const d = new Date();
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

io.on('connection', (socket) => {

    socket.on('register_user', (username) => {
        users.set(socket.id, username);
        userLastSeen.delete(username.toLowerCase());
        broadcastUserList();
    });

    socket.on('load_private_chat', ({ user1, user2 }) => {
        chatHistory.forEach(msg => {
            if (msg.sender.toLowerCase() === user2.toLowerCase() && msg.receiver.toLowerCase() === user1.toLowerCase()) {
                msg.status = 'read';
            }
        });

        for (let [id, name] of users.entries()) {
            if (name.toLowerCase() === user2.toLowerCase()) {
                io.to(id).emit('messages_read_update', { by: user1 });
            }
        }

        const filteredHistory = chatHistory.filter(msg => 
            (msg.sender.toLowerCase() === user1.toLowerCase() && msg.receiver.toLowerCase() === user2.toLowerCase()) ||
            (msg.sender.toLowerCase() === user2.toLowerCase() && msg.receiver.toLowerCase() === user1.toLowerCase())
        );
        
        // Send only last 50 messages to keep it lightning fast
        const recentHistory = filteredHistory.slice(-50);
        socket.emit('load_history', recentHistory);
    });

    socket.on('private_message', (data) => {
        data.id = Date.now() + Math.random().toString(36).substr(2, 5);
        data.status = 'sent';
        data.time = getFormattedTime();

        let recipientSocketId = null;
        for (let [id, name] of users.entries()) {
            if (name.toLowerCase() === data.receiver.toLowerCase()) {
                recipientSocketId = id;
                data.status = 'delivered';
                break;
            }
        }

        chatHistory.push(data);
        
        // Prevent memory overflow by keeping maximum last 2000 global messages
        if (chatHistory.length > 2000) {
            chatHistory.shift();
        }

        if (recipientSocketId) {
            io.to(recipientSocketId).emit('chat_message', data);
        }
        
        socket.emit('chat_message', data);
    });

    socket.on('mark_read', ({ sender, receiver }) => {
        chatHistory.forEach(msg => {
            if (msg.sender.toLowerCase() === sender.toLowerCase() && msg.receiver.toLowerCase() === receiver.toLowerCase()) {
                msg.status = 'read';
            }
        });
        for (let [id, name] of users.entries()) {
            if (name.toLowerCase() === sender.toLowerCase()) {
                io.to(id).emit('messages_read_update', { by: receiver });
            }
        }
    });

    socket.on('typing', ({ receiver, isTyping, sender }) => {
        for (let [id, name] of users.entries()) {
            if (name.toLowerCase() === receiver.toLowerCase()) {
                io.to(id).emit('display_typing', { sender, isTyping });
            }
        }
    });

    socket.on('call_user', ({ userToCall, signalData, from, isVideo }) => {
        for (let [id, name] of users.entries()) {
            if (name.toLowerCase() === userToCall.toLowerCase()) {
                io.to(id).emit('incoming_call', { from, signal: signalData, isVideo });
            }
        }
    });

    socket.on('answer_call', ({ signal, to }) => {
        for (let [id, name] of users.entries()) {
            if (name.toLowerCase() === to.toLowerCase()) {
                io.to(id).emit('call_accepted', signal);
            }
        }
    });

    socket.on('ice_candidate', ({ candidate, to }) => {
        for (let [id, name] of users.entries()) {
            if (name.toLowerCase() === to.toLowerCase()) {
                io.to(id).emit('ice_candidate', candidate);
            }
        }
    });

    socket.on('end_call', ({ to }) => {
        for (let [id, name] of users.entries()) {
            if (name.toLowerCase() === to.toLowerCase()) {
                io.to(id).emit('call_ended');
            }
        }
    });

    socket.on('disconnect', () => {
        const username = users.get(socket.id);
        if (username) {
            userLastSeen.set(username.toLowerCase(), getFormattedTime());
            users.delete(socket.id);
        }
        broadcastUserList();
    });

    function broadcastUserList() {
        const activeUsers = Array.from(new Set(users.values()));
        const allKnownUsers = Array.from(new Set([
            ...activeUsers,
            ...chatHistory.map(m => m.sender),
            ...chatHistory.map(m => m.receiver)
        ]));
        
        const lastSeenObj = Object.fromEntries(userLastSeen);
        io.emit('contacts_update', { contacts: allKnownUsers, online: activeUsers, lastSeen: lastSeenObj });
    }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
