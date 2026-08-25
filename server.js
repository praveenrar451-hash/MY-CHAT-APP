const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

const db = new sqlite3.Database('./database.db');

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sender TEXT,
        receiver TEXT,
        text TEXT,
        type TEXT,
        time TEXT,
        status TEXT
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS last_seen (
        username TEXT PRIMARY KEY,
        time TEXT
    )`);
});

app.use(express.static(path.join(__dirname, 'public')));

let users = {};
let lastSeenMap = {};

io.on('connection', (socket) => {

    socket.on('register_user', (data) => {
        const username = typeof data === 'object' ? data.username : data;
        const clientTime = typeof data === 'object' ? data.time : '';
        if (!username) return;
        
        const normalized = username.trim().toLowerCase();
        socket.username = username.trim();
        users[normalized] = socket.id;

        db.all("SELECT username, time FROM last_seen", [], (err, rows) => {
            if (!err && rows) {
                rows.forEach(r => {
                    lastSeenMap[r.username.toLowerCase()] = r.time;
                });
            }
            io.emit('contacts_update', {
                contacts: Object.keys(users),
                online: Object.keys(users),
                lastSeen: lastSeenMap
            });
        });
    });

    socket.on('update_last_seen', (data) => {
        if (!socket.username || !data.time) return;
        const normalized = socket.username.toLowerCase();
        db.run(`INSERT OR REPLACE INTO last_seen (username, time) VALUES (?, ?)`, [normalized, data.time], () => {
            lastSeenMap[normalized] = data.time;
            io.emit('contacts_update', {
                contacts: Object.keys(users),
                online: Object.keys(users),
                lastSeen: lastSeenMap
            });
        });
    });

    socket.on('load_private_chat', ({ user1, user2 }) => {
        if (!user1 || !user2) return;
        const query = `
            SELECT * FROM messages 
            WHERE (LOWER(sender) = LOWER(?) AND LOWER(receiver) = LOWER(?))
               OR (LOWER(sender) = LOWER(?) AND LOWER(receiver) = LOWER(?))
            ORDER BY id ASC
        `;
        db.all(query, [user1, user2, user2, user1], (err, rows) => {
            if (!err) {
                socket.emit('load_history', rows || []);
            }
        });
    });

    socket.on('private_message', (data) => {
        const timeStr = data.time;
        const receiverSocketId = users[data.receiver.trim().toLowerCase()];
        const status = receiverSocketId ? 'delivered' : 'sent';

        const stmt = db.prepare(`INSERT INTO messages (sender, receiver, text, type, time, status) VALUES (?, ?, ?, ?, ?, ?)`);
        stmt.run(data.sender, data.receiver, data.text, data.type, timeStr, status, function(err) {
            if (err) return;
            const msgObj = {
                id: this.lastID,
                sender: data.sender,
                receiver: data.receiver,
                text: data.text,
                type: data.type,
                time: timeStr,
                status: status
            };

            socket.emit('chat_message', msgObj);
            if (receiverSocketId) {
                io.to(receiverSocketId).emit('chat_message', msgObj);
            }
        });
        stmt.finalize();
    });

    socket.on('mark_read', ({ sender, receiver }) => {
        db.run(`UPDATE messages SET status = 'read' WHERE LOWER(sender) = LOWER(?) AND LOWER(receiver) = LOWER(?)`, [sender, receiver], () => {
            const senderSocketId = users[sender.trim().toLowerCase()];
            if (senderSocketId) {
                io.to(senderSocketId).emit('messages_read_update', { by: receiver });
            }
        });
    });

    socket.on('typing', ({ receiver, isTyping, sender }) => {
        const receiverSocketId = users[receiver.trim().toLowerCase()];
        if (receiverSocketId) {
            io.to(receiverSocketId).emit('display_typing', { sender, isTyping });
        }
    });

    socket.on('call_user', ({ userToCall, signalData, from, isVideo }) => {
        const receiverSocketId = users[userToCall.trim().toLowerCase()];
        if (receiverSocketId) {
            io.to(receiverSocketId).emit('incoming_call', { signal: signalData, from, isVideo });
        }
    });

    socket.on('answer_call', (data) => {
        const receiverSocketId = users[data.to.trim().toLowerCase()];
        if (receiverSocketId) {
            io.to(receiverSocketId).emit('call_accepted', data.signal);
        }
    });

    socket.on('ice_candidate', ({ candidate, to }) => {
        const receiverSocketId = users[to.trim().toLowerCase()];
        if (receiverSocketId) {
            io.to(receiverSocketId).emit('ice_candidate', candidate);
        }
    });

    socket.on('end_call', ({ to }) => {
        const receiverSocketId = users[to.trim().toLowerCase()];
        if (receiverSocketId) {
            io.to(receiverSocketId).emit('call_ended');
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
