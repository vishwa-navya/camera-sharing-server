const express    = require("express");
const http       = require("http");
const { Server } = require("socket.io");

const app    = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

app.get("/", (req, res) => res.send("Camera + Call Signaling Server"));

// room → { socketId: userName }
const rooms = {};

// Track which user is in which call socket
const callUsers = {}; // userName → socketId

io.on("connection", (socket) => {
  console.log("🔌 Connected:", socket.id);

  // ── Camera sharing room ────────────────────────────────────────────────────
  socket.on("join", ({ room, user }) => {
    socket.join(room);
    socket.data.room = room;
    socket.data.user = user;
    if (!rooms[room]) rooms[room] = {};
    rooms[room][socket.id] = user;
    const count = Object.keys(rooms[room]).length;
    socket.emit("joined", { room, count });
    if (count > 1) {
      Object.entries(rooms[room]).forEach(([sid, name]) => {
        if (sid !== socket.id && name === "Vishwa") {
          io.to(sid).emit("request-offer", { to: user });
        }
      });
    }
  });

  socket.on("camera-ready", ({ room, from }) => socket.to(room).emit("camera-ready", { from }));
  socket.on("offer",        ({ room, from, sdp }) => socket.to(room).emit("offer", { from, sdp }));
  socket.on("answer",       ({ room, from, sdp }) => socket.to(room).emit("answer", { from, sdp }));
  socket.on("ice",          ({ room, from, candidate }) => socket.to(room).emit("ice", { from, candidate }));
  socket.on("camera-off",   ({ room, from }) => socket.to(room).emit("camera-off", { from }));

  // ── Voice call signaling ───────────────────────────────────────────────────

  // User registers for call notifications
  socket.on("call-join", ({ room, user }) => {
    socket.join(room);
    callUsers[user] = socket.id;
    socket.data.callUser = user;
    socket.data.callRoom = room;
    console.log(`📞 ${user} registered for calls`);
  });

  // Caller wants to call someone
  socket.on("call-user", ({ room, from, to }) => {
    const targetSocketId = callUsers[to];
    if (!targetSocketId) {
      // Target user is offline
      socket.emit("call-user-offline");
      console.log(`📵 ${to} is offline`);
      return;
    }
    console.log(`📞 ${from} calling ${to}`);
    io.to(targetSocketId).emit("call-incoming", { from });
  });

  // Receiver accepted
  socket.on("call-accept", ({ room, from }) => {
    console.log(`✅ ${from} accepted call`);
    socket.to(room).emit("call-accepted", { from });
  });

  // Receiver rejected
  socket.on("call-reject", ({ room, from }) => {
    console.log(`❌ ${from} rejected call`);
    socket.to(room).emit("call-rejected", { from });
  });

  // End call
  socket.on("call-end", ({ room, from }) => {
    console.log(`📴 ${from} ended call`);
    socket.to(room).emit("call-ended", { from });
  });

  // WebRTC for call
  socket.on("call-offer",  ({ room, from, sdp })       => socket.to(room).emit("call-offer",  { from, sdp }));
  socket.on("call-answer", ({ room, from, sdp })       => socket.to(room).emit("call-answer", { from, sdp }));
  socket.on("call-ice",    ({ room, from, candidate }) => socket.to(room).emit("call-ice",    { from, candidate }));

  // ── Disconnect ─────────────────────────────────────────────────────────────
  socket.on("disconnect", () => {
    const { room, user, callUser, callRoom } = socket.data;

    // Camera room cleanup
    if (room && rooms[room]) {
      delete rooms[room][socket.id];
      if (Object.keys(rooms[room]).length === 0) delete rooms[room];
      else socket.to(room).emit("camera-off", { from: user });
    }

    // Call cleanup
    if (callUser) {
      delete callUsers[callUser];
      if (callRoom) socket.to(callRoom).emit("call-ended", { from: callUser });
    }

    console.log(`🔌 Disconnected: ${user || callUser || socket.id}`);
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));
