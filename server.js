/**
 * server.js — Final (Camera + Voice Call)
 *
 * Multi-device fix:
 *  callUsers[userName] = [socketId1, socketId2, ...]
 *  When Ammu calls Vishwa, ALL of Vishwa's connected devices get the ring.
 *  Whichever device Vishwa accepts on — that socket handles the WebRTC.
 */

const express    = require("express");
const http       = require("http");
const { Server } = require("socket.io");

const app    = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

app.get("/",       (req, res) => res.send("Camera + Call Signaling Server ✅"));
app.get("/health", (req, res) => res.json({ ok: true }));

// ── Camera room state ──────────────────────────────────────────────────────────
const rooms = {}; // { roomId: { socketId: userName } }

// ── Call state ─────────────────────────────────────────────────────────────────
// Multi-device: one user can have multiple sockets (phone + laptop both open)
const callUsers = {}; // { userName: Set<socketId> }

function addCallUser(user, socketId) {
  if (!callUsers[user]) callUsers[user] = new Set();
  callUsers[user].add(socketId);
}

function removeCallUser(user, socketId) {
  if (callUsers[user]) {
    callUsers[user].delete(socketId);
    if (callUsers[user].size === 0) delete callUsers[user];
  }
}

function getCallSockets(user) {
  return callUsers[user] ? [...callUsers[user]] : [];
}

io.on("connection", (socket) => {
  console.log("🔌 Connected:", socket.id);

  // ── Camera sharing ──────────────────────────────────────────────────────────
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

  socket.on("camera-ready", ({ room, from })        => socket.to(room).emit("camera-ready", { from }));
  socket.on("offer",        ({ room, from, sdp })   => socket.to(room).emit("offer",  { from, sdp }));
  socket.on("answer",       ({ room, from, sdp })   => socket.to(room).emit("answer", { from, sdp }));
  socket.on("ice",          ({ room, from, candidate }) => socket.to(room).emit("ice", { from, candidate }));
  socket.on("camera-off",   ({ room, from })        => socket.to(room).emit("camera-off", { from }));

  // ── Voice call ──────────────────────────────────────────────────────────────

  // Register for calls (both devices register)
  socket.on("call-join", ({ room, user }) => {
    socket.join(room);
    socket.data.callUser = user;
    socket.data.callRoom = room;
    addCallUser(user, socket.id);
    const count = getCallSockets(user).length;
    console.log(`📞 ${user} registered (${count} device(s))`);
  });

  // Ammu calls Vishwa — ring ALL of Vishwa's devices
  socket.on("call-user", ({ room, from, to }) => {
    const targets = getCallSockets(to);
    if (targets.length === 0) {
      socket.emit("call-user-offline");
      console.log(`📵 ${to} has no devices online`);
      return;
    }
    console.log(`📞 ${from} → ${to} (${targets.length} device(s))`);
    targets.forEach(sid => {
      io.to(sid).emit("call-incoming", { from });
    });
  });

  // Vishwa accepted — tell Ammu (all her devices)
  socket.on("call-accept", ({ room, from }) => {
    console.log(`✅ ${from} accepted`);
    // Cancel ring on Vishwa's OTHER devices
    const myOtherDevices = getCallSockets(from).filter(sid => sid !== socket.id);
    myOtherDevices.forEach(sid => io.to(sid).emit("call-cancelled-other-device"));
    // Tell caller
    socket.to(room).emit("call-accepted", { from });
  });

  socket.on("call-reject", ({ room, from }) => {
    console.log(`❌ ${from} rejected`);
    socket.to(room).emit("call-rejected", { from });
  });

  socket.on("call-end", ({ room, from }) => {
    console.log(`📴 ${from} ended call`);
    socket.to(room).emit("call-ended", { from });
  });

  // WebRTC for voice call
  socket.on("call-offer",  ({ room, from, sdp })       => socket.to(room).emit("call-offer",  { from, sdp }));
  socket.on("call-answer", ({ room, from, sdp })       => socket.to(room).emit("call-answer", { from, sdp }));
  socket.on("call-ice",    ({ room, from, candidate }) => socket.to(room).emit("call-ice",    { from, candidate }));

  // ── Disconnect ──────────────────────────────────────────────────────────────
  socket.on("disconnect", () => {
    const { room, user, callUser, callRoom } = socket.data;

    // Camera cleanup
    if (room && rooms[room]) {
      delete rooms[room][socket.id];
      if (Object.keys(rooms[room]).length === 0) delete rooms[room];
      else socket.to(room).emit("camera-off", { from: user });
    }

    // Call cleanup
    if (callUser) {
      removeCallUser(callUser, socket.id);
      const remaining = getCallSockets(callUser).length;
      console.log(`🔌 ${callUser} disconnected (${remaining} device(s) left)`);
      if (remaining === 0 && callRoom) {
        socket.to(callRoom).emit("call-ended", { from: callUser });
      }
    }

    console.log(`🔌 Disconnected: ${socket.id}`);
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));
