const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

app.get("/", (req, res) => res.send("Camera Sharing Server Running"));

// room → { socketId: userName }
const rooms = {};

io.on("connection", (socket) => {
  console.log("🔌 Connected:", socket.id);

  socket.on("join", ({ room, user }) => {
    socket.join(room);
    socket.data.room = room;
    socket.data.user = user;

    if (!rooms[room]) rooms[room] = {};
    rooms[room][socket.id] = user;

    const count = Object.keys(rooms[room]).length;
    console.log(`👤 ${user} joined ${room}. Total: ${count}`);

    // Tell THIS user they joined successfully + how many are in room
    socket.emit("joined", { room, count });

    // If there was already someone in the room, tell Vishwa to send a fresh offer
    if (count > 1) {
      // Find the other socket(s) in the room
      Object.entries(rooms[room]).forEach(([sid, name]) => {
        if (sid !== socket.id && name === "Vishwa") {
          // Tell Vishwa to send offer to new joiner
          io.to(sid).emit("request-offer", { to: user });
          console.log(`📨 Told Vishwa to send offer to ${user}`);
        }
      });
    }
  });

  socket.on("camera-ready", ({ room, from }) => {
    console.log(`📷 camera-ready: ${from}`);
    socket.to(room).emit("camera-ready", { from });
  });

  socket.on("offer", ({ room, from, sdp }) => {
    console.log(`📡 offer from ${from}`);
    socket.to(room).emit("offer", { from, sdp });
  });

  socket.on("answer", ({ room, from, sdp }) => {
    console.log(`📡 answer from ${from}`);
    socket.to(room).emit("answer", { from, sdp });
  });

  socket.on("ice", ({ room, from, candidate }) => {
    socket.to(room).emit("ice", { from, candidate });
  });

  socket.on("camera-off", ({ room, from }) => {
    console.log(`❌ camera-off: ${from}`);
    socket.to(room).emit("camera-off", { from });
  });

  socket.on("disconnect", () => {
    const { room, user } = socket.data;
    console.log(`🔌 Disconnected: ${user}`);
    if (room && rooms[room]) {
      delete rooms[room][socket.id];
      if (Object.keys(rooms[room]).length === 0) delete rooms[room];
      else socket.to(room).emit("camera-off", { from: user });
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));
