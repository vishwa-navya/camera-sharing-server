const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");

const app = express();

app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

io.on("connection", (socket) => {

  console.log("User Connected:", socket.id);

  socket.on("join-room", (roomId) => {
    socket.join(roomId);
  });

  socket.on("offer", (data) => {
    socket.to(data.roomId).emit("offer", data);
  });

  socket.on("answer", (data) => {
    socket.to(data.roomId).emit("answer", data);
  });

  socket.on("ice-candidate", (data) => {
    socket.to(data.roomId).emit("ice-candidate", data);
  });

  socket.on("camera-started", (data) => {
    socket.to(data.roomId).emit("camera-started", data);
  });

  socket.on("camera-stopped", (data) => {
    socket.to(data.roomId).emit("camera-stopped", data);
  });

  socket.on("disconnect", () => {
    console.log("Disconnected:", socket.id);
  });

});

app.get("/", (req, res) => {
  res.send("Camera Sharing Server Running");
});

const PORT = process.env.PORT || 5000;

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
