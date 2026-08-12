const express = require("express");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT) || 3000;
const HOST = "0.0.0.0";

const io = new Server(server, {
  cors: {
    origin: true,
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"]
});

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const meetings = new Map();

function makeMeetingId() {
  let id;

  do {
    id = crypto.randomBytes(4).toString("hex").toUpperCase();
  } while (meetings.has(id));

  return id;
}

function safeName(value, fallback = "Participant") {
  const name = String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 80);

  return name || fallback;
}

function safeText(value, max = 1000) {
  return String(value || "")
    .trim()
    .slice(0, max);
}

function getMeeting(socket) {
  const meetingId = socket.data.meetingId;

  if (!meetingId) {
    return null;
  }

  return meetings.get(meetingId) || null;
}

function publicParticipant(socketId, participant) {
  return {
    socketId,
    name: participant.name,
    role: participant.role
  };
}

function meetingParticipants(meeting) {
  return [...meeting.participants.entries()]
    .map(([socketId, participant]) =>
      publicParticipant(socketId, participant)
    );
}

function removeFromCurrentMeeting(socket) {
  const meetingId = socket.data.meetingId;

  if (!meetingId) {
    return;
  }

  const meeting = meetings.get(meetingId);

  socket.data.meetingId = null;

  if (!meeting) {
    return;
  }

  const wasHost = meeting.hostSocket === socket.id;

  meeting.participants.delete(socket.id);
  socket.leave(meetingId);

  if (wasHost) {
    io.to(meetingId).emit("meeting-ended");

    meetings.delete(meetingId);
    return;
  }

  socket.to(meetingId).emit("participant-left", {
    socketId: socket.id
  });

  io.to(meetingId).emit("participants-updated", {
    participants: meetingParticipants(meeting)
  });

  if (meeting.participants.size === 0) {
    meetings.delete(meetingId);
  }
}

io.on("connection", (socket) => {
  console.log("Socket connected:", socket.id);

  /*
   * List active meetings
   */
  socket.on("list-meetings", (callback) => {
    const result = [...meetings.values()].map((meeting) => ({
      id: meeting.id,
      title: meeting.title,
      hostName: meeting.hostName,
      participantCount: meeting.participants.size
    }));

    if (typeof callback === "function") {
      callback(result);
    }
  });

  /*
   * Create meeting
   */
  socket.on("create-meeting", (data = {}, callback) => {
    try {
      removeFromCurrentMeeting(socket);

      const hostName = safeName(data.name, "Admin");
      const title = safeName(data.title, "AICTE Secure Meeting");

      const id = makeMeetingId();

      const meeting = {
        id,
        title,
        hostSocket: socket.id,
        hostName,
        createdAt: Date.now(),
        participants: new Map()
      };

      meeting.participants.set(socket.id, {
        name: hostName,
        role: "Admin"
      });

      meetings.set(id, meeting);

      socket.join(id);

      socket.data.meetingId = id;
      socket.data.name = hostName;
      socket.data.role = "Admin";

      console.log(
        `Meeting created: ${id} by ${hostName} (${socket.id})`
      );

      callback?.({
        ok: true,
        meetingId: id,
        title: meeting.title,
        hostSocket: meeting.hostSocket,
        hostName: meeting.hostName,
        participants: meetingParticipants(meeting)
      });
    } catch (error) {
      console.error("create-meeting error:", error);

      callback?.({
        ok: false,
        error: "Unable to create meeting."
      });
    }
  });

  /*
   * Join meeting
   */
  socket.on("join-meeting", (data = {}, callback) => {
    try {
      const meetingId = String(data.meetingId || "")
        .trim()
        .toUpperCase();

      const meeting = meetings.get(meetingId);

      if (!meeting) {
        callback?.({
          ok: false,
          error: "Meeting not found or has ended."
        });

        return;
      }

      removeFromCurrentMeeting(socket);

      const participantName = safeName(data.name);

      /*
       * IMPORTANT:
       * Return existing participants BEFORE adding this socket.
       * The new client will create peer connections to them.
       */
      const existingParticipants =
        meetingParticipants(meeting);

      meeting.participants.set(socket.id, {
        name: participantName,
        role: "Participant"
      });

      socket.join(meetingId);

      socket.data.meetingId = meetingId;
      socket.data.name = participantName;
      socket.data.role = "Participant";

      callback?.({
        ok: true,
        meetingId,
        title: meeting.title,
        hostSocket: meeting.hostSocket,
        hostName: meeting.hostName,
        participants: existingParticipants
      });

      socket.to(meetingId).emit("participant-joined", {
        socketId: socket.id,
        name: participantName,
        role: "Participant"
      });

      io.to(meetingId).emit("participants-updated", {
        participants: meetingParticipants(meeting)
      });

      console.log(
        `${participantName} joined ${meetingId} (${socket.id})`
      );
    } catch (error) {
      console.error("join-meeting error:", error);

      callback?.({
        ok: false,
        error: "Unable to join meeting."
      });
    }
  });

  /*
   * WebRTC OFFER
   */
  socket.on("offer", (data = {}) => {
    const meeting = getMeeting(socket);

    if (!meeting) {
      return;
    }

    const target = String(data.target || "");

    if (!target || !data.offer) {
      return;
    }

    if (!meeting.participants.has(target)) {
      return;
    }

    io.to(target).emit("offer", {
      sender: socket.id,
      offer: data.offer
    });
  });

  /*
   * WebRTC ANSWER
   */
  socket.on("answer", (data = {}) => {
    const meeting = getMeeting(socket);

    if (!meeting) {
      return;
    }

    const target = String(data.target || "");

    if (!target || !data.answer) {
      return;
    }

    if (!meeting.participants.has(target)) {
      return;
    }

    io.to(target).emit("answer", {
      sender: socket.id,
      answer: data.answer
    });
  });

  /*
   * ICE CANDIDATE
   */
  socket.on("ice-candidate", (data = {}) => {
    const meeting = getMeeting(socket);

    if (!meeting) {
      return;
    }

    const target = String(data.target || "");

    if (!target || !data.candidate) {
      return;
    }

    if (!meeting.participants.has(target)) {
      return;
    }

    io.to(target).emit("ice-candidate", {
      sender: socket.id,
      candidate: data.candidate
    });
  });

  /*
   * CHAT
   */
  socket.on("chat-message", (data = {}) => {
    const meeting = getMeeting(socket);

    if (!meeting) {
      return;
    }

    const message = safeText(data.message, 1000);

    if (!message) {
      return;
    }

    io.to(meeting.id).emit("chat-message", {
      sender: socket.data.name || "Participant",
      socketId: socket.id,
      message,
      time: Date.now()
    });
  });

  /*
   * REACTION
   */
  socket.on("reaction", (data = {}) => {
    const meeting = getMeeting(socket);

    if (!meeting) {
      return;
    }

    const allowed = ["👏", "👍", "❤️", "😂", "🎉", "🔥"];

    if (!allowed.includes(data.reaction)) {
      return;
    }

    io.to(meeting.id).emit("reaction", {
      socketId: socket.id,
      name: socket.data.name || "Participant",
      reaction: data.reaction
    });
  });

  /*
   * Leave
   */
  socket.on("leave-meeting", () => {
    console.log(
      `${socket.data.name || socket.id} left`
    );

    removeFromCurrentMeeting(socket);
  });

  /*
   * Host ends meeting
   */
  socket.on("end-meeting", () => {
    const meeting = getMeeting(socket);

    if (!meeting) {
      return;
    }

    if (meeting.hostSocket !== socket.id) {
      return;
    }

    console.log(`Meeting ended: ${meeting.id}`);

    io.to(meeting.id).emit("meeting-ended");

    for (const socketId of meeting.participants.keys()) {
      const participantSocket =
        io.sockets.sockets.get(socketId);

      if (participantSocket) {
        participantSocket.data.meetingId = null;
        participantSocket.leave(meeting.id);
      }
    }

    meetings.delete(meeting.id);
  });

  /*
   * Disconnect
   */
  socket.on("disconnect", (reason) => {
    console.log(
      `Socket disconnected: ${socket.id} (${reason})`
    );

    removeFromCurrentMeeting(socket);
  });
});

/*
 * Health endpoint
 */
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "AICTE SecureMeet",
    time: new Date().toISOString(),
    meetings: meetings.size
  });
});

/*
 * Simple API information
 */
app.get("/api/status", (req, res) => {
  res.json({
    ok: true,
    service: "AICTE SecureMeet",
    socketio: true,
    webrtc: true,
    meetings: meetings.size
  });
});

/*
 * SPA fallback
 */
app.get("*", (req, res) => {
  res.sendFile(
    path.join(__dirname, "public", "index.html")
  );
});

server.listen(PORT, HOST, () => {
  console.log(
    `AICTE SecureMeet running on http://${HOST}:${PORT}`
  );
});
