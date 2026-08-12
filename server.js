const express = require("express");
const http = require("http");
const path = require("path");
const crypto = require("crypto");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: true,
    methods: ["GET", "POST"]
  },
  transports: ["websocket", "polling"]
});

const PORT = process.env.PORT || 3000;

const meetings = new Map();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

function makeMeetingId() {
  return crypto.randomBytes(4).toString("hex").toUpperCase();
}

function safeName(name, fallback = "Participant") {
  return String(name || fallback)
    .trim()
    .slice(0, 80) || fallback;
}

function getMeeting(socket) {
  const id = socket.data.meetingId;
  return id ? meetings.get(id) : null;
}

function meetingInfo(meeting) {
  return {
    id: meeting.id,
    title: meeting.title,
    hostName: meeting.hostName,
    hostSocket: meeting.hostSocket,
    participantCount: meeting.participants.size,
    participants: [...meeting.participants.entries()].map(
      ([socketId, user]) => ({
        socketId,
        name: user.name,
        role: user.role
      })
    )
  };
}

/*
|--------------------------------------------------------------------------
| HTTP
|--------------------------------------------------------------------------
*/

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "AICTE SecureMeet",
    time: new Date().toISOString(),
    meetings: meetings.size
  });
});

app.get("/api/live-meetings", (req, res) => {
  res.json(
    [...meetings.values()].map(meetingInfo)
  );
});

/*
|--------------------------------------------------------------------------
| SOCKET.IO
|--------------------------------------------------------------------------
*/

io.on("connection", socket => {
  console.log("Connected:", socket.id);

  /*
  |--------------------------------------------------------------------------
  | List live meetings
  |--------------------------------------------------------------------------
  */

  socket.on("list-meetings", callback => {
    const list = [...meetings.values()].map(meetingInfo);

    if (typeof callback === "function") {
      callback({
        ok: true,
        meetings: list
      });
    }
  });

  /*
  |--------------------------------------------------------------------------
  | Admin creates live meeting
  |--------------------------------------------------------------------------
  */

  socket.on("create-meeting", (data = {}, callback) => {
    const name = safeName(data.name, "AICTE Administrator");
    const title = safeName(data.title, "AICTE Secure Meeting");

    let id;

    do {
      id = makeMeetingId();
    } while (meetings.has(id));

    const meeting = {
      id,
      title,
      hostSocket: socket.id,
      hostName: name,
      createdAt: Date.now(),
      participants: new Map()
    };

    meeting.participants.set(socket.id, {
      name,
      role: "Admin"
    });

    meetings.set(id, meeting);

    socket.join(id);

    socket.data.meetingId = id;
    socket.data.name = name;
    socket.data.role = "Admin";

    console.log(`Meeting created: ${id}`);

    if (typeof callback === "function") {
      callback({
        ok: true,
        meeting: meetingInfo(meeting),
        meetingId: id
      });
    }

    io.emit("live-meetings-updated");
  });

  /*
  |--------------------------------------------------------------------------
  | Participant joins meeting
  |--------------------------------------------------------------------------
  */

  socket.on("join-meeting", (data = {}, callback) => {
    const id = String(data.meetingId || "")
      .trim()
      .toUpperCase();

    const meeting = meetings.get(id);

    if (!meeting) {
      if (typeof callback === "function") {
        callback({
          ok: false,
          error: "Meeting not found or has ended."
        });
      }

      return;
    }

    if (socket.data.meetingId && socket.data.meetingId !== id) {
      removeSocket(socket);
    }

    const participantName = safeName(
      data.name,
      "Participant"
    );

    const existingParticipants = [
      ...meeting.participants.entries()
    ]
      .filter(([socketId]) => socketId !== socket.id)
      .map(([socketId, user]) => ({
        socketId,
        name: user.name,
        role: user.role
      }));

    meeting.participants.set(socket.id, {
      name: participantName,
      role: "Participant"
    });

    socket.join(id);

    socket.data.meetingId = id;
    socket.data.name = participantName;
    socket.data.role = "Participant";

    if (typeof callback === "function") {
      callback({
        ok: true,
        meeting: meetingInfo(meeting),
        meetingId: id,
        hostSocket: meeting.hostSocket,
        hostName: meeting.hostName,
        participants: existingParticipants
      });
    }

    socket.to(id).emit("participant-joined", {
      socketId: socket.id,
      name: participantName,
      role: "Participant"
    });

    io.to(id).emit(
      "participants-updated",
      meetingInfo(meeting)
    );

    io.emit("live-meetings-updated");

    console.log(
      `${participantName} joined ${id}`
    );
  });

  /*
  |--------------------------------------------------------------------------
  | WebRTC OFFER
  |--------------------------------------------------------------------------
  */

  socket.on("offer", data => {
    const meeting = getMeeting(socket);

    if (!meeting) return;

    const target = data?.target;
    const offer = data?.offer;

    if (!target || !offer) return;

    const targetSocket = io.sockets.sockets.get(target);

    if (!targetSocket) return;

    if (targetSocket.data.meetingId !== meeting.id) {
      return;
    }

    targetSocket.emit("offer", {
      sender: socket.id,
      offer
    });
  });

  /*
  |--------------------------------------------------------------------------
  | WebRTC ANSWER
  |--------------------------------------------------------------------------
  */

  socket.on("answer", data => {
    const meeting = getMeeting(socket);

    if (!meeting) return;

    const target = data?.target;
    const answer = data?.answer;

    if (!target || !answer) return;

    const targetSocket = io.sockets.sockets.get(target);

    if (!targetSocket) return;

    if (targetSocket.data.meetingId !== meeting.id) {
      return;
    }

    targetSocket.emit("answer", {
      sender: socket.id,
      answer
    });
  });

  /*
  |--------------------------------------------------------------------------
  | ICE CANDIDATE
  |--------------------------------------------------------------------------
  */

  socket.on("ice-candidate", data => {
    const meeting = getMeeting(socket);

    if (!meeting) return;

    const target = data?.target;
    const candidate = data?.candidate;

    if (!target || !candidate) return;

    const targetSocket = io.sockets.sockets.get(target);

    if (!targetSocket) return;

    if (targetSocket.data.meetingId !== meeting.id) {
      return;
    }

    targetSocket.emit("ice-candidate", {
      sender: socket.id,
      candidate
    });
  });

  /*
  |--------------------------------------------------------------------------
  | CHAT
  |--------------------------------------------------------------------------
  */

  socket.on("chat-message", data => {
    const meeting = getMeeting(socket);

    if (!meeting) return;

    const message = String(data?.message || "")
      .trim()
      .slice(0, 1000);

    if (!message) return;

    io.to(meeting.id).emit("chat-message", {
      sender: socket.data.name || "User",
      socketId: socket.id,
      message,
      time: Date.now()
    });
  });

  /*
  |--------------------------------------------------------------------------
  | REACTION
  |--------------------------------------------------------------------------
  */

  socket.on("reaction", data => {
    const meeting = getMeeting(socket);

    if (!meeting) return;

    const allowed = [
      "👍",
      "👏",
      "❤️",
      "😂",
      "🎉",
      "🔥",
      "😮"
    ];

    const reaction = String(data?.reaction || "");

    if (!allowed.includes(reaction)) return;

    io.to(meeting.id).emit("reaction", {
      sender: socket.data.name || "User",
      socketId: socket.id,
      reaction
    });
  });

  /*
  |--------------------------------------------------------------------------
  | LEAVE
  |--------------------------------------------------------------------------
  */

  socket.on("leave-meeting", () => {
    removeSocket(socket);
  });

  /*
  |--------------------------------------------------------------------------
  | ADMIN ENDS MEETING
  |--------------------------------------------------------------------------
  */

  socket.on("end-meeting", () => {
    const meeting = getMeeting(socket);

    if (!meeting) return;

    if (meeting.hostSocket !== socket.id) {
      return;
    }

    io.to(meeting.id).emit("meeting-ended");

    for (const [socketId] of meeting.participants) {
      const participant =
        io.sockets.sockets.get(socketId);

      if (participant) {
        participant.data.meetingId = null;
        participant.data.name = null;
        participant.data.role = null;
        participant.leave(meeting.id);
      }
    }

    console.log(`Meeting ended: ${meeting.id}`);

    meetings.delete(meeting.id);

    socket.data.meetingId = null;
    socket.data.name = null;
    socket.data.role = null;

    io.emit("live-meetings-updated");
  });

  /*
  |--------------------------------------------------------------------------
  | DISCONNECT
  |--------------------------------------------------------------------------
  */

  socket.on("disconnect", () => {
    console.log("Disconnected:", socket.id);

    removeSocket(socket);
  });
});

/*
|--------------------------------------------------------------------------
| REMOVE SOCKET FROM MEETING
|--------------------------------------------------------------------------
*/

function removeSocket(socket) {
  const meetingId = socket.data.meetingId;

  if (!meetingId) return;

  const meeting = meetings.get(meetingId);

  if (!meeting) {
    socket.data.meetingId = null;
    return;
  }

  const wasHost =
    meeting.hostSocket === socket.id;

  const name =
    socket.data.name || "Participant";

  meeting.participants.delete(socket.id);

  socket.to(meetingId).emit("participant-left", {
    socketId: socket.id,
    name
  });

  socket.leave(meetingId);

  socket.data.meetingId = null;
  socket.data.name = null;
  socket.data.role = null;

  /*
   * If host leaves, terminate the meeting.
   */

  if (wasHost) {
    io.to(meetingId).emit("meeting-ended");

    meetings.delete(meetingId);

    console.log(
      `Host left. Meeting ended: ${meetingId}`
    );
  } else {
    io.to(meetingId).emit(
      "participants-updated",
      meetingInfo(meeting)
    );
  }

  io.emit("live-meetings-updated");
}

/*
|--------------------------------------------------------------------------
| START
|--------------------------------------------------------------------------
*/

server.listen(PORT, "0.0.0.0", () => {
  console.log(
    `AICTE SecureMeet running on port ${PORT}`
  );
});
