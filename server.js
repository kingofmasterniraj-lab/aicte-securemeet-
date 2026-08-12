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

// Serve public/index.html, CSS, JS, etc.
app.use(express.static(path.join(__dirname, "public")));


// =====================================================
// HELPERS
// =====================================================

function makeMeetingId() {
  return crypto.randomBytes(4).toString("hex").toUpperCase();
}

function safeName(name, fallback = "Participant") {
  const value = String(name ?? "").trim().slice(0, 80);
  return value || fallback;
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

    participants: Array.from(
      meeting.participants.entries()
    ).map(([socketId, user]) => ({
      socketId,
      name: user.name,
      role: user.role
    }))
  };
}


// =====================================================
// REMOVE SOCKET FROM MEETING
// =====================================================

function removeSocket(socket) {
  const meetingId = socket.data.meetingId;

  if (!meetingId) {
    return;
  }

  const meeting = meetings.get(meetingId);

  if (!meeting) {
    socket.data.meetingId = null;
    socket.data.name = null;
    socket.data.role = null;
    return;
  }

  const wasHost = meeting.hostSocket === socket.id;
  const name = socket.data.name || "Participant";

  meeting.participants.delete(socket.id);

  socket.to(meetingId).emit("participant-left", {
    socketId: socket.id,
    name
  });

  socket.leave(meetingId);

  socket.data.meetingId = null;
  socket.data.name = null;
  socket.data.role = null;

  // If admin/host leaves, end the meeting.
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


// =====================================================
// HEALTH
// =====================================================

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "AICTE SecureMeet",
    time: new Date().toISOString(),
    meetings: meetings.size
  });
});


// =====================================================
// REST API - LIVE MEETINGS
// =====================================================

app.get("/api/live-meetings", (req, res) => {
  res.json(
    Array.from(meetings.values()).map(meetingInfo)
  );
});


// =====================================================
// SOCKET.IO
// =====================================================

io.on("connection", (socket) => {
  console.log("Socket connected:", socket.id);


  // ===================================================
  // LIST MEETINGS
  // ===================================================

  socket.on("list-meetings", (callback) => {
    const list = Array.from(
      meetings.values()
    ).map(meetingInfo);

    if (typeof callback === "function") {
      callback({
        ok: true,
        meetings: list
      });
    }
  });


  // ===================================================
  // CREATE MEETING
  // ===================================================

  socket.on(
    "create-meeting",
    (data = {}, callback) => {

      try {
        // Leave previous meeting if necessary.
        if (socket.data.meetingId) {
          removeSocket(socket);
        }

        let meetingId;

        do {
          meetingId = makeMeetingId();
        } while (meetings.has(meetingId));


        const hostName = safeName(
          data.name,
          "AICTE Administrator"
        );

        const title = safeName(
          data.title,
          "AICTE Secure Meeting"
        );


        const meeting = {
          id: meetingId,
          title,
          hostSocket: socket.id,
          hostName,
          createdAt: Date.now(),
          participants: new Map()
        };


        // Admin is a participant too.
        meeting.participants.set(
          socket.id,
          {
            name: hostName,
            role: "Admin"
          }
        );


        meetings.set(
          meetingId,
          meeting
        );


        socket.join(meetingId);

        socket.data.meetingId = meetingId;
        socket.data.name = hostName;
        socket.data.role = "Admin";


        console.log(
          `Meeting created: ${meetingId} by ${hostName}`
        );


        if (typeof callback === "function") {
          callback({
            ok: true,
            meetingId,
            title,
            hostSocket: socket.id,
            hostName,
            meeting: meetingInfo(meeting)
          });
        }


        // Notify all connected clients.
        io.emit(
          "meeting-created",
          meetingInfo(meeting)
        );

        io.emit(
          "live-meetings-updated"
        );

      } catch (error) {

        console.error(
          "CREATE MEETING ERROR:",
          error
        );

        if (typeof callback === "function") {
          callback({
            ok: false,
            error: "Unable to create meeting."
          });
        }
      }
    }
  );


  // ===================================================
  // JOIN MEETING
  // ===================================================

  socket.on(
    "join-meeting",
    (data = {}, callback) => {

      try {

        const meetingId = String(
          data.meetingId || ""
        )
          .trim()
          .toUpperCase();


        if (!meetingId) {
          callback?.({
            ok: false,
            error: "Meeting ID is required."
          });

          return;
        }


        const meeting =
          meetings.get(meetingId);


        if (!meeting) {
          callback?.({
            ok: false,
            error:
              "Meeting not found or has ended."
          });

          return;
        }


        // If already inside another meeting,
        // leave it first.
        if (
          socket.data.meetingId &&
          socket.data.meetingId !== meetingId
        ) {
          removeSocket(socket);
        }


        const participantName =
          safeName(
            data.name,
            "Participant"
          );


        // Existing users BEFORE adding new user.
        const existingParticipants =
          Array.from(
            meeting.participants.entries()
          )
            .filter(
              ([socketId]) =>
                socketId !== socket.id
            )
            .map(
              ([socketId, user]) => ({
                socketId,
                name: user.name,
                role: user.role
              })
            );


        // Add participant.
        meeting.participants.set(
          socket.id,
          {
            name: participantName,
            role: "Participant"
          }
        );


        socket.join(meetingId);

        socket.data.meetingId =
          meetingId;

        socket.data.name =
          participantName;

        socket.data.role =
          "Participant";


        console.log(
          `${participantName} joined ${meetingId}`
        );


        // Send meeting information
        // to the joining participant.
        callback?.({
          ok: true,
          meetingId,
          title: meeting.title,
          hostSocket:
            meeting.hostSocket,
          hostName:
            meeting.hostName,

          participants:
            existingParticipants,

          meeting:
            meetingInfo(meeting)
        });


        // Tell everyone ELSE that a new
        // participant arrived.
        socket
          .to(meetingId)
          .emit(
            "participant-joined",
            {
              socketId: socket.id,
              name: participantName,
              role: "Participant"
            }
          );


        // Updated participant count.
        io.to(meetingId).emit(
          "participants-updated",
          meetingInfo(meeting)
        );


        io.emit(
          "live-meetings-updated"
        );

      } catch (error) {

        console.error(
          "JOIN MEETING ERROR:",
          error
        );

        callback?.({
          ok: false,
          error: "Unable to join meeting."
        });
      }
    }
  );


  // ===================================================
  // WEBRTC OFFER
  // ===================================================

  socket.on(
    "offer",
    (data = {}) => {

      const meeting =
        getMeeting(socket);

      if (!meeting) {
        return;
      }


      const target =
        data.target;

      const offer =
        data.offer;


      if (!target || !offer) {
        return;
      }


      const targetSocket =
        io.sockets.sockets.get(
          target
        );


      if (!targetSocket) {
        return;
      }


      // Security: both sockets must be
      // inside the same meeting.
      if (
        targetSocket.data.meetingId !==
        socket.data.meetingId
      ) {
        return;
      }


      targetSocket.emit(
        "offer",
        {
          sender: socket.id,
          offer
        }
      );
    }
  );


  // ===================================================
  // WEBRTC ANSWER
  // ===================================================

  socket.on(
    "answer",
    (data = {}) => {

      const meeting =
        getMeeting(socket);

      if (!meeting) {
        return;
      }


      const target =
        data.target;

      const answer =
        data.answer;


      if (!target || !answer) {
        return;
      }


      const targetSocket =
        io.sockets.sockets.get(
          target
        );


      if (!targetSocket) {
        return;
      }


      if (
        targetSocket.data.meetingId !==
        socket.data.meetingId
      ) {
        return;
      }


      targetSocket.emit(
        "answer",
        {
          sender: socket.id,
          answer
        }
      );
    }
  );


  // ===================================================
  // WEBRTC ICE
  // ===================================================

  socket.on(
    "ice-candidate",
    (data = {}) => {

      const meeting =
        getMeeting(socket);

      if (!meeting) {
        return;
      }


      const target =
        data.target;

      const candidate =
        data.candidate;


      if (!target || !candidate) {
        return;
      }


      const targetSocket =
        io.sockets.sockets.get(
          target
        );


      if (!targetSocket) {
        return;
      }


      if (
        targetSocket.data.meetingId !==
        socket.data.meetingId
      ) {
        return;
      }


      targetSocket.emit(
        "ice-candidate",
        {
          sender: socket.id,
          candidate
        }
      );
    }
  );


  // ===================================================
  // CHAT
  // ===================================================

  socket.on(
    "chat-message",
    (data = {}) => {

      const meeting =
        getMeeting(socket);

      if (!meeting) {
        return;
      }


      const message =
        String(
          data.message || ""
        )
          .trim()
          .slice(0, 1000);


      if (!message) {
        return;
      }


      io.to(meeting.id).emit(
        "chat-message",
        {
          sender:
            socket.data.name ||
            "User",

          socketId:
            socket.id,

          message,

          time:
            Date.now()
        }
      );
    }
  );


  // ===================================================
  // REACTION
  // ===================================================

  socket.on(
    "reaction",
    (data = {}) => {

      const meeting =
        getMeeting(socket);

      if (!meeting) {
        return;
      }


      const allowed = [
        "👍",
        "👏",
        "❤️",
        "😂",
        "🎉",
        "🔥",
        "😮"
      ];


      const reaction =
        String(
          data.reaction || ""
        );


      if (
        !allowed.includes(
          reaction
        )
      ) {
        return;
      }


      io.to(meeting.id).emit(
        "reaction",
        {
          sender:
            socket.data.name ||
            "User",

          socketId:
            socket.id,

          reaction
        }
      );
    }
  );


  // ===================================================
  // LEAVE MEETING
  // ===================================================

  socket.on(
    "leave-meeting",
    () => {
      removeSocket(socket);
    }
  );


  // ===================================================
  // ADMIN ENDS MEETING
  // ===================================================

  socket.on(
    "end-meeting",
    () => {

      const meeting =
        getMeeting(socket);


      if (!meeting) {
        return;
      }


      // Only Admin/host can end it.
      if (
        meeting.hostSocket !==
        socket.id
      ) {
        return;
      }


      io.to(meeting.id).emit(
        "meeting-ended"
      );


      for (
        const [
          socketId
        ]
        of meeting.participants
      ) {

        const participant =
          io.sockets.sockets.get(
            socketId
          );


        if (participant) {

          participant.data.meetingId =
            null;

          participant.data.name =
            null;

          participant.data.role =
            null;

          participant.leave(
            meeting.id
          );
        }
      }


      console.log(
        `Meeting ended: ${meeting.id}`
      );


      meetings.delete(
        meeting.id
      );


      socket.data.meetingId =
        null;

      socket.data.name =
        null;

      socket.data.role =
        null;


      io.emit(
        "live-meetings-updated"
      );
    }
  );


  // ===================================================
  // DISCONNECT
  // ===================================================

  socket.on(
    "disconnect",
    (reason) => {

      console.log(
        "Socket disconnected:",
        socket.id,
        reason
      );

      removeSocket(socket);
    }
  );

});


// =====================================================
// SPA FALLBACK
// =====================================================

app.get("*", (req, res) => {
  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});


// =====================================================
// START SERVER
// =====================================================

server.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      `AICTE SecureMeet running on port ${PORT}`
    );

  }
);
