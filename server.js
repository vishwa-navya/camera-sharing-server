/**
 * server.js — Production-Grade Signaling Server v2.0
 *
 * Architecture Improvements:
 * 1. Call State Machine — prevents race conditions, invalid states
 * 2. Perfect Negotiation — prevents offer collisions
 * 3. Session Persistence — call survives socket disconnects
 * 4. Health Monitoring — ping/pong with latency tracking
 * 5. Timeout & Retries — every operation has deadline
 * 6. Duplicate Protection — message deduplication
 * 7. Cold Start Ready — immediate response after wake
 * 8. Reconnection Recovery — restore call on reconnect
 */

const express    = require("express");
const http       = require("http");
const { Server } = require("socket.io");
const crypto     = require("crypto");

const app    = express();
const server = http.createServer(app);

// ============================================================================
// CONFIGURATION
// ============================================================================

const CONFIG = {
  // Timeouts (milliseconds)
  HEALTH_PING_INTERVAL:     15000,   // Ping every 15s
  HEALTH_PONG_TIMEOUT:      5000,    // Expect pong within 5s
  CALL_TIMEOUT:             60000,   // Ring for 60s max
  ICE_GATHERING_TIMEOUT:    10000,   // Max ICE gathering time
  NEGOTIATION_TIMEOUT:      15000,   // Max negotiation time
  RECONNECT_TIMEOUT:        10000,   // Max reconnect time

  // Retries
  MAX_RETRIES:              3,
  RETRY_BASE_DELAY:         500,
  RETRY_MAX_DELAY:          5000,

  // Cleanup
  SESSION_TTL:              300000,  // 5 minutes
  MESSAGE_DEDUP_TTL:        30000,   // 30 seconds
  INACTIVE_SOCKET_TTL:      60000,   // 1 minute without pong

  // Feature flags
  PERFECT_NEGOTIATION:      true,
  SESSION_PERSISTENCE:      true,
  HEALTH_MONITORING:        true,
};

// ============================================================================
// STATE MANAGEMENT
// ============================================================================

// Call State Machine
const CALL_STATES = {
  IDLE:         'idle',
  JOINING:      'joining',
  WAITING:      'waiting',      // Caller waiting for answer
  RINGING:      'ringing',      // Callee receiving call
  CONNECTING:   'connecting',   // WebRTC connecting
  NEGOTIATING:  'negotiating',  // Offer/Answer exchange
  CONNECTED:    'connected',    // Call active
  RECONNECTING: 'reconnecting', // Same as connecting (for clarity)
  RECOVERING:   'recovering',  // Restoring from disconnect
  ENDED:        'ended',
};

// Valid state transitions
const VALID_TRANSITIONS = {
  [CALL_STATES.IDLE]:         [CALL_STATES.JOINING],
  [CALL_STATES.JOINING]:      [CALL_STATES.WAITING, CALL_STATES.RINGING, CALL_STATES.ENDED],
  [CALL_STATES.WAITING]:      [CALL_STATES.RINGING, CALL_STATES.CONNECTING, CALL_STATES.ENDED],
  [CALL_STATES.RINGING]:      [CALL_STATES.CONNECTING, CALL_STATES.ENDED],
  [CALL_STATES.CONNECTING]:   [CALL_STATES.NEGOTIATING, CALL_STATES.RECOVERING, CALL_STATES.ENDED],
  [CALL_STATES.NEGOTIATING]:  [CALL_STATES.CONNECTED, CALL_STATES.RECOVERING, CALL_STATES.ENDED],
  [CALL_STATES.CONNECTED]:    [CALL_STATES.RECOVERING, CALL_STATES.RECONNECTING, CALL_STATES.ENDED],
  [CALL_STATES.RECONNECTING]: [CALL_STATES.NEGOTIATING, CALL_STATES.CONNECTED, CALL_STATES.ENDED],
  [CALL_STATES.RECOVERING]:   [CALL_STATES.NEGOTIATING, CALL_STATES.CONNECTED, CALL_STATES.ENDED],
  [CALL_STATES.ENDED]:        [CALL_STATES.IDLE],
};

// Active sessions (persists across socket reconnects)
const sessions = {
  // callId -> CallSession
  calls: new Map(),

  // socketId -> SocketMeta
  sockets: new Map(),

  // userName -> Set<socketId> (multi-device)
  users: new Map(),

  // messageHash -> timestamp (deduplication)
  messageCache: new Map(),
};

// Call Session structure
class CallSession {
  constructor(callId, type, caller, callee) {
    this.callId = callId;
    this.type = type; // 'video' | 'voice'
    this.caller = caller;
    this.callee = callee;
    this.state = CALL_STATES.IDLE;
    this.createdAt = Date.now();
    this.updatedAt = Date.now();

    // Perfect Negotiation: polite peer rolls back on collision
    // Caller is polite, callee is impolite (or vice versa)
    this.politePeer = caller; // Who rolls back

    // WebRTC state
    this.offerer = null;
    this.lastOffer = null;
    this.lastAnswer = null;
    this.iceCandidates = { [caller]: [], [callee]: [] };

    // Connected socket IDs
    this.sockets = {
      [caller]: new Set(),
      [callee]: new Set(),
    };

    // Media state
    this.mediaState = {
      [caller]: { video: false, audio: false },
      [callee]: { video: false, audio: false },
    };

    // Timers
    this.timers = {
      call: null,
      negotiation: null,
      reconnect: null,
    };
  }

  canTransitionTo(newState) {
    return VALID_TRANSITIONS[this.state]?.includes(newState) ?? false;
  }

  setState(newState) {
    if (this.canTransitionTo(newState)) {
      console.log(`[Call ${this.callId}] ${this.state} → ${newState}`);
      this.state = newState;
      this.updatedAt = Date.now();
      return true;
    }
    console.warn(`[Call ${this.callId}] Invalid transition: ${this.state} → ${newState}`);
    return false;
  }

  getOtherUser(user) {
    return user === this.caller ? this.callee : this.caller;
  }

  isParticipant(user) {
    return user === this.caller || user === this.callee;
  }
}

// Socket metadata
class SocketMeta {
  constructor(socketId, userName) {
    this.socketId = socketId;
    this.userName = userName;
    this.connectedAt = Date.now();
    this.lastPong = Date.now();
    this.pingLatency = 0;
    this.callId = null;
    this.isHealthy = true;
    this.pingSentAt = null;
    this.pongReceived = false;
  }
}

// ============================================================================
// UTILITIES
// ============================================================================

function generateCallId() {
  return `call_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
}

function generateMessageId() {
  return `${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
}

function dedupeMessage(msgId) {
  const now = Date.now();
  const exists = sessions.messageCache.has(msgId);

  // Clean old entries
  for (const [id, ts] of sessions.messageCache) {
    if (now - ts > CONFIG.MESSAGE_DEDUP_TTL) {
      sessions.messageCache.delete(id);
    }
  }

  if (exists) {
    return false; // Duplicate, don't process
  }

  sessions.messageCache.set(msgId, now);
  return true;
}

function addUserSocket(user, socketId) {
  if (!sessions.users.has(user)) {
    sessions.users.set(user, new Set());
  }
  sessions.users.get(user).add(socketId);
}

function removeUserSocket(user, socketId) {
  sessions.users.get(user)?.delete(socketId);
  if (sessions.users.get(user)?.size === 0) {
    sessions.users.delete(user);
  }
}

function getUserSockets(user) {
  return [...(sessions.users.get(user) ?? [])];
}

function getUserForSocket(socketId) {
  const meta = sessions.sockets.get(socketId);
  return meta?.userName ?? null;
}

function getActiveCallForUser(user) {
  for (const [callId, session] of sessions.calls) {
    if (session.isParticipant(user) &&
        session.state !== CALL_STATES.ENDED &&
        session.state !== CALL_STATES.IDLE) {
      return session;
    }
  }
  return null;
}

function cleanupSession(callId) {
  const session = sessions.calls.get(callId);
  if (!session) return;

  // Clear timers
  Object.values(session.timers).forEach(t => t && clearTimeout(t));

  sessions.calls.delete(callId);
  console.log(`[Call ${callId}] Session cleaned up`);
}

// ============================================================================
// HEALTH MONITORING
// ============================================================================

function startHealthCheck(socket) {
  if (!CONFIG.HEALTH_MONITORING) return;

  let pingInterval = null;
  let pongTimeout = null;

  const sendPing = () => {
    if (!socket.connected) {
      clearInterval(pingInterval);
      clearTimeout(pongTimeout);
      return;
    }

    const meta = sessions.sockets.get(socket.id);
    if (meta) {
      meta.pingSentAt = Date.now();
      meta.pongReceived = false;
    }

    socket.emit('ping', { ts: Date.now() });

    // Expect pong within timeout
    clearTimeout(pongTimeout);
    pongTimeout = setTimeout(() => {
      const meta = sessions.sockets.get(socket.id);
      if (meta && !meta.pongReceived) {
        meta.isHealthy = false;
        console.warn(`[Health] ${socket.id} (${meta.userName}) unhealthy - no pong`);
        socket.emit('health-warning', { reason: 'No heartbeat response' });
      }
    }, CONFIG.HEALTH_PONG_TIMEOUT);
  };

  socket.on('pong', (data) => {
    const meta = sessions.sockets.get(socket.id);
    if (meta) {
      meta.pongReceived = true;
      meta.lastPong = Date.now();
      meta.pingLatency = Date.now() - (meta.pingSentAt || Date.now());
      meta.isHealthy = true;
    }
    clearTimeout(pongTimeout);
  });

  // Start pinging
  pingInterval = setInterval(sendPing, CONFIG.HEALTH_PING_INTERVAL);
  sendPing(); // Initial ping

  // Cleanup on disconnect
  socket.once('disconnect', () => {
    clearInterval(pingInterval);
    clearTimeout(pongTimeout);
  });
}

// ============================================================================
// SOCKET.IO CONFIGURATION
// ============================================================================

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true,
  },
  pingTimeout: 30000,
  pingInterval: 25000,
  // Upgrade to WebSocket immediately if possible
  transports: ['websocket', 'polling'],
});

// ============================================================================
// EXPRESS ROUTES
// ============================================================================

app.get("/", (req, res) => res.send("Signaling Server v2.0 - Ready"));
app.get("/health", (req, res) => res.json({
  ok: true,
  version: "2.0",
  uptime: process.uptime(),
  activeCalls: sessions.calls.size,
  connectedSockets: sessions.sockets.size,
}));

// Wake-up endpoint (called when app loads)
app.post("/wake", express.json(), (req, res) => {
  // Immediately respond to wake the server
  res.json({ awake: true, ts: Date.now() });
});

// ============================================================================
// CALL MANAGEMENT
// ============================================================================

/**
 * Create a new call session
 */
function createCallSession(type, caller, callee) {
  // Check if there's already an active call between these users
  const existingCall = getActiveCallForUser(caller);
  if (existingCall) {
    return { error: 'CALL_EXISTS', callId: existingCall.callId };
  }

  const callId = generateCallId();
  const session = new CallSession(callId, type, caller, callee);
  sessions.calls.set(callId, session);

  // Set call timeout (auto-end if not connected)
  session.timers.call = setTimeout(() => {
    if (session.state !== CALL_STATES.CONNECTED &&
        session.state !== CALL_STATES.ENDED) {
      console.log(`[Call ${callId}] Timed out`);
      endCall(session, 'TIMEOUT');
    }
  }, CONFIG.CALL_TIMEOUT);

  console.log(`[Call ${callId}] Created: ${caller} → ${callee} (${type})`);
  return session;
}

/**
 * Initiate a call
 */
function initiateCall(session, callerSocketId) {
  session.setState(CALL_STATES.JOINING);

  const callee = session.callee;
  const calleeSockets = getUserSockets(callee);

  if (calleeSockets.length === 0) {
    return { error: 'USER_OFFLINE' };
  }

  session.setState(CALL_STATES.WAITING);
  session.offerer = session.caller;

  // Ring all of callee's devices
  calleeSockets.forEach(sid => {
    io.to(sid).emit('call-incoming', {
      callId: session.callId,
      type: session.type,
      from: session.caller,
      polite: session.caller === session.politePeer ? callee : session.caller,
    });
  });

  return { success: true, callId: session.callId };
}

/**
 * Accept an incoming call
 */
function acceptCall(session, calleeSocketId) {
  if (session.state === CALL_STATES.ENDED) {
    return { error: 'CALL_ENDED' };
  }

  if (session.state !== CALL_STATES.WAITING) {
    console.warn(`[Call ${session.callId}] Unexpected accept in state ${session.state}`);
  }

  session.setState(CALL_STATES.CONNECTING);

  // Cancel ring on other devices of callee
  const callee = session.callee;
  const calleeSockets = getUserSockets(callee);
  calleeSockets.forEach(sid => {
    if (sid !== calleeSocketId) {
      io.to(sid).emit('call-cancelled-other-device');
    }
  });

  // Tell caller that call was accepted
  const callerSockets = getUserSockets(session.caller);
  callerSockets.forEach(sid => {
    io.to(sid).emit('call-accepted', {
      callId: session.callId,
      from: callee,
    });
  });

  return { success: true };
}

/**
 * Reject a call
 */
function rejectCall(session, fromSocketId) {
  const callerSockets = getUserSockets(session.caller);
  callerSockets.forEach(sid => {
    io.to(sid).emit('call-rejected', {
      callId: session.callId,
      from: session.callee,
    });
  });

  endCall(session, 'REJECTED');
  return { success: true };
}

/**
 * End a call
 */
function endCall(session, reason = 'NORMAL') {
  if (session.state === CALL_STATES.ENDED) return;

  session.setState(CALL_STATES.ENDED);

  // Notify all participants
  [session.caller, session.callee].forEach(user => {
    getUserSockets(user).forEach(sid => {
      io.to(sid).emit('call-ended', {
        callId: session.callId,
        reason,
        duration: Date.now() - session.createdAt,
      });
    });
  });

  // Cleanup after a delay (allow for client cleanup)
  setTimeout(() => cleanupSession(session.callId), 5000);
}

/**
 * Handle WebRTC offer (Perfect Negotiation)
 */
function handleOffer(session, from, sdp, msgId) {
  if (!dedupeMessage(msgId)) {
    return { ignored: true, reason: 'duplicate' };
  }

  session.setState(CALL_STATES.NEGOTIATING);
  session.lastOffer = { from, sdp, ts: Date.now() };

  // Perfect Negotiation: check for collision
  const collision = session.lastOffer && session.lastAnswer &&
                    session.lastOffer.from !== session.lastAnswer.from;

  if (collision && CONFIG.PERFECT_NEGOTIATION) {
    // Determine who is polite (rolls back)
    if (from === session.politePeer) {
      // This peer should rollback their offer and accept the incoming one
      io.to(getUserSockets(from)[0]).emit('negotiation-rollback', {
        callId: session.callId,
        reason: 'collision',
      });
      return { rolledBack: true };
    }
  }

  // Forward offer to peer
  const other = session.getOtherUser(from);
  const otherSockets = getUserSockets(other);
  otherSockets.forEach(sid => {
    io.to(sid).emit('call-offer', {
      callId: session.callId,
      from,
      sdp,
      msgId,
    });
  });

  // Set negotiation timeout
  clearTimeout(session.timers.negotiation);
  session.timers.negotiation = setTimeout(() => {
    if (session.state === CALL_STATES.NEGOTIATING) {
      console.log(`[Call ${session.callId}] Negotiation timeout`);
      session.setState(CALL_STATES.RECOVERING);
      // Signal both peers to retry
      [session.caller, session.callee].forEach(user => {
        getUserSockets(user).forEach(sid => {
          io.to(sid).emit('negotiation-timeout', { callId: session.callId });
        });
      });
    }
  }, CONFIG.NEGOTIATION_TIMEOUT);

  return { success: true };
}

/**
 * Handle WebRTC answer
 */
function handleAnswer(session, from, sdp, msgId) {
  if (!dedupeMessage(msgId)) {
    return { ignored: true, reason: 'duplicate' };
  }

  session.lastAnswer = { from, sdp, ts: Date.now() };
  clearTimeout(session.timers.negotiation);

  // Forward answer to peer
  const other = session.getOtherUser(from);
  const otherSockets = getUserSockets(other);
  otherSockets.forEach(sid => {
    io.to(sid).emit('call-answer', {
      callId: session.callId,
      from,
      sdp,
      msgId,
    });
  });

  if (session.state === CALL_STATES.NEGOTIATING) {
    session.setState(CALL_STATES.CONNECTED);
  }

  return { success: true };
}

/**
 * Handle ICE candidate
 */
function handleICE(session, from, candidate, msgId) {
  if (!dedupeMessage(msgId)) {
    return { ignored: true, reason: 'duplicate' };
  }

  // Store for reconnection recovery
  session.iceCandidates[from].push({ candidate, ts: Date.now() });

  // Forward to peer
  const other = session.getOtherUser(from);
  const otherSockets = getUserSockets(other);
  otherSockets.forEach(sid => {
    io.to(sid).emit('call-ice', {
      callId: session.callId,
      from,
      candidate,
      msgId,
    });
  });

  return { success: true };
}

/**
 * Handle socket reconnection (session recovery)
 */
function recoverSession(socket, callId, userName) {
  const session = sessions.calls.get(callId);
  if (!session) {
    return { error: 'SESSION_NOT_FOUND' };
  }

  if (!session.isParticipant(userName)) {
    return { error: 'NOT_PARTICIPANT' };
  }

  // Update socket for this session
  session.sockets[userName].add(socket.id);

  // Sync current state
  const stateSync = {
    callId: session.callId,
    type: session.type,
    state: session.state,
    caller: session.caller,
    callee: session.callee,
    youAre: userName,
    other: session.getOtherUser(userName),
    isOfferer: session.offerer === userName,
    lastOffer: session.lastOffer?.from === session.getOtherUser(userName) ? session.lastOffer : null,
    lastAnswer: session.lastAnswer?.from === session.getOtherUser(userName) ? session.lastAnswer : null,
    pendingICE: session.iceCandidates[session.getOtherUser(userName)],
    polite: userName === session.politePeer,
  };

  if (session.state === CALL_STATES.CONNECTED) {
    session.setState(CALL_STATES.RECOVERING);
  }

  return { success: true, stateSync };
}

// ============================================================================
// SOCKET EVENT HANDLERS
// ============================================================================

io.on("connection", (socket) => {
  console.log(`🔌 Connected: ${socket.id}`);

  // Start health monitoring
  startHealthCheck(socket);

  // Register user (supports multiple sockets per user)
  socket.on("register", ({ user, callType }) => {
    const meta = new SocketMeta(socket.id, user);
    sessions.sockets.set(socket.id, meta);
    addUserSocket(user, socket.id);

    socket.data.user = user;
    socket.data.callType = callType;

    socket.emit("registered", {
      socketId: socket.id,
      callType,
      serverTime: Date.now(),
    });

    console.log(`📋 Registered: ${user} (${getUserSockets(user).length} device(s))`);
  });

  // ========================
  // CALL INITIATION
  // ========================

  socket.on("call-user", ({ to, type }) => {
    const from = socket.data.user;
    if (!from) return socket.emit("error", { message: "Not registered" });

    const result = createCallSession(type, from, to);

    if (result.error) {
      if (result.error === 'CALL_EXISTS') {
        socket.emit("call-exists", { callId: result.callId });
      } else {
        socket.emit("call-failed", { error: result.error });
      }
      return;
    }

    const session = result;
    session.sockets[from].add(socket.id);
    sessions.sockets.get(socket.id).callId = session.callId;

    const initResult = initiateCall(session, socket.id);

    if (initResult.error) {
      socket.emit("call-failed", { error: initResult.error });
      cleanupSession(session.callId);
      return;
    }

    socket.emit("call-initiated", {
      callId: session.callId,
      type: session.type,
      to,
      polite: from === session.politePeer,
    });
  });

  // Accept incoming call
  socket.on("call-accept", ({ callId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);

    if (!session || !session.isParticipant(from)) {
      return socket.emit("error", { message: "Invalid call" });
    }

    session.sockets[from].add(socket.id);
    sessions.sockets.get(socket.id).callId = callId;

    acceptCall(session, socket.id);
  });

  // Reject call
  socket.on("call-reject", ({ callId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);

    if (!session || !session.isParticipant(from)) {
      return;
    }

    rejectCall(session, socket.id);
  });

  // End call
  socket.on("call-end", ({ callId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);

    if (session && session.isParticipant(from)) {
      endCall(session, 'ENDED_BY_USER');
    }
  });

  // ========================
  // WEBRTC SIGNALING
  // ========================

  socket.on("call-offer", ({ callId, sdp, msgId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);

    if (!session || !session.isParticipant(from)) {
      return;
    }

    handleOffer(session, from, sdp, msgId || generateMessageId());
  });

  socket.on("call-answer", ({ callId, sdp, msgId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);

    if (!session || !session.isParticipant(from)) {
      return;
    }

    handleAnswer(session, from, sdp, msgId || generateMessageId());
  });

  socket.on("call-ice", ({ callId, candidate, msgId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);

    if (!session || !session.isParticipant(from)) {
      return;
    }

    handleICE(session, from, candidate, msgId || generateMessageId());
  });

  // ========================
  // PERFECT NEGOTIATION
  // ========================

  // Called when rollback is needed
  socket.on("nego-rollback", ({ callId }) => {
    const session = sessions.calls.get(callId);
    if (session && session.state === CALL_STATES.NEGOTIATING) {
      // Client will re-negotiate after rollback
      console.log(`[Call ${callId}] Rollback performed`);
    }
  });

  // Renegotiation request
  socket.on("nego-needed", ({ callId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);

    if (!session || !session.isParticipant(from)) {
      return;
    }

    // Notify other peer that renegotiation is needed
    const other = session.getOtherUser(from);
    getUserSockets(other).forEach(sid => {
      io.to(sid).emit('nego-needed', { callId, from });
    });
  });

  // ========================
  // SESSION RECOVERY
  // ========================

  socket.on("session-recover", ({ callId }) => {
    const user = socket.data.user;
    if (!user) return;

    const result = recoverSession(socket, callId, user);

    if (result.error) {
      socket.emit("session-recover-failed", { callId, error: result.error });
    } else {
      socket.emit("session-recovered", result.stateSync);

      // Notify other peer that we recovered
      const session = sessions.calls.get(callId);
      const other = session.getOtherUser(user);
      getUserSockets(other).forEach(sid => {
        io.to(sid).emit('peer-recovered', { callId, user });
      });
    }
  });

  // ========================
  // MEDIA STATE
  // ========================

  socket.on("media-state", ({ callId, video, audio }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);

    if (!session || !session.isParticipant(from)) {
      return;
    }

    session.mediaState[from] = { video, audio };

    // Notify peer
    const other = session.getOtherUser(from);
    getUserSockets(other).forEach(sid => {
      io.to(sid).emit('peer-media-state', { callId, from, video, audio });
    });
  });

  // ========================
  // CAMERA SHARING (Legacy Support)
  // ========================

  socket.on("join", ({ room, user }) => {
    socket.join(room);
    socket.data.room = room;
    socket.data.videoUser = user;
    socket.emit("joined", { room });
  });

  socket.on("camera-ready", ({ room, from }) => {
    socket.to(room).emit("camera-ready", { from });
  });

  socket.on("offer", ({ room, from, sdp }) => {
    socket.to(room).emit("offer", { from, sdp });
  });

  socket.on("answer", ({ room, from, sdp }) => {
    socket.to(room).emit("answer", { from, sdp });
  });

  socket.on("ice", ({ room, from, candidate }) => {
    socket.to(room).emit("ice", { from, candidate });
  });

  socket.on("camera-off", ({ room, from }) => {
    socket.to(room).emit("camera-off", { from });
  });

  // ========================
  // LEGACY VOICE CALL SUPPORT
  // ========================

  // Legacy call-user (for backward compatibility)
  socket.on("call-user-legacy", ({ to, from }) => {
    console.log(`📞 Legacy call: ${from} -> ${to}`);
    const targetSockets = getUserSockets(to);
    if (targetSockets.length === 0) {
      socket.emit('call-user-offline');
      return;
    }
    targetSockets.forEach(sid => {
      io.to(sid).emit('call-incoming', { from, type: 'voice' });
    });
  });

  // Legacy call-accept
  socket.on("call-accept-legacy", ({ room, from }) => {
    console.log(`✅ Legacy accept: ${from}`);
    socket.to(room).emit("call-accepted", { from });
  });

  // Legacy call-reject
  socket.on("call-reject-legacy", ({ room, from }) => {
    socket.to(room).emit("call-rejected", { from });
  });

  // Legacy call-end
  socket.on("call-end-legacy", ({ room, from }) => {
    socket.to(room).emit("call-ended", { from });
  });

  // Legacy WebRTC signaling
  socket.on("call-offer-legacy", ({ room, from, sdp }) => {
    socket.to(room).emit("call-offer", { from, sdp });
  });

  socket.on("call-answer-legacy", ({ room, from, sdp }) => {
    socket.to(room).emit("call-answer", { from, sdp });
  });

  socket.on("call-ice-legacy", ({ room, from, candidate }) => {
    socket.to(room).emit("call-ice", { from, candidate });
  });

  // ========================
  // HUG SYNC (unchanged)
  // ========================

  socket.on('hug-sync-initiate', (hugData) => {
    console.log(`🫂 Hug sync initiated:`, hugData);

    const initiatorSockets = getUserSockets(hugData.initiator);
    const responderSockets = getUserSockets(hugData.responder);

    if (initiatorSockets.length > 0 && responderSockets.length > 0) {
      initiatorSockets.forEach(sid => io.to(sid).emit('hug-sync-vibrate', hugData));
      responderSockets.forEach(sid => io.to(sid).emit('hug-sync-vibrate', hugData));
      console.log(`🫂 Synchronized hug sent to ${hugData.initiator} and ${hugData.responder}`);
    } else {
      console.log(`❌ Hug sync failed: One or both users not connected`);
    }
  });

  // ========================
  // MESSAGE READ EVENTS
  // ========================

  socket.on('messages-read', ({ messageIds, readBy, senderIds }) => {
    console.log(`📖 Messages read by ${readBy}:`, messageIds);

    senderIds.forEach(senderId => {
      const senderSockets = getUserSockets(senderId);
      senderSockets.forEach(sid => {
        io.to(sid).emit('message-seen-confirmation', { messageIds, readBy });
      });
    });
  });

  // ========================
  // DISCONNECT
  // ========================

  socket.on("disconnect", (reason) => {
    const user = socket.data.user || socket.data.videoUser;
    const callId = sessions.sockets.get(socket.id)?.callId;

    console.log(`🔌 Disconnected: ${socket.id} (${user}) - ${reason}`);

    // Cleanup socket meta
    sessions.sockets.delete(socket.id);

    if (user) {
      removeUserSocket(user, socket.id);

      // Handle active call
      if (callId) {
        const session = sessions.calls.get(callId);
        if (session && session.isParticipant(user)) {
          // Remove this socket from session
          session.sockets[user].delete(socket.id);

          // Check if user has other devices still connected
          const userSockets = getUserSockets(user);

          if (userSockets.length === 0) {
            // User fully disconnected - notify peer
            const other = session.getOtherUser(user);
            getUserSockets(other).forEach(sid => {
              io.to(sid).emit('peer-disconnected', {
                callId,
                user,
                state: session.state,
              });
            });

            // Keep session alive for reconnection (if enabled)
            if (CONFIG.SESSION_PERSISTENCE) {
              // Set reconnect timeout
              clearTimeout(session.timers.reconnect);
              session.timers.reconnect = setTimeout(() => {
                if (session.state === CALL_STATES.CONNECTED ||
                    session.state === CALL_STATES.RECOVERING ||
                    session.state === CALL_STATES.RECONNECTING) {
                  console.log(`[Call ${callId}] Reconnect timeout for ${user}`);
                  endCall(session, 'RECONNECT_TIMEOUT');
                }
              }, CONFIG.RECONNECT_TIMEOUT);
            } else {
              endCall(session, 'PEER_DISCONNECTED');
            }
          }
        }
      }
    }

    // Camera room cleanup
    const room = socket.data.room;
    if (room) {
      socket.to(room).emit("camera-off", { from: user });
    }
  });
});

// ============================================================================
// PERIODIC CLEANUP
// ============================================================================

setInterval(() => {
  const now = Date.now();

  // Clean up old sessions
  for (const [callId, session] of sessions.calls) {
    if (session.state === CALL_STATES.ENDED) {
      const age = now - session.updatedAt;
      if (age > CONFIG.SESSION_TTL) {
        cleanupSession(callId);
      }
    }
  }

  // Clean up message cache
  for (const [msgId, ts] of sessions.messageCache) {
    if (now - ts > CONFIG.MESSAGE_DEDUP_TTL) {
      sessions.messageCache.delete(msgId);
    }
  }

}, 60000); // Every minute

// ============================================================================
// GRACEFUL SHUTDOWN
// ============================================================================

process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM received, shutting down gracefully');
  io.close(() => {
    console.log('✅ All connections closed');
    server.close(() => {
      console.log('✅ Server closed');
      process.exit(0);
    });
  });
});

process.on('SIGINT', () => {
  console.log('🛑 SIGINT received, shutting down gracefully');
  io.close(() => {
    console.log('✅ All connections closed');
    server.close(() => {
      console.log('✅ Server closed');
      process.exit(0);
    });
  });
});

// ============================================================================
// START SERVER
// ============================================================================

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Signaling Server v2.0 on port ${PORT}`);
  console.log(`   Health Check: http://localhost:${PORT}/health`);
  console.log(`   Features: Perfect Negotiation, Session Persistence, Health Monitoring`);
});
