/**
 * server.js — Production-Grade Signaling Server v3.1
 *
 * Architecture:
 * 1. Call State Machine — prevents race conditions, invalid states
 * 2. Perfect Negotiation — prevents offer collisions
 * 3. Session Persistence — call survives socket disconnects
 * 4. Health Monitoring — ping/pong with latency tracking
 * 5. Timeout & Retries — every operation has deadline
 * 6. Duplicate Protection — message deduplication
 * 7. Cold Start Ready — immediate response after wake
 * 8. Reconnection Recovery — restore call on reconnect
 * 9. Rate Limiting — prevent abuse
 * 10. Call History — persist call records
 * 11. Typing via Socket.IO — zero latency
 * 12. Mood Cleanup — server-side expiry
 * 13. FCM Push Notifications — server-side sending
 * 14. Screen Sharing — NEW: full desktop/window sharing with audio
 *
 * Note: Uses Node.js built-in fetch (Node 18+) — no node-fetch package needed
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
  HEALTH_PING_INTERVAL:     15000,
  HEALTH_PONG_TIMEOUT:      5000,
  CALL_TIMEOUT:             60000,
  ICE_GATHERING_TIMEOUT:    10000,
  NEGOTIATION_TIMEOUT:      15000,
  RECONNECT_TIMEOUT:        10000,

  MAX_RETRIES:              3,
  RETRY_BASE_DELAY:         500,
  RETRY_MAX_DELAY:          5000,

  SESSION_TTL:              300000,
  MESSAGE_DEDUP_TTL:        30000,
  INACTIVE_SOCKET_TTL:      60000,

  PERFECT_NEGOTIATION:      true,
  SESSION_PERSISTENCE:      true,
  HEALTH_MONITORING:        true,

  RATE_LIMITS: {
    MESSAGES:       60,
    TYPING:         120,
    CALLS:          10,
    MEDIA_UPLOADS:  20,
    SCREEN_SHARE:   10,
    GENERAL:        200,
  },

  FIREBASE_PROJECT_ID: 'lastseen-8800e',
};

// ============================================================================
// RATE LIMITING
// ============================================================================

const rateLimits = new Map();

function checkRateLimit(userId, eventType = 'GENERAL') {
  const now = Date.now();
  const limit = CONFIG.RATE_LIMITS[eventType] || CONFIG.RATE_LIMITS.GENERAL;
  const windowMs = 60000;

  if (!rateLimits.has(userId)) rateLimits.set(userId, {});
  const userLimits = rateLimits.get(userId);
  if (!userLimits[eventType]) userLimits[eventType] = { events: [], blockedUntil: 0 };

  const limitData = userLimits[eventType];

  if (limitData.blockedUntil > now) {
    return { allowed: false, retryAfter: limitData.blockedUntil - now };
  }

  limitData.events = limitData.events.filter(ts => now - ts < windowMs);

  if (limitData.events.length >= limit) {
    limitData.blockedUntil = now + 30000;
    console.warn(`[RateLimit] ${userId} blocked for ${eventType}`);
    return { allowed: false, retryAfter: 30000 };
  }

  limitData.events.push(now);
  return { allowed: true };
}

setInterval(() => {
  const now = Date.now();
  for (const [userId, limits] of rateLimits) {
    for (const [type, data] of Object.entries(limits)) {
      data.events = data.events.filter(ts => now - ts < 60000);
      if (data.events.length === 0 && data.blockedUntil < now) delete limits[type];
    }
    if (Object.keys(limits).length === 0) rateLimits.delete(userId);
  }
}, 300000);

// ============================================================================
// STATE MANAGEMENT
// ============================================================================

const CALL_STATES = {
  IDLE:         'idle',
  JOINING:      'joining',
  WAITING:      'waiting',
  RINGING:      'ringing',
  CONNECTING:   'connecting',
  NEGOTIATING:  'negotiating',
  CONNECTED:    'connected',
  RECONNECTING: 'reconnecting',
  RECOVERING:   'recovering',
  ENDED:        'ended',
};

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

const sessions = {
  calls:        new Map(),
  sockets:      new Map(),
  users:        new Map(),
  messageCache: new Map(),
  callHistory:  [],
  fcmTokens:    new Map(),
};

// NEW: Screen share room state (separate from camera rooms)
const shareRooms = {}; // { roomId: { socketId: userName } }

// Camera sharing room state (existing)
const cameraRooms = {}; // { roomId: { socketId: userName } }

class CallSession {
  constructor(callId, type, caller, callee) {
    this.callId       = callId;
    this.type         = type;
    this.caller       = caller;
    this.callee       = callee;
    this.state        = CALL_STATES.IDLE;
    this.createdAt    = Date.now();
    this.updatedAt    = Date.now();
    this.connectedAt  = null;
    this.endedAt       = null;
    this.endReason    = null;
    this.politePeer   = caller;
    this.offerer      = null;
    this.lastOffer    = null;
    this.lastAnswer   = null;
    this.iceCandidates = { [caller]: [], [callee]: [] };
    this.sockets      = { [caller]: new Set(), [callee]: new Set() };
    this.mediaState   = {
      [caller]: { video: false, audio: false },
      [callee]: { video: false, audio: false },
    };
    this.timers = { call: null, negotiation: null, reconnect: null };
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

  getOtherUser(user) { return user === this.caller ? this.callee : this.caller; }
  isParticipant(user) { return user === this.caller || user === this.callee; }
}

class SocketMeta {
  constructor(socketId, userName) {
    this.socketId      = socketId;
    this.userName      = userName;
    this.connectedAt   = Date.now();
    this.lastPong      = Date.now();
    this.pingLatency   = 0;
    this.callId        = null;
    this.isHealthy     = true;
    this.pingSentAt    = null;
    this.pongReceived  = false;
    this.lastActivity  = Date.now();
  }
}

// ============================================================================
// FIREBASE HELPERS
// ============================================================================

async function writeToFirestore(collection, docId, data) {
  try {
    const url = `https://firestore.googleapis.com/v1/projects/${CONFIG.FIREBASE_PROJECT_ID}/databases/(default)/documents/${collection}/${docId}`;
    const fields = {};
    for (const [key, value] of Object.entries(data)) {
      if (value === null || value === undefined) continue;
      if (typeof value === 'string')       fields[key] = { stringValue: value };
      else if (typeof value === 'number')  fields[key] = { integerValue: value.toString() };
      else if (typeof value === 'boolean') fields[key] = { booleanValue: value };
      else if (value instanceof Date)      fields[key] = { timestampValue: value.toISOString() };
      else if (typeof value === 'object') {
        if (key === 'expiresAt' && value && value.toDate) {
          fields[key] = { timestampValue: value.toDate().toISOString() };
        }
      }
    }
    await fetch(url, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ fields }),
    });
  } catch (err) {
    console.error('[Firestore] Write error:', err.message);
  }
}

async function deleteFromFirestore(collection, docId) {
  try {
    const url = `https://firestore.googleapis.com/v1/projects/${CONFIG.FIREBASE_PROJECT_ID}/databases/(default)/documents/${collection}/${docId}`;
    await fetch(url, { method: 'DELETE' });
    console.log(`[Firestore] Deleted ${collection}/${docId}`);
  } catch (err) {
    console.error('[Firestore] Delete error:', err.message);
  }
}

async function sendFCMNotification(fcmToken, title, body, data = {}) {
  if (!fcmToken) { console.warn('[FCM] No token provided'); return false; }
  try {
    const serverKey = process.env.FCM_SERVER_KEY;
    if (!serverKey) { console.warn('[FCM] No server key configured'); return false; }

    const response = await fetch('https://fcm.googleapis.com/fcm/send', {
      method: 'POST',
      headers: { 'Authorization': `key=${serverKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ to: fcmToken, notification: { title, body }, data, priority: 'high' }),
    });
    const result = await response.json();
    console.log('[FCM] Notification sent:', result.success);
    return result.success === 1;
  } catch (err) {
    console.error('[FCM] Send error:', err.message);
    return false;
  }
}

// ============================================================================
// UTILITIES
// ============================================================================

function generateCallId()    { return `call_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`; }
function generateMessageId() { return `${Date.now()}_${crypto.randomBytes(3).toString('hex')}`; }

function broadcastPresence(user, isOnline) {
  const knownUsers = ["Vishwa", "Ammu"];
  const otherUser = knownUsers.find(u => u !== user);
  if (otherUser) {
    getUserSockets(otherUser).forEach(sid => {
      io.to(sid).emit("presence-update", { user, isOnline, timestamp: Date.now() });
    });
    console.log(`[Presence] ${user} is ${isOnline ? "ONLINE" : "OFFLINE"} → notified ${otherUser}`);
  }
}

function dedupeMessage(msgId) {
  const now = Date.now();
  const exists = sessions.messageCache.has(msgId);
  for (const [id, ts] of sessions.messageCache) {
    if (now - ts > CONFIG.MESSAGE_DEDUP_TTL) sessions.messageCache.delete(id);
  }
  if (exists) return false;
  sessions.messageCache.set(msgId, now);
  return true;
}

function addUserSocket(user, socketId) {
  if (!sessions.users.has(user)) sessions.users.set(user, new Set());
  sessions.users.get(user).add(socketId);
}

function removeUserSocket(user, socketId) {
  sessions.users.get(user)?.delete(socketId);
  if (sessions.users.get(user)?.size === 0) sessions.users.delete(user);
}

function getUserSockets(user) { return [...(sessions.users.get(user) ?? [])]; }

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
  Object.values(session.timers).forEach(t => t && clearTimeout(t));
  sessions.calls.delete(callId);
  console.log(`[Call ${callId}] Session cleaned up`);
}

// ============================================================================
// CALL HISTORY
// ============================================================================

function saveCallToHistory(session, endReason) {
  const callRecord = {
    callId:      session.callId,
    type:        session.type,
    caller:      session.caller,
    callee:      session.callee,
    createdAt:   session.createdAt,
    connectedAt: session.connectedAt,
    endedAt:     Date.now(),
    duration:    session.connectedAt ? Date.now() - session.connectedAt : 0,
    endReason,
    didConnect:  !!session.connectedAt,
  };
  sessions.callHistory.push(callRecord);
  if (sessions.callHistory.length > 100) sessions.callHistory = sessions.callHistory.slice(-100);
  writeToFirestore('callHistory', session.callId, callRecord);
  console.log(`[CallHistory] Saved: ${session.caller} → ${session.callee} (${callRecord.duration}ms, ${endReason})`);
  return callRecord;
}

// ============================================================================
// HEALTH MONITORING
// ============================================================================

function startHealthCheck(socket) {
  if (!CONFIG.HEALTH_MONITORING) return;
  let pingInterval = null;
  let pongTimeout  = null;

  const sendPing = () => {
    if (!socket.connected) { clearInterval(pingInterval); clearTimeout(pongTimeout); return; }
    const meta = sessions.sockets.get(socket.id);
    if (meta) { meta.pingSentAt = Date.now(); meta.pongReceived = false; }
    socket.emit('ping', { ts: Date.now() });
    clearTimeout(pongTimeout);
    pongTimeout = setTimeout(() => {
      const meta = sessions.sockets.get(socket.id);
      if (meta && !meta.pongReceived) {
        meta.isHealthy = false;
        console.warn(`[Health] ${socket.id} (${meta.userName}) unhealthy`);
        socket.emit('health-warning', { reason: 'No heartbeat response' });
      }
    }, CONFIG.HEALTH_PONG_TIMEOUT);
  };

  socket.on('pong', () => {
    const meta = sessions.sockets.get(socket.id);
    if (meta) {
      meta.pongReceived = true;
      meta.lastPong = Date.now();
      meta.pingLatency = Date.now() - (meta.pingSentAt || Date.now());
      meta.isHealthy = true;
    }
    clearTimeout(pongTimeout);
  });

  pingInterval = setInterval(sendPing, CONFIG.HEALTH_PING_INTERVAL);
  sendPing();

  socket.once('disconnect', () => { clearInterval(pingInterval); clearTimeout(pongTimeout); });
}

// ============================================================================
// MOOD CLEANUP (Periodic)
// ============================================================================

async function cleanupExpiredMoods() {
  console.log('[MoodCleanup] Checking for expired moods...');
  const now = Date.now();
  const chatId = 'privateMessages';
  const users = ['Vishwa', 'Ammu'];

  for (const user of users) {
    try {
      const docId = `${chatId}_${user}`;
      const url = `https://firestore.googleapis.com/v1/projects/${CONFIG.FIREBASE_PROJECT_ID}/databases/(default)/documents/moods/${docId}`;
      const response = await fetch(url);
      const data = await response.json();
      if (data.fields?.expiresAt) {
        let expiresAt;
        if (data.fields.expiresAt.timestampValue) {
          expiresAt = new Date(data.fields.expiresAt.timestampValue).getTime();
        }
        if (expiresAt && expiresAt < now) {
          await deleteFromFirestore('moods', docId);
          console.log(`[MoodCleanup] Deleted expired mood for ${user}`);
        }
      }
    } catch (err) {
      // Document might not exist, that's fine
    }
  }
}

setInterval(cleanupExpiredMoods, 300000);
console.log('[MoodCleanup] Scheduled every 5 minutes');

// ============================================================================
// SOCKET.IO CONFIGURATION
// ============================================================================

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"], credentials: true },
  pingTimeout:  30000,
  pingInterval: 25000,
  transports: ['websocket', 'polling'],
});

// ============================================================================
// EXPRESS ROUTES
// ============================================================================

app.get("/", (req, res) => res.send("Signaling Server v3.1 - Ready"));

app.get("/health", (req, res) => res.json({
  ok: true,
  version: "3.1",
  uptime: process.uptime(),
  activeCalls: sessions.calls.size,
  connectedSockets: sessions.sockets.size,
  callHistoryCount: sessions.callHistory.length,
  activeScreenShares: Object.keys(shareRooms).length,
}));

app.post("/wake", express.json(), (req, res) => {
  res.json({ awake: true, ts: Date.now() });
  console.log('[Wake] Server woke up');
});

app.get("/call-history/:user", async (req, res) => {
  const { user } = req.params;
  const userCalls = sessions.callHistory.filter(c => c.caller === user || c.callee === user);
  res.json({ calls: userCalls.slice(-50) });
});

app.post("/fcm-token", express.json(), (req, res) => {
  const { user, token } = req.body;
  if (user && token) {
    sessions.fcmTokens.set(user, token);
    console.log(`[FCM] Token registered for ${user}`);
    res.json({ success: true });
  } else {
    res.status(400).json({ error: 'Missing user or token' });
  }
});

app.post("/notify", express.json(), async (req, res) => {
  const { user, title, body, data } = req.body;
  const token = sessions.fcmTokens.get(user);
  if (!token) return res.status(404).json({ error: 'User token not found' });
  const sent = await sendFCMNotification(token, title, body, data);
  res.json({ sent });
});

// Self-ping every 9 minutes to prevent Render free tier sleep
const SELF_URL = process.env.RENDER_EXTERNAL_URL || "https://camera-sharing-server.onrender.com";
setInterval(async () => {
  try {
    await fetch(`${SELF_URL}/health`, { signal: AbortSignal.timeout(8000) });
    console.log("💓 Self-ping OK");
  } catch (e) {
    console.log("⚠️ Self-ping failed:", e.message);
  }
}, 9 * 60 * 1000);

// ============================================================================
// CALL MANAGEMENT
// ============================================================================

function createCallSession(type, caller, callee) {
  const existingCall = getActiveCallForUser(caller);
  if (existingCall) return { error: 'CALL_EXISTS', callId: existingCall.callId };

  const callId = generateCallId();
  const session = new CallSession(callId, type, caller, callee);
  sessions.calls.set(callId, session);

  session.timers.call = setTimeout(() => {
    if (session.state !== CALL_STATES.CONNECTED && session.state !== CALL_STATES.ENDED) {
      console.log(`[Call ${callId}] Timed out`);
      endCall(session, 'TIMEOUT');
    }
  }, CONFIG.CALL_TIMEOUT);

  console.log(`[Call ${callId}] Created: ${caller} → ${callee} (${type})`);
  return session;
}

function initiateCall(session) {
  session.setState(CALL_STATES.JOINING);
  const calleeSockets = getUserSockets(session.callee);
  if (calleeSockets.length === 0) return { error: 'USER_OFFLINE' };

  session.setState(CALL_STATES.WAITING);
  session.offerer = session.caller;

  calleeSockets.forEach(sid => {
    io.to(sid).emit('call-incoming', {
      callId: session.callId,
      type: session.type,
      from: session.caller,
      polite: session.caller === session.politePeer ? session.callee : session.caller,
    });
  });

  return { success: true, callId: session.callId };
}

function acceptCall(session, calleeSocketId) {
  if (session.state === CALL_STATES.ENDED) return { error: 'CALL_ENDED' };
  if (session.state !== CALL_STATES.WAITING) {
    console.warn(`[Call ${session.callId}] Unexpected accept in state ${session.state}`);
  }

  session.setState(CALL_STATES.CONNECTING);

  getUserSockets(session.callee).forEach(sid => {
    if (sid !== calleeSocketId) io.to(sid).emit('call-cancelled-other-device');
  });

  getUserSockets(session.caller).forEach(sid => {
    io.to(sid).emit('call-accepted', { callId: session.callId, from: session.callee });
  });

  return { success: true };
}

function rejectCall(session) {
  getUserSockets(session.caller).forEach(sid => {
    io.to(sid).emit('call-rejected', { callId: session.callId, from: session.callee });
  });
  endCall(session, 'REJECTED');
  return { success: true };
}

function endCall(session, reason = 'NORMAL') {
  if (session.state === CALL_STATES.ENDED) return;

  session.setState(CALL_STATES.ENDED);
  session.endedAt   = Date.now();
  session.endReason = reason;

  const callRecord = saveCallToHistory(session, reason);

  [session.caller, session.callee].forEach(user => {
    getUserSockets(user).forEach(sid => {
      io.to(sid).emit('call-ended', {
        callId: session.callId, reason,
        duration: callRecord.duration, callRecord,
      });
    });
  });

  setTimeout(() => cleanupSession(session.callId), 5000);
}

function handleOffer(session, from, sdp, msgId) {
  if (!dedupeMessage(msgId)) return { ignored: true, reason: 'duplicate' };

  session.setState(CALL_STATES.NEGOTIATING);
  session.lastOffer = { from, sdp, ts: Date.now() };

  const collision = session.lastOffer && session.lastAnswer &&
                     session.lastOffer.from !== session.lastAnswer.from;

  if (collision && CONFIG.PERFECT_NEGOTIATION) {
    if (from === session.politePeer) {
      io.to(getUserSockets(from)[0]).emit('negotiation-rollback', { callId: session.callId, reason: 'collision' });
      return { rolledBack: true };
    }
  }

  const other = session.getOtherUser(from);
  getUserSockets(other).forEach(sid => {
    io.to(sid).emit('call-offer', { callId: session.callId, from, sdp, msgId });
  });

  clearTimeout(session.timers.negotiation);
  session.timers.negotiation = setTimeout(() => {
    if (session.state === CALL_STATES.NEGOTIATING) {
      console.log(`[Call ${session.callId}] Negotiation timeout`);
      session.setState(CALL_STATES.RECOVERING);
      [session.caller, session.callee].forEach(user => {
        getUserSockets(user).forEach(sid => io.to(sid).emit('negotiation-timeout', { callId: session.callId }));
      });
    }
  }, CONFIG.NEGOTIATION_TIMEOUT);

  return { success: true };
}

function handleAnswer(session, from, sdp, msgId) {
  if (!dedupeMessage(msgId)) return { ignored: true, reason: 'duplicate' };

  session.lastAnswer = { from, sdp, ts: Date.now() };
  clearTimeout(session.timers.negotiation);

  const other = session.getOtherUser(from);
  getUserSockets(other).forEach(sid => {
    io.to(sid).emit('call-answer', { callId: session.callId, from, sdp, msgId });
  });

  if (session.state === CALL_STATES.NEGOTIATING) {
    session.setState(CALL_STATES.CONNECTED);
    session.connectedAt = Date.now();
  }

  return { success: true };
}

function handleICE(session, from, candidate, msgId) {
  if (!dedupeMessage(msgId)) return { ignored: true, reason: 'duplicate' };

  session.iceCandidates[from].push({ candidate, ts: Date.now() });

  const other = session.getOtherUser(from);
  getUserSockets(other).forEach(sid => {
    io.to(sid).emit('call-ice', { callId: session.callId, from, candidate, msgId });
  });

  return { success: true };
}

function recoverSession(socket, callId, userName) {
  const session = sessions.calls.get(callId);
  if (!session) return { error: 'SESSION_NOT_FOUND' };
  if (!session.isParticipant(userName)) return { error: 'NOT_PARTICIPANT' };

  session.sockets[userName].add(socket.id);

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

  if (session.state === CALL_STATES.CONNECTED) session.setState(CALL_STATES.RECOVERING);

  return { success: true, stateSync };
}

// ============================================================================
// SOCKET EVENT HANDLERS
// ============================================================================

io.on("connection", (socket) => {
  console.log(`🔌 Connected: ${socket.id}`);

  startHealthCheck(socket);

  // ── Register user ──────────────────────────────────────────────────────────
  socket.on("register", ({ user, callType }) => {
    const rateCheck = checkRateLimit(user, 'GENERAL');
    if (!rateCheck.allowed) return socket.emit("rate-limited", { retryAfter: rateCheck.retryAfter, event: 'register' });

    const meta = new SocketMeta(socket.id, user);
    sessions.sockets.set(socket.id, meta);
    addUserSocket(user, socket.id);

    socket.data.user     = user;
    socket.data.callType = callType;

    socket.emit("registered", { socketId: socket.id, callType, serverTime: Date.now() });
    console.log(`📋 Registered: ${user} (${getUserSockets(user).length} device(s))`);
    broadcastPresence(user, true);
  });

  // ── Presence ───────────────────────────────────────────────────────────────
  socket.on("presence-check", ({ targetUser }) => {
    const isOnline = sessions.users.has(targetUser) && sessions.users.get(targetUser).size > 0;
    socket.emit("presence-status", { user: targetUser, isOnline });
  });

  socket.on("presence-heartbeat", () => {
    const user = socket.data.user;
    if (user) {
      const meta = sessions.sockets.get(socket.id);
      if (meta) meta.lastActivity = Date.now();
    }
  });

  // ── Typing indicators via Socket.IO ───────────────────────────────────────
  socket.on("typing-start", ({ chatId, user }) => {
    const rateCheck = checkRateLimit(user, 'TYPING');
    if (!rateCheck.allowed) return;
    const otherUser = user === 'Vishwa' ? 'Ammu' : 'Vishwa';
    getUserSockets(otherUser).forEach(sid => {
      io.to(sid).emit("typing-update", { user, isTyping: true, chatId, timestamp: Date.now() });
    });
  });

  socket.on("typing-stop", ({ chatId, user }) => {
    const otherUser = user === 'Vishwa' ? 'Ammu' : 'Vishwa';
    getUserSockets(otherUser).forEach(sid => {
      io.to(sid).emit("typing-update", { user, isTyping: false, chatId, timestamp: Date.now() });
    });
  });

  // ── FCM token registration ────────────────────────────────────────────────
  socket.on("fcm-register", ({ user, token }) => {
    if (user && token) {
      sessions.fcmTokens.set(user, token);
      console.log(`[FCM] Token registered for ${user} via socket`);
    }
  });

  // ── Call initiation ───────────────────────────────────────────────────────
  socket.on("call-user", ({ to, type }) => {
    const from = socket.data.user;
    if (!from) return socket.emit("error", { message: "Not registered" });

    const rateCheck = checkRateLimit(from, 'CALLS');
    if (!rateCheck.allowed) return socket.emit("rate-limited", { retryAfter: rateCheck.retryAfter, event: 'call-user' });

    const result = createCallSession(type, from, to);
    if (result.error) {
      if (result.error === 'CALL_EXISTS') socket.emit("call-exists", { callId: result.callId });
      else socket.emit("call-failed", { error: result.error });
      return;
    }

    const session = result;
    session.sockets[from].add(socket.id);
    sessions.sockets.get(socket.id).callId = session.callId;

    const initResult = initiateCall(session);
    if (initResult.error) {
      socket.emit("call-failed", { error: initResult.error });
      cleanupSession(session.callId);
      return;
    }

    socket.emit("call-initiated", {
      callId: session.callId, type: session.type, to,
      polite: from === session.politePeer,
    });
  });

  socket.on("call-accept", ({ callId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);
    if (!session || !session.isParticipant(from)) return socket.emit("error", { message: "Invalid call" });

    session.sockets[from].add(socket.id);
    sessions.sockets.get(socket.id).callId = callId;
    acceptCall(session, socket.id);
  });

  socket.on("call-reject", ({ callId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);
    if (session && session.isParticipant(from)) rejectCall(session);
  });

  socket.on("call-end", ({ callId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);
    if (session && session.isParticipant(from)) endCall(session, 'ENDED_BY_USER');
  });

  // ── WebRTC signaling ───────────────────────────────────────────────────────
  socket.on("call-offer", ({ callId, sdp, msgId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);
    if (session && session.isParticipant(from)) handleOffer(session, from, sdp, msgId || generateMessageId());
  });

  socket.on("call-answer", ({ callId, sdp, msgId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);
    if (session && session.isParticipant(from)) handleAnswer(session, from, sdp, msgId || generateMessageId());
  });

  socket.on("call-ice", ({ callId, candidate, msgId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);
    if (session && session.isParticipant(from)) handleICE(session, from, candidate, msgId || generateMessageId());
  });

  // ── Perfect negotiation ────────────────────────────────────────────────────
  socket.on("nego-rollback", ({ callId }) => {
    const session = sessions.calls.get(callId);
    if (session && session.state === CALL_STATES.NEGOTIATING) console.log(`[Call ${callId}] Rollback performed`);
  });

  socket.on("nego-needed", ({ callId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);
    if (!session || !session.isParticipant(from)) return;
    const other = session.getOtherUser(from);
    getUserSockets(other).forEach(sid => io.to(sid).emit('nego-needed', { callId, from }));
  });

  // ── Session recovery ───────────────────────────────────────────────────────
  socket.on("session-recover", ({ callId }) => {
    const user = socket.data.user;
    if (!user) return;
    const result = recoverSession(socket, callId, user);
    if (result.error) {
      socket.emit("session-recover-failed", { callId, error: result.error });
    } else {
      socket.emit("session-recovered", result.stateSync);
      const session = sessions.calls.get(callId);
      const other = session.getOtherUser(user);
      getUserSockets(other).forEach(sid => io.to(sid).emit('peer-recovered', { callId, user }));
    }
  });

  // ── Media state ────────────────────────────────────────────────────────────
  socket.on("media-state", ({ callId, video, audio }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);
    if (!session || !session.isParticipant(from)) return;
    session.mediaState[from] = { video, audio };
    const other = session.getOtherUser(from);
    getUserSockets(other).forEach(sid => io.to(sid).emit('peer-media-state', { callId, from, video, audio }));
  });

  // ── Camera sharing ─────────────────────────────────────────────────────────
  socket.on("join", ({ room, user }) => {
    socket.join(room);
    socket.data.room = room;
    socket.data.videoUser = user;
    if (!cameraRooms[room]) cameraRooms[room] = {};
    cameraRooms[room][socket.id] = user;
    const count = Object.keys(cameraRooms[room]).length;
    socket.emit("joined", { room, count });
    if (count > 1) {
      Object.entries(cameraRooms[room]).forEach(([sid, name]) => {
        if (sid !== socket.id && name === "Vishwa") io.to(sid).emit("request-offer", { to: user });
      });
    }
  });

  socket.on("camera-ready", ({ room, from })        => socket.to(room).emit("camera-ready", { from }));
  socket.on("offer",        ({ room, from, sdp })   => socket.to(room).emit("offer",  { from, sdp }));
  socket.on("answer",       ({ room, from, sdp })   => socket.to(room).emit("answer", { from, sdp }));
  socket.on("ice",          ({ room, from, candidate }) => socket.to(room).emit("ice", { from, candidate }));
  socket.on("camera-off",   ({ room, from })        => socket.to(room).emit("camera-off", { from }));

  // ════════════════════════════════════════════════════════════════════════
  // SCREEN SHARE SIGNALING (NEW)
  // ════════════════════════════════════════════════════════════════════════

  socket.on("share-join", ({ room, user }) => {
    const rateCheck = checkRateLimit(user, 'SCREEN_SHARE');
    if (!rateCheck.allowed) return socket.emit("rate-limited", { retryAfter: rateCheck.retryAfter, event: 'share-join' });

    socket.join(room);
    socket.data.shareRoom = room;
    socket.data.shareUser = user;
    if (!shareRooms[room]) shareRooms[room] = {};
    shareRooms[room][socket.id] = user;
    const count = Object.keys(shareRooms[room]).length;
    socket.emit("share-joined", { room, count });
    console.log(`🖥️ ${user} joined screen-share room (${count} device(s))`);
  });

  socket.on("share-ready", ({ room, from }) => {
    console.log(`🖥️ share-ready from ${from}`);
    socket.to(room).emit("share-ready", { from });
  });

  socket.on("share-offer", ({ room, from, sdp }) => {
    console.log(`🖥️ share-offer from ${from}`);
    socket.to(room).emit("share-offer", { from, sdp });
  });

  socket.on("share-answer", ({ room, from, sdp }) => {
    console.log(`🖥️ share-answer from ${from}`);
    socket.to(room).emit("share-answer", { from, sdp });
  });

  socket.on("share-ice", ({ room, from, candidate }) => {
    socket.to(room).emit("share-ice", { from, candidate });
  });

  socket.on("share-off", ({ room, from }) => {
    console.log(`🖥️ share-off from ${from}`);
    socket.to(room).emit("share-off", { from });
    if (room && shareRooms[room]) {
      // Don't delete socket entry here — disconnect handler manages cleanup
    }
  });

  // ── Legacy voice call support ──────────────────────────────────────────────
  socket.on("call-user-legacy", ({ to, from }) => {
    console.log(`📞 Legacy call: ${from} -> ${to}`);
    const targetSockets = getUserSockets(to);
    if (targetSockets.length === 0) { socket.emit('call-user-offline'); return; }
    targetSockets.forEach(sid => io.to(sid).emit('call-incoming', { from, type: 'voice' }));
  });

  socket.on("call-accept-legacy", ({ room, from }) => {
    console.log(`✅ Legacy accept: ${from}`);
    socket.to(room).emit("call-accepted", { from });
  });

  socket.on("call-reject-legacy",     ({ room, from }) => socket.to(room).emit("call-rejected", { from }));
  socket.on("call-end-legacy",        ({ room, from }) => socket.to(room).emit("call-ended", { from }));
  socket.on("call-offer-legacy",      ({ room, from, sdp }) => socket.to(room).emit("call-offer", { from, sdp }));
  socket.on("call-answer-legacy",     ({ room, from, sdp }) => socket.to(room).emit("call-answer", { from, sdp }));
  socket.on("call-ice-legacy",        ({ room, from, candidate }) => socket.to(room).emit("call-ice", { from, candidate }));

  // ── Hug sync ───────────────────────────────────────────────────────────────
  socket.on('hug-sync-initiate', (hugData) => {
    console.log(`🫂 Hug sync initiated:`, hugData);
    const initiatorSockets = getUserSockets(hugData.initiator);
    const responderSockets = getUserSockets(hugData.responder);
    if (initiatorSockets.length > 0 && responderSockets.length > 0) {
      initiatorSockets.forEach(sid => io.to(sid).emit('hug-sync-vibrate', hugData));
      responderSockets.forEach(sid => io.to(sid).emit('hug-sync-vibrate', hugData));
      console.log(`🫂 Synchronized hug sent`);
    } else {
      console.log(`❌ Hug sync failed: One or both users not connected`);
    }
  });

  // ── Message read events ────────────────────────────────────────────────────
  socket.on('messages-read', ({ messageIds, readBy, senderIds }) => {
    const rateCheck = checkRateLimit(readBy, 'MESSAGES');
    if (!rateCheck.allowed) return;
    console.log(`📖 Messages read by ${readBy}:`, messageIds);
    senderIds.forEach(senderId => {
      getUserSockets(senderId).forEach(sid => io.to(sid).emit('message-seen-confirmation', { messageIds, readBy }));
    });
  });

  // ── Disconnect ─────────────────────────────────────────────────────────────
  socket.on("disconnect", (reason) => {
    const user   = socket.data.user || socket.data.videoUser;
    const callId = sessions.sockets.get(socket.id)?.callId;

    console.log(`🔌 Disconnected: ${socket.id} (${user}) - ${reason}`);

    sessions.sockets.delete(socket.id);

    if (user) {
      removeUserSocket(user, socket.id);
      if (getUserSockets(user).length === 0) broadcastPresence(user, false);

      if (callId) {
        const session = sessions.calls.get(callId);
        if (session && session.isParticipant(user)) {
          session.sockets[user].delete(socket.id);
          const userSockets = getUserSockets(user);

          if (userSockets.length === 0) {
            const other = session.getOtherUser(user);
            getUserSockets(other).forEach(sid => {
              io.to(sid).emit('peer-disconnected', { callId, user, state: session.state });
            });

            if (CONFIG.SESSION_PERSISTENCE) {
              clearTimeout(session.timers.reconnect);
              session.timers.reconnect = setTimeout(() => {
                if ([CALL_STATES.CONNECTED, CALL_STATES.RECOVERING, CALL_STATES.RECONNECTING].includes(session.state)) {
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
    if (room && cameraRooms[room]) {
      delete cameraRooms[room][socket.id];
      if (Object.keys(cameraRooms[room]).length === 0) delete cameraRooms[room];
      else socket.to(room).emit("camera-off", { from: user });
    }

    // Screen share room cleanup
    const shareRoom = socket.data.shareRoom;
    const shareUser = socket.data.shareUser;
    if (shareRoom && shareRooms[shareRoom]) {
      delete shareRooms[shareRoom][socket.id];
      if (Object.keys(shareRooms[shareRoom]).length === 0) {
        delete shareRooms[shareRoom];
      } else {
        socket.to(shareRoom).emit("share-off", { from: shareUser });
      }
      console.log(`🖥️ ${shareUser} left screen-share room`);
    }
  });
});

// ============================================================================
// PERIODIC CLEANUP
// ============================================================================

setInterval(() => {
  const now = Date.now();
  for (const [callId, session] of sessions.calls) {
    if (session.state === CALL_STATES.ENDED && now - session.updatedAt > CONFIG.SESSION_TTL) {
      cleanupSession(callId);
    }
  }
  for (const [msgId, ts] of sessions.messageCache) {
    if (now - ts > CONFIG.MESSAGE_DEDUP_TTL) sessions.messageCache.delete(msgId);
  }
}, 60000);

// ============================================================================
// GRACEFUL SHUTDOWN
// ============================================================================

process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM received, shutting down gracefully');
  io.close(() => server.close(() => process.exit(0)));
});

process.on('SIGINT', () => {
  console.log('🛑 SIGINT received, shutting down gracefully');
  io.close(() => server.close(() => process.exit(0)));
});

// ============================================================================
// START SERVER
// ============================================================================

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Signaling Server v3.1 on port ${PORT}`);
  console.log(`   Health Check: http://localhost:${PORT}/health`);
  console.log(`   Features: Rate Limiting, Typing via Socket, Call History, Mood Cleanup, FCM, Screen Share`);
});
