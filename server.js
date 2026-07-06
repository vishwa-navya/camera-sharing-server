/**
 * server.js — Production-Grade Signaling Server v3.0
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
 * 9. Rate Limiting — prevent abuse
 * 10. Call History — persist call records
 * 11. Typing via Socket.IO — zero latency
 * 12. Mood Cleanup — server-side expiry
 * 13. FCM Push Notifications — server-side sending
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

  // Rate Limiting (requests per minute)
  RATE_LIMITS: {
    MESSAGES:       60,    // 60 messages/minute
    TYPING:         120,   // 120 typing events/minute
    CALLS:          10,    // 10 calls/minute
    MEDIA_UPLOADS:  20,    // 20 uploads/minute
    GENERAL:        200,   // 200 general events/minute
  },

  // Firebase (for mood cleanup and FCM)
  FIREBASE_PROJECT_ID: 'lastseen-8800e',
};

// ============================================================================
// RATE LIMITING
// ============================================================================

const rateLimits = new Map(); // userId -> { events: [timestamps], blockedUntil }

function checkRateLimit(userId, eventType = 'GENERAL') {
  const now = Date.now();
  const limit = CONFIG.RATE_LIMITS[eventType] || CONFIG.RATE_LIMITS.GENERAL;
  const windowMs = 60000; // 1 minute window

  if (!rateLimits.has(userId)) {
    rateLimits.set(userId, {});
  }

  const userLimits = rateLimits.get(userId);

  if (!userLimits[eventType]) {
    userLimits[eventType] = { events: [], blockedUntil: 0 };
  }

  const limitData = userLimits[eventType];

  // Check if blocked
  if (limitData.blockedUntil > now) {
    return { allowed: false, retryAfter: limitData.blockedUntil - now };
  }

  // Clean old events
  limitData.events = limitData.events.filter(ts => now - ts < windowMs);

  // Check limit
  if (limitData.events.length >= limit) {
    // Block for 30 seconds
    limitData.blockedUntil = now + 30000;
    console.warn(`[RateLimit] ${userId} blocked for ${eventType} (too many requests)`);
    return { allowed: false, retryAfter: 30000 };
  }

  // Add event
  limitData.events.push(now);
  return { allowed: true };
}

// Clean up old rate limit entries every 5 minutes
setInterval(() => {
  const now = Date.now();
  for (const [userId, limits] of rateLimits) {
    for (const [type, data] of Object.entries(limits)) {
      data.events = data.events.filter(ts => now - ts < 60000);
      if (data.events.length === 0 && data.blockedUntil < now) {
        delete limits[type];
      }
    }
    if (Object.keys(limits).length === 0) {
      rateLimits.delete(userId);
    }
  }
}, 300000);

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

  // Call history (in-memory, also persisted to Firebase)
  callHistory: [],

  // FCM tokens: userName -> token
  fcmTokens: new Map(),
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
    this.connectedAt = null;    // When call actually connects
    this.endedAt = null;         // When call ends
    this.endReason = null;       // Reason call ended

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
    this.lastActivity = Date.now();
  }
}

// ============================================================================
// FIREBASE HELPERS (for mood cleanup, call history, FCM)
// ============================================================================

async function writeToFirestore(collection, docId, data) {
  try {
    const url = `https://firestore.googleapis.com/v1/projects/${CONFIG.FIREBASE_PROJECT_ID}/databases/(default)/documents/${collection}/${docId}`;
    const fields = {};
    for (const [key, value] of Object.entries(data)) {
      if (value === null || value === undefined) continue;
      if (typeof value === 'string') {
        fields[key] = { stringValue: value };
      } else if (typeof value === 'number') {
        fields[key] = { integerValue: value.toString() };
      } else if (typeof value === 'boolean') {
        fields[key] = { booleanValue: value };
      } else if (value instanceof Date) {
        fields[key] = { timestampValue: value.toISOString() };
      } else if (typeof value === 'object') {
        // Skip invalid values
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

async function queryFirestore(collection, filter = null) {
  try {
    // Simple approach: we can't do complex queries via REST easily
    // So we'll use a different approach for mood cleanup
    const url = `https://firestore.googleapis.com/v1/projects/${CONFIG.FIREBASE_PROJECT_ID}/databases/(default)/documents/${collection}`;
    const response = await fetch(url);
    const data = await response.json();
    return data.documents || [];
  } catch (err) {
    console.error('[Firestore] Query error:', err.message);
    return [];
  }
}

// Send FCM push notification
async function sendFCMNotification(fcmToken, title, body, data = {}) {
  if (!fcmToken) {
    console.warn('[FCM] No token provided');
    return false;
  }

  try {
    // Use Firebase REST API or a server key approach
    // For now, we'll store the token for server-side sending
    // The actual FCM send requires a server key which should be in env vars
    const serverKey = process.env.FCM_SERVER_KEY;
    if (!serverKey) {
      console.warn('[FCM] No server key configured - notifications disabled');
      return false;
    }

    const response = await fetch('https://fcm.googleapis.com/fcm/send', {
      method: 'POST',
      headers: {
        'Authorization': `key=${serverKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        to: fcmToken,
        notification: { title, body },
        data,
        priority: 'high',
      }),
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

function generateCallId() {
  return `call_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
}

function generateMessageId() {
  return `${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
}

/**
 * REAL-TIME PRESENCE: Broadcast user online/offline status
 * Immediately notifies all relevant parties
 */
function broadcastPresence(user, isOnline) {
  const knownUsers = ["Vishwa", "Ammu"];
  const otherUser = knownUsers.find(u => u !== user);

  if (otherUser) {
    const otherSockets = getUserSockets(otherUser);
    otherSockets.forEach(sid => {
      io.to(sid).emit("presence-update", {
        user,
        isOnline,
        timestamp: Date.now(),
      });
    });
    console.log(`[Presence] ${user} is ${isOnline ? "ONLINE" : "OFFLINE"} → notified ${otherUser}`);
  }
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
    return false;
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
// CALL HISTORY
// ============================================================================

function saveCallToHistory(session, endReason) {
  const callRecord = {
    callId: session.callId,
    type: session.type,
    caller: session.caller,
    callee: session.callee,
    createdAt: session.createdAt,
    connectedAt: session.connectedAt,
    endedAt: Date.now(),
    duration: session.connectedAt ? Date.now() - session.connectedAt : 0,
    endReason: endReason,
    didConnect: !!session.connectedAt,
  };

  // Save to in-memory history
  sessions.callHistory.push(callRecord);

  // Keep only last 100 calls in memory
  if (sessions.callHistory.length > 100) {
    sessions.callHistory = sessions.callHistory.slice(-100);
  }

  // Save to Firestore for persistence
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

  pingInterval = setInterval(sendPing, CONFIG.HEALTH_PING_INTERVAL);
  sendPing();

  socket.once('disconnect', () => {
    clearInterval(pingInterval);
    clearTimeout(pongTimeout);
  });
}

// ============================================================================
// MOOD CLEANUP (Periodic)
// ============================================================================

async function cleanupExpiredMoods() {
  console.log('[MoodCleanup] Checking for expired moods...');
  const now = Date.now();

  // Firebase REST API doesn't support queries well, so we'll use
  // a simpler approach: check known mood documents
  const chatId = 'privateMessages';
  const users = ['Vishwa', 'Ammu'];

  for (const user of users) {
    try {
      const docId = `${chatId}_${user}`;
      const url = `https://firestore.googleapis.com/v1/projects/${CONFIG.FIREBASE_PROJECT_ID}/databases/(default)/documents/moods/${docId}`;
      const response = await fetch(url);
      const data = await response.json();

      if (data.fields && data.fields.expiresAt) {
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

// Run mood cleanup every 5 minutes
setInterval(cleanupExpiredMoods, 300000);
console.log('[MoodCleanup] Scheduled every 5 minutes');

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
  transports: ['websocket', 'polling'],
});

// ============================================================================
// EXPRESS ROUTES
// ============================================================================

app.get("/", (req, res) => res.send("Signaling Server v3.0 - Ready"));

app.get("/health", (req, res) => res.json({
  ok: true,
  version: "3.0",
  uptime: process.uptime(),
  activeCalls: sessions.calls.size,
  connectedSockets: sessions.sockets.size,
  callHistoryCount: sessions.callHistory.length,
}));

// Wake-up endpoint (called when app loads)
app.post("/wake", express.json(), (req, res) => {
  // Immediately respond to wake the server
  res.json({ awake: true, ts: Date.now() });
  console.log('[Wake] Server woke up');
});

// Get call history
app.get("/call-history/:user", async (req, res) => {
  const { user } = req.params;
  const userCalls = sessions.callHistory.filter(
    c => c.caller === user || c.callee === user
  );
  res.json({ calls: userCalls.slice(-50) }); // Last 50 calls
});

// Register FCM token
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

// Trigger FCM notification (for testing)
app.post("/notify", express.json(), async (req, res) => {
  const { user, title, body, data } = req.body;
  const token = sessions.fcmTokens.get(user);
  if (!token) {
    return res.status(404).json({ error: 'User token not found' });
  }
  const sent = await sendFCMNotification(token, title, body, data);
  res.json({ sent });
});

// ============================================================================
// CALL MANAGEMENT
// ============================================================================

function createCallSession(type, caller, callee) {
  const existingCall = getActiveCallForUser(caller);
  if (existingCall) {
    return { error: 'CALL_EXISTS', callId: existingCall.callId };
  }

  const callId = generateCallId();
  const session = new CallSession(callId, type, caller, callee);
  sessions.calls.set(callId, session);

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

function initiateCall(session, callerSocketId) {
  session.setState(CALL_STATES.JOINING);

  const callee = session.callee;
  const calleeSockets = getUserSockets(callee);

  if (calleeSockets.length === 0) {
    return { error: 'USER_OFFLINE' };
  }

  session.setState(CALL_STATES.WAITING);
  session.offerer = session.caller;

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

function acceptCall(session, calleeSocketId) {
  if (session.state === CALL_STATES.ENDED) {
    return { error: 'CALL_ENDED' };
  }

  if (session.state !== CALL_STATES.WAITING) {
    console.warn(`[Call ${session.callId}] Unexpected accept in state ${session.state}`);
  }

  session.setState(CALL_STATES.CONNECTING);

  const callee = session.callee;
  const calleeSockets = getUserSockets(callee);
  calleeSockets.forEach(sid => {
    if (sid !== calleeSocketId) {
      io.to(sid).emit('call-cancelled-other-device');
    }
  });

  const callerSockets = getUserSockets(session.caller);
  callerSockets.forEach(sid => {
    io.to(sid).emit('call-accepted', {
      callId: session.callId,
      from: callee,
    });
  });

  return { success: true };
}

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

function endCall(session, reason = 'NORMAL') {
  if (session.state === CALL_STATES.ENDED) return;

  session.setState(CALL_STATES.ENDED);
  session.endedAt = Date.now();
  session.endReason = reason;

  // Save to call history
  const callRecord = saveCallToHistory(session, reason);

  // Notify all participants
  [session.caller, session.callee].forEach(user => {
    getUserSockets(user).forEach(sid => {
      io.to(sid).emit('call-ended', {
        callId: session.callId,
        reason,
        duration: callRecord.duration,
        callRecord,
      });
    });
  });

  setTimeout(() => cleanupSession(session.callId), 5000);
}

function handleOffer(session, from, sdp, msgId) {
  if (!dedupeMessage(msgId)) {
    return { ignored: true, reason: 'duplicate' };
  }

  session.setState(CALL_STATES.NEGOTIATING);
  session.lastOffer = { from, sdp, ts: Date.now() };

  const collision = session.lastOffer && session.lastAnswer &&
                    session.lastOffer.from !== session.lastAnswer.from;

  if (collision && CONFIG.PERFECT_NEGOTIATION) {
    if (from === session.politePeer) {
      io.to(getUserSockets(from)[0]).emit('negotiation-rollback', {
        callId: session.callId,
        reason: 'collision',
      });
      return { rolledBack: true };
    }
  }

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

  clearTimeout(session.timers.negotiation);
  session.timers.negotiation = setTimeout(() => {
    if (session.state === CALL_STATES.NEGOTIATING) {
      console.log(`[Call ${session.callId}] Negotiation timeout`);
      session.setState(CALL_STATES.RECOVERING);
      [session.caller, session.callee].forEach(user => {
        getUserSockets(user).forEach(sid => {
          io.to(sid).emit('negotiation-timeout', { callId: session.callId });
        });
      });
    }
  }, CONFIG.NEGOTIATION_TIMEOUT);

  return { success: true };
}

function handleAnswer(session, from, sdp, msgId) {
  if (!dedupeMessage(msgId)) {
    return { ignored: true, reason: 'duplicate' };
  }

  session.lastAnswer = { from, sdp, ts: Date.now() };
  clearTimeout(session.timers.negotiation);

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
    session.connectedAt = Date.now(); // Mark when call actually connects
  }

  return { success: true };
}

function handleICE(session, from, candidate, msgId) {
  if (!dedupeMessage(msgId)) {
    return { ignored: true, reason: 'duplicate' };
  }

  session.iceCandidates[from].push({ candidate, ts: Date.now() });

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

function recoverSession(socket, callId, userName) {
  const session = sessions.calls.get(callId);
  if (!session) {
    return { error: 'SESSION_NOT_FOUND' };
  }

  if (!session.isParticipant(userName)) {
    return { error: 'NOT_PARTICIPANT' };
  }

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

  startHealthCheck(socket);

  // Register user
  socket.on("register", ({ user, callType }) => {
    // Rate limit check
    const rateCheck = checkRateLimit(user, 'GENERAL');
    if (!rateCheck.allowed) {
      return socket.emit("rate-limited", {
        retryAfter: rateCheck.retryAfter,
        event: 'register'
      });
    }

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

    broadcastPresence(user, true);
  });

  // ========================
  // REAL-TIME PRESENCE
  // ========================

  socket.on("presence-check", ({ targetUser }) => {
    const isOnline = sessions.users.has(targetUser) && sessions.users.get(targetUser).size > 0;
    socket.emit("presence-status", { user: targetUser, isOnline });
  });

  socket.on("presence-heartbeat", () => {
    const user = socket.data.user;
    if (user) {
      const meta = sessions.sockets.get(socket.id);
      if (meta) {
        meta.lastActivity = Date.now();
      }
    }
  });

  // ========================
  // TYPING INDICATORS (via Socket.IO - instant, zero Firebase cost)
  // ========================

  socket.on("typing-start", ({ chatId, user }) => {
    // Rate limit
    const rateCheck = checkRateLimit(user, 'TYPING');
    if (!rateCheck.allowed) return;

    const otherUser = user === 'Vishwa' ? 'Ammu' : 'Vishwa';
    const otherSockets = getUserSockets(otherUser);
    otherSockets.forEach(sid => {
      io.to(sid).emit("typing-update", {
        user,
        isTyping: true,
        chatId,
        timestamp: Date.now(),
      });
    });
  });

  socket.on("typing-stop", ({ chatId, user }) => {
    const otherUser = user === 'Vishwa' ? 'Ammu' : 'Vishwa';
    const otherSockets = getUserSockets(otherUser);
    otherSockets.forEach(sid => {
      io.to(sid).emit("typing-update", {
        user,
        isTyping: false,
        chatId,
        timestamp: Date.now(),
      });
    });
  });

  // ========================
  // FCM TOKEN REGISTRATION
  // ========================

  socket.on("fcm-register", ({ user, token }) => {
    if (user && token) {
      sessions.fcmTokens.set(user, token);
      console.log(`[FCM] Token registered for ${user} via socket`);
    }
  });

  // ========================
  // CALL INITIATION
  // ========================

  socket.on("call-user", ({ to, type }) => {
    const from = socket.data.user;
    if (!from) return socket.emit("error", { message: "Not registered" });

    // Rate limit
    const rateCheck = checkRateLimit(from, 'CALLS');
    if (!rateCheck.allowed) {
      return socket.emit("rate-limited", {
        retryAfter: rateCheck.retryAfter,
        event: 'call-user'
      });
    }

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

  socket.on("nego-rollback", ({ callId }) => {
    const session = sessions.calls.get(callId);
    if (session && session.state === CALL_STATES.NEGOTIATING) {
      console.log(`[Call ${callId}] Rollback performed`);
    }
  });

  socket.on("nego-needed", ({ callId }) => {
    const from = socket.data.user;
    const session = sessions.calls.get(callId);

    if (!session || !session.isParticipant(from)) {
      return;
    }

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

  socket.on("call-accept-legacy", ({ room, from }) => {
    console.log(`✅ Legacy accept: ${from}`);
    socket.to(room).emit("call-accepted", { from });
  });

  socket.on("call-reject-legacy", ({ room, from }) => {
    socket.to(room).emit("call-rejected", { from });
  });

  socket.on("call-end-legacy", ({ room, from }) => {
    socket.to(room).emit("call-ended", { from });
  });

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
  // HUG SYNC
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
    // Rate limit
    const rateCheck = checkRateLimit(readBy, 'MESSAGES');
    if (!rateCheck.allowed) return;

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

    sessions.sockets.delete(socket.id);

    if (user) {
      removeUserSocket(user, socket.id);

      const remainingSockets = getUserSockets(user);
      if (remainingSockets.length === 0) {
        broadcastPresence(user, false);
      }

      if (callId) {
        const session = sessions.calls.get(callId);
        if (session && session.isParticipant(user)) {
          session.sockets[user].delete(socket.id);

          const userSockets = getUserSockets(user);

          if (userSockets.length === 0) {
            const other = session.getOtherUser(user);
            getUserSockets(other).forEach(sid => {
              io.to(sid).emit('peer-disconnected', {
                callId,
                user,
                state: session.state,
              });
            });

            if (CONFIG.SESSION_PERSISTENCE) {
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

  for (const [callId, session] of sessions.calls) {
    if (session.state === CALL_STATES.ENDED) {
      const age = now - session.updatedAt;
      if (age > CONFIG.SESSION_TTL) {
        cleanupSession(callId);
      }
    }
  }

  for (const [msgId, ts] of sessions.messageCache) {
    if (now - ts > CONFIG.MESSAGE_DEDUP_TTL) {
      sessions.messageCache.delete(msgId);
    }
  }

}, 60000);

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
  console.log(`🚀 Signaling Server v3.0 on port ${PORT}`);
  console.log(`   Health Check: http://localhost:${PORT}/health`);
  console.log(`   Features: Rate Limiting, Typing via Socket, Call History, Mood Cleanup, FCM`);
});
