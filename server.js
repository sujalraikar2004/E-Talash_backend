const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const cors = require('cors');
const dotenv = require('dotenv');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { promisify } = require('util');

const scrypt = promisify(crypto.scrypt);

// Load the server's environment file even when the process is started from the repository root.
dotenv.config({ path: path.join(__dirname, '.env') });

// Create express app
const app = express();
app.use(cors({ origin: true }));
app.use(express.json());

app.get('/api/health', (_req, res) => res.json({ ok: true }));

// Create HTTP server
const server = http.createServer(app);

// Connect to MongoDB
mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/treasurehunt', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('MongoDB connected'))
.catch(err => console.error('MongoDB connection error:', err));

// Define player schema
const playerSchema = new mongoose.Schema({
  username: String,
  email: { type: String, unique: true, sparse: true, lowercase: true, trim: true },
  passwordHash: String,
  userId: String,
  highScore: { type: Number, default: 0 },
  gamesCompleted: { type: Number, default: 0 },
  fastestTime: { type: Number, default: null }
});

const Player = mongoose.model('Player', playerSchema);

async function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const derivedKey = await scrypt(password, salt, 64);
  return `${salt}:${derivedKey.toString('hex')}`;
}

async function verifyPassword(password, storedHash) {
  if (!storedHash) return false;
  const [salt, key] = storedHash.split(':');
  const derivedKey = await scrypt(password, salt, 64);
  return crypto.timingSafeEqual(Buffer.from(key, 'hex'), derivedKey);
}

// Define room schema
const roomSchema = new mongoose.Schema({
  code: { type: String, required: true, unique: true },
  hostUserId: { type: String, default: null },
  createdAt: { type: Date, default: Date.now },
  players: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Player' }],
  isActive: { type: Boolean, default: true },
  gameState: { type: mongoose.Schema.Types.Mixed, default: null }
});

const Room = mongoose.model('Room', roomSchema);

// Initialize Socket.IO server
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

// Store active rooms and players
const activeRooms = new Map();

const REGULAR_QUESTION_COUNT = 9;
const QUESTION_COUNT = 10;
const FINAL_QUESTION_ID = 10;
function createQuestionOrder(existingPlayers = []) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const regularOrder = Array.from({ length: REGULAR_QUESTION_COUNT }, (_, index) => index + 1)
      .sort(() => Math.random() - 0.5);
    const order = [...regularOrder, FINAL_QUESTION_ID];
    const doesNotDuplicateCurrentSlots = existingPlayers.every(existingPlayer =>
      !existingPlayer.questionOrder || regularOrder.every((questionId, index) => questionId !== existingPlayer.questionOrder[index])
    );
    if (doesNotDuplicateCurrentSlots) return order;
  }
  return [
    ...Array.from({ length: REGULAR_QUESTION_COUNT }, (_, index) => index + 1)
      .sort(() => Math.random() - 0.5),
    FINAL_QUESTION_ID,
  ];
}

function normalizePlayerQuestionState(player, existingPlayers = []) {
  if (!Array.isArray(player.questionOrder) || player.questionOrder.length !== QUESTION_COUNT || player.questionOrder[QUESTION_COUNT - 1] !== FINAL_QUESTION_ID) {
    player.questionOrder = createQuestionOrder(existingPlayers.filter(existing => existing.id !== player.id));
  }
  if (!Array.isArray(player.solvedQuestionIds)) player.solvedQuestionIds = [];
  player.solvedQuestionIds = player.solvedQuestionIds.filter(questionId => player.questionOrder.includes(questionId));
  player.currentQuestionIndex = Math.min(Math.max(Number(player.currentQuestionIndex) || 0, 0), QUESTION_COUNT - 1);
}

function persistedRoomState(room) {
  return {
    hostId: room.hostId,
    phase: room.phase,
    isGameActive: room.isGameActive,
    gameComplete: room.gameComplete,
    countdownEndsAt: room.countdownEndsAt,
    gameStartedAt: room.gameStartedAt,
    players: room.players.map(({ socketId, ...player }) => ({ ...player, connected: false })),
  };
}

function persistRoomState(roomCode, room) {
  Room.findOneAndUpdate({ code: roomCode, isActive: true }, { gameState: persistedRoomState(room) })
    .catch(error => console.error(`Failed to persist room ${roomCode}:`, error));
}

const createActiveRoom = () => ({
  players: [],
  hostId: null,
  phase: 'waiting',
  isGameActive: false,
  currentQuestionIndex: 0,
  gameComplete: false,
  countdownEndsAt: null,
  gameStartedAt: null,
  cleanupTimer: null,
});

function roomState(room) {
  return {
    hostId: room.hostId,
    players: room.players,
    phase: room.phase,
    isGameActive: room.phase === 'playing',
    gameComplete: room.gameComplete,
    countdownEndsAt: room.countdownEndsAt,
    gameStartedAt: room.gameStartedAt,
    serverNow: Date.now(),
  };
}

function startRoom(roomCode, room) {
  if (room.phase !== 'waiting' || room.players.length === 0 || !room.players.every(player => player.isReady)) return;
  const countdownEndsAt = Date.now() + 10000;
  room.phase = 'countdown';
  room.countdownEndsAt = countdownEndsAt;
  room.gameStartedAt = countdownEndsAt;
  room.isGameActive = false;
  persistRoomState(roomCode, room);
  io.to(roomCode).emit('gameStart', { startsAt: countdownEndsAt, serverNow: Date.now() });
  setTimeout(() => {
    if (room.phase === 'countdown' && room.gameStartedAt === countdownEndsAt) {
      room.phase = 'playing';
      room.isGameActive = true;
      persistRoomState(roomCode, room);
      io.to(roomCode).emit('gameLive', roomState(room));
    }
  }, 10000);
}

// Socket.IO connection handler
io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  // Create a new room
  socket.on('createRoom', async ({ roomCode, user }) => {
    try {
      if (!roomCode || !/^[A-Z0-9]{4,8}$/.test(roomCode)) {
        socket.emit('error', { message: 'Invalid room code' });
        return;
      }
      const normalizedCode = roomCode.toUpperCase();
      const existingRoom = await Room.findOne({ code: normalizedCode, isActive: true });
      if (existingRoom) {
        socket.emit('error', { message: 'Room code already exists' });
        return;
      }
      // Create room in database
      const newRoom = new Room({ code: normalizedCode, hostUserId: user?.id || null });
      await newRoom.save();

      const activeRoom = createActiveRoom();
      activeRoom.hostId = user?.id || null;
      activeRooms.set(normalizedCode, activeRoom);

      if (user?.id && user?.username) {
        const player = await Player.findOneAndUpdate(
          { userId: user.id },
          { $setOnInsert: { userId: user.id, username: user.username } },
          { new: true, upsert: true }
        );
        newRoom.players.push(player._id);
        await newRoom.save();
        activeRoom.players.push({ id: user.id, username: user.username, isReady: false, score: 0, currentQuestionIndex: 0, solvedQuestionIds: [], questionOrder: createQuestionOrder(), connected: true, socketId: socket.id });
        socket.userId = user.id;
        socket.roomCode = normalizedCode;
      }

      socket.join(normalizedCode);
      socket.emit('roomCreated', normalizedCode);
      persistRoomState(normalizedCode, activeRoom);
      socket.emit('roomState', roomState(activeRoom));
      
      console.log(`Room created: ${roomCode}`);
    } catch (error) {
      console.error('Error creating room:', error);
      socket.emit('error', { message: 'Failed to create room' });
    }
  });

  // Join an existing room
  socket.on('joinRoom', async ({ roomCode, user }) => {
    try {
      if (!roomCode || !user?.id || !user?.username) {
        socket.emit('error', { message: 'Room code and user are required' });
        return;
      }
      roomCode = roomCode.toUpperCase();
      // Check if room exists
      const room = await Room.findOne({ code: roomCode, isActive: true });
      
      if (!room) {
        socket.emit('error', { message: 'Room not found' });
        return;
      }

      // Check if player exists in database
      let player = await Player.findOne({ userId: user.id });
      
      if (!player) {
        // Create new player
        player = new Player({
          username: user.username,
          userId: user.id
        });
        await player.save();
      }

      // Add player to room in database if not already added
      if (!room.players.includes(player._id)) {
        room.players.push(player._id);
        await room.save();
      }

      // Add player to active room
      if (!activeRooms.has(roomCode)) {
        const restoredRoom = createActiveRoom();
        if (room.gameState) Object.assign(restoredRoom, room.gameState);
        restoredRoom.hostId = room.hostUserId || restoredRoom.hostId || null;
        restoredRoom.players = (room.gameState?.players || []).map(player => ({ ...player, connected: false, socketId: null }));
        activeRooms.set(roomCode, restoredRoom);
        if (restoredRoom.phase === 'countdown' && restoredRoom.gameStartedAt) {
          const remaining = Math.max(0, restoredRoom.gameStartedAt - Date.now());
          setTimeout(() => {
            if (restoredRoom.phase === 'countdown') {
              restoredRoom.phase = 'playing';
              restoredRoom.isGameActive = true;
              persistRoomState(roomCode, restoredRoom);
              io.to(roomCode).emit('gameLive', roomState(restoredRoom));
            }
          }, remaining);
        }
      }

      const activeRoom = activeRooms.get(roomCode);
      
      // Track the socket id with the user for future reference
      socket.userId = user.id;
      socket.roomCode = roomCode;
      
      // Check if player already exists in the room to avoid duplicates
      const existingPlayer = activeRoom.players.find(p => p.id === user.id);
      if (!activeRoom.hostId && activeRoom.players.length <= 1 && (!activeRoom.players.length || existingPlayer)) {
        activeRoom.hostId = user.id;
        Room.findOneAndUpdate({ code: roomCode }, { hostUserId: user.id }).catch(error => console.error(`Failed to migrate host for ${roomCode}:`, error));
      }
      if (existingPlayer) {
        existingPlayer.username = user.username;
        existingPlayer.connected = true;
        existingPlayer.socketId = socket.id;
        normalizePlayerQuestionState(existingPlayer, activeRoom.players);
        if (activeRoom.cleanupTimer) {
          clearTimeout(activeRoom.cleanupTimer);
          activeRoom.cleanupTimer = null;
        }
      } else {
        activeRoom.players.push({
          id: user.id,
          username: user.username,
          isReady: false,
          score: 0,
          currentQuestionIndex: 0,
          solvedQuestionIds: [],
          questionOrder: createQuestionOrder(activeRoom.players),
          connected: true,
          socketId: socket.id
        });
      }

      socket.join(roomCode);

      // Broadcast to other players in the room
      socket.to(roomCode).emit(existingPlayer ? 'playerReconnected' : 'playerJoined', activeRoom.players.find(p => p.id === user.id));

      // Send current room state to the new player
      socket.emit('roomState', roomState(activeRoom));
      persistRoomState(roomCode, activeRoom);
      
      console.log(`User ${user.username} joined room ${roomCode}, players in room:`, activeRoom.players.length);
    } catch (error) {
      console.error('Error joining room:', error);
      socket.emit('error', { message: 'Failed to join room' });
    }
  });

  // Player ready status
  socket.on('playerReady', ({ roomCode }) => {
    if (!activeRooms.has(roomCode)) {
      console.log('Room not found:', roomCode);
      return;
    }

    const room = activeRooms.get(roomCode);
    const playerId = socket.userId;
    console.log(`Player ${playerId} ready in room ${roomCode}`);
    const playerIndex = room.players.findIndex(p => p.id === playerId && p.socketId === socket.id);
    
    if (playerIndex !== -1) {
      room.players[playerIndex].isReady = true;
      persistRoomState(roomCode, room);
      
      // Broadcast player ready to everyone in the room
      io.to(roomCode).emit('playerReady', playerId);
      
      const allReady = room.players.every(p => p.isReady);
      console.log(`All players ready in room ${roomCode}: ${allReady}`);
      console.log(`Players in room: ${room.players.length}`);
      
      if (allReady && room.players.length > 0) {
        // Start the game countdown
        console.log(`Starting game in room ${roomCode}`);
        startRoom(roomCode, room);
      }
    }
  });

  // Keep the currently viewed question in sync so reconnects restore navigation.
  socket.on('selectQuestion', ({ roomCode, questionIndex }) => {
    const room = activeRooms.get(roomCode);
    if (!room) return;
    const player = room.players.find(p => p.id === socket.userId && p.socketId === socket.id);
    const index = Number(questionIndex);
    if (!player || !Number.isInteger(index) || index < 0 || index >= QUESTION_COUNT) return;
    if (index === QUESTION_COUNT - 1 && (player.solvedQuestionIds?.length || 0) < REGULAR_QUESTION_COUNT) return;
    player.currentQuestionIndex = index;
    persistRoomState(roomCode, room);
  });

  // Correct answer submitted
  socket.on('correctAnswer', ({ roomCode, questionIndex, questionId, points }) => {
    if (!activeRooms.has(roomCode)) return;
    
    const room = activeRooms.get(roomCode);
    if (room.phase !== 'playing') return;
    
    // Update the player's score
    const playerId = socket.userId;
    const playerIndex = room.players.findIndex(p => p.id === playerId && p.socketId === socket.id);
    
    if (playerIndex !== -1) {
      const player = room.players[playerIndex];
      const normalizedQuestionId = Number(questionId);
      if (player.completionTime !== undefined || Number(questionIndex) !== player.currentQuestionIndex || normalizedQuestionId !== player.questionOrder[player.currentQuestionIndex]) return;
      if (player.solvedQuestionIds?.includes(normalizedQuestionId)) return;
      if (normalizedQuestionId === FINAL_QUESTION_ID && (player.solvedQuestionIds?.length || 0) < REGULAR_QUESTION_COUNT) return;
      // Add points for correct answer
      player.score += Math.max(1, Math.min(Number(points) || 100, 500));
      player.solvedQuestionIds = [...(player.solvedQuestionIds || []), normalizedQuestionId];
      persistRoomState(roomCode, room);
      
      // Broadcast updated player score to all players in the room
      io.to(roomCode).emit('updateScores', room.players);
    }
  });

  // Player completes game
  socket.on('completeGame', async ({ roomCode }) => {
    if (!activeRooms.has(roomCode)) return;
    
    const room = activeRooms.get(roomCode);
    const playerId = socket.userId;
    const playerIndex = room.players.findIndex(p => p.id === playerId && p.socketId === socket.id);
    
    if (playerIndex !== -1) {
      const playerState = room.players[playerIndex];
      if (playerState.completionTime !== undefined || (playerState.solvedQuestionIds?.length || 0) < QUESTION_COUNT || !room.gameStartedAt) return;
      // Update player's completion time in the room
      const time = Math.max(0, Math.floor((Date.now() - room.gameStartedAt) / 1000));
      playerState.completionTime = time;
      
      try {
        // Update player stats in database
        const player = await Player.findOne({ userId: playerId });
        
        if (player) {
          player.gamesCompleted += 1;
          player.highScore = Math.max(player.highScore, playerState.score);
          
          // Update fastest time if this is faster or first completion
          if (!player.fastestTime || time < player.fastestTime) {
            player.fastestTime = time;
            console.log(`Updated player ${playerId} with new fastest time: ${time}`);
          }
          
          await player.save();
          console.log(`Updated player stats in database for ${playerId}`);
        } else {
          console.log(`No player found with userId: ${playerId}`);
        }
        
        // Sort players by completion time
        room.players.sort((a, b) => {
          const timeA = a.completionTime || Number.MAX_VALUE;
          const timeB = b.completionTime || Number.MAX_VALUE;
          return timeA - timeB;
        });
        
        room.gameComplete = room.players.some(player => player.completionTime !== undefined);
        persistRoomState(roomCode, room);
        
        // Send updated leaderboard to all players
        io.to(roomCode).emit('updateScores', room.players);
        
        // Check if all active players have completed
        const activePlayers = room.players.filter(p => p.isReady);
        const allCompleted = activePlayers.length > 0 && activePlayers.every(p => Number.isFinite(p.completionTime));
        
        if (allCompleted) {
          console.log(`All players completed the game in room ${roomCode}`);
          io.to(roomCode).emit('gameEnd', room.players);
        }
      } catch (error) {
        console.error('Error updating player stats:', error);
      }
    } else {
      console.error(`Player with ID ${playerId} not found in room ${roomCode}`);
    }
  });

  // Reset game
  socket.on('resetGame', ({ roomCode }) => {
    if (!activeRooms.has(roomCode)) return;
    
    const room = activeRooms.get(roomCode);
    
    room.phase = 'waiting';
    room.isGameActive = false;
    room.currentQuestionIndex = 0;
    room.gameComplete = false;
    room.countdownEndsAt = null;
    room.gameStartedAt = null;
    room.players = room.players.map(p => ({
      ...p,
      isReady: false,
      score: 0,
      completionTime: undefined,
      currentQuestionIndex: 0,
      solvedQuestionIds: []
    }));
    persistRoomState(roomCode, room);
    
    io.to(roomCode).emit('gameReset', roomState(room));
  });

  socket.on('endRoom', ({ roomCode }) => {
    if (!activeRooms.has(roomCode)) return;
    const room = activeRooms.get(roomCode);
    if (!room.hostId || socket.userId !== room.hostId) {
      socket.emit('error', { message: 'Only the room creator can end this room' });
      return;
    }
    if (room.cleanupTimer) clearTimeout(room.cleanupTimer);
    activeRooms.delete(roomCode);
    Room.findOneAndUpdate({ code: roomCode }, { isActive: false, gameState: null })
      .catch(error => console.error(`Failed to close room ${roomCode}:`, error));
    io.to(roomCode).emit('roomEnded', { roomCode, endedBy: socket.userId });
    io.in(roomCode).socketsLeave(roomCode);
  });

  // Leave room
  socket.on('leaveRoom', ({ roomCode }) => {
    removePlayerFromRoom(socket, roomCode);
  });

  socket.on('pauseRoom', ({ roomCode }) => {
    markPlayerDisconnected(socket, roomCode);
  });

  // Handle disconnections
  socket.on('disconnect', () => {
    const roomCode = socket.roomCode;
    markPlayerDisconnected(socket, roomCode);
    console.log('User disconnected:', socket.id);
  });
});

function markPlayerDisconnected(socket, roomCode) {
  if (!roomCode || !activeRooms.has(roomCode)) return;
  const room = activeRooms.get(roomCode);
  const playerId = socket.userId;
  if (!playerId) {
    return;
  }
  const player = room.players.find(p => p.id === playerId && p.socketId === socket.id);
  if (!player || (player.socketId && player.socketId !== socket.id)) return;
  player.connected = false;
  player.socketId = null;
  persistRoomState(roomCode, room);
  socket.to(roomCode).emit('playerDisconnected', playerId);
  if (!room.players.some(p => p.connected)) {
    room.cleanupTimer = setTimeout(() => {
      if (!room.players.some(p => p.connected)) {
        activeRooms.delete(roomCode);
        Room.findOneAndUpdate({ code: roomCode }, { isActive: false })
          .catch(err => console.error('Error updating room status:', err));
      }
    }, 30 * 60 * 1000);
  }
  socket.leave(roomCode);
}

function removePlayerFromRoom(socket, roomCode) {
  if (!roomCode || !activeRooms.has(roomCode)) return;
  const room = activeRooms.get(roomCode);
  const playerId = socket.userId;
  const player = room.players.find(p => p.id === playerId && p.socketId === socket.id);
  if (!player) return;
  room.players = room.players.filter(p => p.id !== playerId);
  persistRoomState(roomCode, room);
  socket.to(roomCode).emit('playerLeft', playerId);
  socket.leave(roomCode);
  if (room.players.length === 0) {
    activeRooms.delete(roomCode);
    Room.findOneAndUpdate({ code: roomCode }, { isActive: false })
      .catch(err => console.error('Error updating room status:', err));
  }
}

// API Routes
app.get('/api/leaderboard', async (req, res) => {
  try {
    const players = await Player.find()
      .sort({ highScore: -1 })
      .limit(10);
    
    res.json(players);
  } catch (error) {
    console.error('Error fetching leaderboard:', error);
    res.status(500).json({ message: 'Failed to fetch leaderboard' });
  }
});

app.post('/api/auth/signup', async (req, res) => {
  try {
    const username = String(req.body.username || '').trim();
    const email = String(req.body.email || '').trim().toLowerCase();
    const password = String(req.body.password || '');
    if (username.length < 2 || !email.includes('@') || password.length < 6) {
      return res.status(400).json({ message: 'Username, valid email, and a 6+ character password are required' });
    }
    if (await Player.findOne({ email })) return res.status(409).json({ message: 'Email is already registered' });
    const player = await Player.create({ username, email, passwordHash: await hashPassword(password) });
    return res.status(201).json({ user: { id: player._id.toString(), username, email } });
  } catch (error) {
    console.error('Signup error:', error);
    return res.status(500).json({ message: 'Unable to create account' });
  }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const email = String(req.body.email || '').trim().toLowerCase();
    const password = String(req.body.password || '');
    const player = await Player.findOne({ email });
    if (!player || !(await verifyPassword(password, player.passwordHash))) {
      return res.status(401).json({ message: 'Invalid email or password' });
    }
    return res.json({ user: { id: player._id.toString(), username: player.username, email: player.email } });
  } catch (error) {
    console.error('Login error:', error);
    return res.status(500).json({ message: 'Unable to log in' });
  }
});

// Serve the frontend only when it has been built alongside the server.
// On Render, the frontend is deployed separately on Vercel, so this directory
// may not exist in the backend service filesystem.
const frontendDistPath = path.join(__dirname, '../quest-chronicles-unlocked/dist');
if (process.env.NODE_ENV === 'production' && fs.existsSync(path.join(frontendDistPath, 'index.html'))) {
  app.use(express.static(frontendDistPath));

  app.get('*', (_req, res) => {
    res.sendFile(path.join(frontendDistPath, 'index.html'));
  });
}

// Start server
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
