/*
 * P2P File Sharing — client logic
 * ---------------------------------
 * 1. Connects to the Flask-SocketIO server for signaling only.
 * 2. Uses RTCPeerConnection + a public STUN server to establish a
 *    direct WebRTC connection with the other peer in the room.
 * 3. Once the RTCDataChannel is open, files are sliced into chunks,
 *    sent as raw ArrayBuffers, and reassembled on the other side.
 * 4. A tiny JSON-based application protocol (file-start / file-end)
 *    rides alongside the binary chunks so the receiver knows what
 *    it's getting and when it's done.
 * 5. INTERRUPT-BASED RESUME: if the connection drops mid-transfer, the
 *    receiver keeps every chunk it already confirmed. Both peers
 *    automatically reconnect, the receiver reports its last confirmed
 *    chunk, and the sender continues from the NEXT chunk instead of
 *    restarting from 0%.
 */

// ---------- Config ----------
const CHUNK_SIZE = 16 * 1024;          // 16 KB per chunk (safe for all browsers)
const BUFFERED_AMOUNT_LOW_THRESHOLD = 1 * 1024 * 1024; // 1 MB
const MAX_BUFFERED_AMOUNT = 8 * 1024 * 1024;           // 8 MB backpressure ceiling

// Resume / reconnection tuning
const DISCONNECT_GRACE_MS = 5000; // a transient ICE "disconnected" state is tolerated this long before we treat it as lost
const RECONNECT_DELAY_MS = 1000;  // short pause before re-establishing a lost connection
const MAX_REJOIN_ATTEMPTS = 30;   // while waiting for a stale room slot to be released

const ICE_SERVERS = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun1.l.google.com:19302" },
];

// ---------- DOM ----------
const el = (id) => document.getElementById(id);

const roomInput = el("room-input");
const joinBtn = el("join-btn");
const randomRoomBtn = el("random-room-btn");
const connectionStatus = el("connection-status");

const peerCard = el("peer-card");
const signalStatus = el("signal-status");
const webrtcStatus = el("webrtc-status");
const datachannelStatus = el("datachannel-status");

const fileCard = el("file-card");
const fileInput = el("file-input");
const fileMeta = el("file-meta");
const sendBtn = el("send-btn");
const interruptBtn = el("interrupt-btn");

const sendProgressBlock = el("send-progress-block");
const sendFilenameEl = el("send-filename");
const sendPercentEl = el("send-percent");
const sendProgressFill = el("send-progress-fill");
const sendSizeEl = el("send-size");
const sendSpeedEl = el("send-speed");

const recvProgressBlock = el("recv-progress-block");
const recvFilenameEl = el("recv-filename");
const recvPercentEl = el("recv-percent");
const recvProgressFill = el("recv-progress-fill");
const recvSizeEl = el("recv-size");
const recvSpeedEl = el("recv-speed");

const receivedFilesEl = el("received-files");
const logEl = el("log");

// ---------- State ----------
let socket = null;
let roomId = null;
let isInitiator = false;
let pc = null;            // RTCPeerConnection
let dataChannel = null;   // RTCDataChannel
let selectedFile = null;

let incoming = null; // { name, size, mimeType, id, receivedBytes, chunks: [], startTime, expectedHash, interrupted }

// ---- Resume state ----
// Random token identifying THIS browser tab to the server, so that when we come
// back after a network drop the server can replace our old, dead socket.
const clientId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
let outgoing = null;              // sender side: { id, file, name, size, mimeType, checksum, totalChunks, nextChunk, status }
                                  //   status: "sending" | "interrupted" | "awaiting-confirm"
const completedIds = new Map();   // receiver side: transfer id -> verified flag, for transfers already fully received
let connGen = 0;                  // bumps every time the peer connection is replaced; lets stale callbacks/loops detect they are obsolete
let pcHasConnected = false;       // has the CURRENT peer connection ever reached "connected"?
let peerPresent = false;          // is the other peer currently in the room (according to the server)?
let hasJoined = false;            // have we successfully joined a room at least once?
let rejoinAttempts = 0;
let disconnectTimer = null;
let recoverTimer = null;
let pendingDrainResolve = null;   // lets us abort a send loop that is waiting for the buffer to drain

// ---------- Logging ----------
function log(message, kind = "") {
  const line = document.createElement("div");
  if (kind) line.className = `log-${kind}`;
  const time = new Date().toLocaleTimeString();
  line.textContent = `[${time}] ${message}`;
  logEl.appendChild(line);
  logEl.scrollTop = logEl.scrollHeight;
}

function setStatus(elm, text, level) {
  elm.textContent = text;
  elm.className = `status status-${level}`;
}

function formatBytes(bytes) {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
}

function randomRoomId() {
  return Math.random().toString(36).substring(2, 8).toUpperCase();
}

randomRoomBtn.addEventListener("click", () => {
  roomInput.value = randomRoomId();
});

// =====================================================================
// STEP 1: Connect to signaling server & join room
// =====================================================================

function connectSocket() {
  if (socket) return;
  socket = io();

  socket.on("connect", () => {
    setStatus(connectionStatus, "Connected to server", "connected");
    setStatus(signalStatus, "Connected", "connected");
    log("Connected to signaling server.", "ok");

    // RESUME: we lost the signaling connection earlier and it has come back
    // (socket.io reconnects automatically). Our old socket id is gone, so
    // re-join the room; the server recognises us by clientId.
    if (hasJoined && roomId) {
      log(`Signaling reconnected — re-joining room "${roomId}"…`);
      socket.emit("join-room", { room: roomId, client_id: clientId });
    }
  });

  socket.on("disconnect", () => {
    setStatus(connectionStatus, "Disconnected", "disconnected");
    setStatus(signalStatus, "Disconnected", "disconnected");
    log("Disconnected from signaling server.", "error");
  });

  socket.on("room-error", (data) => {
    log(`Room error: ${data.message}`, "error");
    if (hasJoined && roomId) {
      // We are only trying to get BACK into our room after a drop; the old slot
      // may not have been released yet. Retry quietly instead of popping an alert.
      if (rejoinAttempts++ < MAX_REJOIN_ATTEMPTS) {
        setTimeout(() => {
          if (socket.connected) socket.emit("join-room", { room: roomId, client_id: clientId });
        }, 2000);
      }
      return;
    }
    alert(data.message);
  });

  socket.on("joined-room", (data) => {
    isInitiator = data.initiator;
    peerPresent = data.peers > 1;
    hasJoined = true;
    rejoinAttempts = 0;
    log(`Joined room "${data.room}" as ${isInitiator ? "initiator" : "receiver-side peer"}.`, "ok");
    peerCard.hidden = false;

    teardownConnection(); // re-joining after a drop: discard any stale connection first
    setupPeerConnection();

    if (isInitiator) {
      // Wait for the second peer; nothing to do yet.
      setStatus(webrtcStatus, "Waiting for peer…", "connecting");
    } else {
      setStatus(webrtcStatus, "Peer found, negotiating…", "connecting");
    }
  });

  socket.on("peer-joined", async () => {
    peerPresent = true;
    log("A peer joined the room. Creating WebRTC offer…");
    setStatus(webrtcStatus, "Peer found, negotiating…", "connecting");

    // First-time join: our connection is brand new and can be used as is.
    // Returning peer (resume): our old connection is dead, so build a fresh one.
    const fresh = pc && isInitiator && dataChannel && !pcHasConnected && !pc.localDescription;
    if (!fresh) {
      teardownConnection();
      isInitiator = true; // the peer that was already in the room is the one that offers
      setupPeerConnection();
    }
    await createOffer();
  });

  socket.on("peer-left", () => {
    log("The other peer left the room.", "error");
    setStatus(webrtcStatus, "Peer disconnected", "disconnected");
    setStatus(datachannelStatus, "Closed", "disconnected");
    sendBtn.disabled = true;
    peerPresent = false;
    // Any transfer in progress is now INTERRUPTED (not cancelled): its state is
    // kept so it can resume when the peer comes back.
    teardownConnection();
  });

  socket.on("signal", async (data) => {
    const { type, payload } = data;
    if (type === "offer") {
      await handleOffer(payload);
    } else if (type === "answer") {
      await handleAnswer(payload);
    } else if (type === "ice-candidate") {
      await handleRemoteIceCandidate(payload);
    } else if (type === "reconnect-request") {
      await handleReconnectRequest();
    }
  });
}

joinBtn.addEventListener("click", () => {
  const room = roomInput.value.trim();
  if (!room) {
    alert("Please enter a room ID.");
    return;
  }
  connectSocket();
  roomId = room;

  // If socket isn't connected yet, wait for it.
  if (socket.connected) {
    socket.emit("join-room", { room, client_id: clientId });
  } else {
    socket.once("connect", () => socket.emit("join-room", { room, client_id: clientId }));
  }

  joinBtn.disabled = true;
  roomInput.disabled = true;
});

// =====================================================================
// STEP 2: WebRTC peer connection + signaling
// =====================================================================

function sendSignal(type, payload) {
  // Only talk to the signaling server while it is actually connected; socket.io
  // would otherwise queue the message and replay it later, when it is stale.
  if (!socket || !socket.connected) return false;
  socket.emit("signal", { room: roomId, type, payload });
  return true;
}

function setupPeerConnection() {
  const gen = ++connGen; // every callback below checks this, so callbacks of a replaced connection do nothing
  pcHasConnected = false;
  pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

  pc.onicecandidate = (event) => {
    if (gen !== connGen) return;
    if (event.candidate) {
      sendSignal("ice-candidate", event.candidate);
    }
  };

  pc.onconnectionstatechange = () => {
    if (gen !== connGen) return;
    log(`WebRTC connection state: ${pc.connectionState}`);
    if (pc.connectionState === "connected") {
      pcHasConnected = true;
      clearTimeout(disconnectTimer);
      setStatus(webrtcStatus, "Connected", "connected");
    } else if (["disconnected", "failed", "closed"].includes(pc.connectionState)) {
      setStatus(webrtcStatus, pc.connectionState, "disconnected");
      if (pc.connectionState === "failed") {
        handleConnectionLoss("WebRTC connection failed", gen);
      } else if (pc.connectionState === "disconnected") {
        // "disconnected" can heal by itself; give it a few seconds before giving up.
        clearTimeout(disconnectTimer);
        disconnectTimer = setTimeout(() => {
          if (gen === connGen && pc && pc.connectionState === "disconnected") {
            handleConnectionLoss("WebRTC connection stayed disconnected", gen);
          }
        }, DISCONNECT_GRACE_MS);
      }
    } else {
      setStatus(webrtcStatus, pc.connectionState, "connecting");
    }
  };

  if (isInitiator) {
    // The initiator creates the data channel; the other side receives it via ondatachannel.
    dataChannel = pc.createDataChannel("fileTransfer", { ordered: true });
    setupDataChannel();
  } else {
    pc.ondatachannel = (event) => {
      dataChannel = event.channel;
      setupDataChannel();
    };
  }
}

async function createOffer() {
  const gen = connGen;
  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  if (gen !== connGen) return; // connection was replaced while we were negotiating
  sendSignal("offer", offer);
}

async function handleOffer(offer) {
  // An offer arriving on a connection that was already used (or is mid-negotiation)
  // means the peer re-established its side: start from a clean connection.
  if (!pc || pcHasConnected || pc.signalingState !== "stable") {
    teardownConnection();
    isInitiator = false; // the sender of an offer is the initiator; we answer
    setupPeerConnection();
  }
  const gen = connGen;
  await pc.setRemoteDescription(new RTCSessionDescription(offer));
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);
  if (gen !== connGen) return;
  sendSignal("answer", answer);
}

async function handleAnswer(answer) {
  if (!pc || pc.signalingState !== "have-local-offer") return; // stale answer from a replaced connection
  await pc.setRemoteDescription(new RTCSessionDescription(answer));
}

async function handleRemoteIceCandidate(candidate) {
  if (!pc) return;
  try {
    await pc.addIceCandidate(new RTCIceCandidate(candidate));
  } catch (err) {
    log(`Error adding ICE candidate: ${err.message}`, "error");
  }
}

// =====================================================================
// STEP 3: Data channel + file transfer protocol
// =====================================================================

function setupDataChannel() {
  const gen = connGen;
  dataChannel.binaryType = "arraybuffer";
  dataChannel.bufferedAmountLowThreshold = BUFFERED_AMOUNT_LOW_THRESHOLD;

  dataChannel.onopen = () => {
    if (gen !== connGen) return;
    setStatus(datachannelStatus, "Open", "connected");
    log("Data channel open. Ready to send files.", "ok");
    fileCard.hidden = false;
    interruptBtn.disabled = false;
    if (selectedFile && !isSending()) sendBtn.disabled = false;
    offerResumeIfNeeded(); // RESUME: if a transfer was interrupted, negotiate where to continue
  };

  dataChannel.onclose = () => {
    if (gen !== connGen) return;
    setStatus(datachannelStatus, "Closed", "disconnected");
    sendBtn.disabled = true;
    handleConnectionLoss("data channel closed", gen);
  };

  dataChannel.onerror = (err) => {
    if (gen !== connGen) return;
    log(`Data channel error: ${err.message || err}`, "error");
  };

  dataChannel.onmessage = (event) => {
    if (gen !== connGen) return;
    handleIncomingMessage(event.data);
  };
}

// =====================================================================
// RESUME: connection-loss handling & automatic reconnection
// =====================================================================

function isSending() {
  return !!outgoing && (outgoing.status === "sending" || outgoing.status === "interrupted");
}

function setNote(elm, text) {
  elm.textContent = text;
  elm.classList.add("note-warn");
}

// Marks any transfer that is in flight as INTERRUPTED. Nothing is discarded:
// the receiver keeps the chunks it already has and the sender keeps its file
// handle, so the transfer can continue from where it stopped.
function markTransfersInterrupted() {
  if (outgoing && outgoing.status === "sending") {
    outgoing.status = "interrupted";
    log("Transfer interrupted — it will resume automatically once reconnected.", "error");
    setNote(sendSpeedEl, "⚠ Interrupted — reconnecting…");
  }
  if (incoming && !incoming.interrupted) {
    incoming.interrupted = true;
    const confirmed = incoming.chunks.length;
    const percent = incoming.size ? Math.floor((incoming.receivedBytes / incoming.size) * 100) : 0;
    log(`Connection lost at ${percent}% — last confirmed chunk: ${confirmed}. Progress is saved.`, "error");
    setNote(recvSpeedEl, "⚠ Interrupted — waiting to resume…");
  }
}

// Tears down the current RTCPeerConnection/RTCDataChannel without triggering
// any of their callbacks, and aborts a send loop that may be waiting on them.
function teardownConnection() {
  connGen++;
  clearTimeout(disconnectTimer);
  if (pendingDrainResolve) pendingDrainResolve();
  markTransfersInterrupted();

  if (dataChannel) {
    dataChannel.onopen = dataChannel.onclose = dataChannel.onerror = dataChannel.onmessage = null;
    try { dataChannel.close(); } catch { /* already closed */ }
    dataChannel = null;
  }
  if (pc) {
    pc.onicecandidate = pc.onconnectionstatechange = pc.ondatachannel = null;
    try { pc.close(); } catch { /* already closed */ }
    pc = null;
  }
  pcHasConnected = false;
  sendBtn.disabled = true;
  interruptBtn.disabled = true;
}

function handleConnectionLoss(reason, gen) {
  if (gen !== connGen) return; // already handled, or the connection was already replaced
  log(`Connection lost (${reason}).`, "error");
  setStatus(webrtcStatus, "Connection lost — reconnecting…", "connecting");
  setStatus(datachannelStatus, "Closed", "disconnected");
  teardownConnection();
  scheduleRecovery();
}

function scheduleRecovery() {
  clearTimeout(recoverTimer);
  if (!roomId) return;
  recoverTimer = setTimeout(recoverConnection, RECONNECT_DELAY_MS);
}

// Re-establish the peer connection. Roles stay deterministic: the initiator
// makes the offer; the other side builds a fresh connection and asks for one.
async function recoverConnection() {
  if (pc) return; // something else already rebuilt it
  if (!socket || !socket.connected) {
    log("Waiting for the signaling server to reconnect…");
    return; // the socket "connect" handler re-joins the room, which restarts negotiation
  }
  if (!peerPresent) {
    log("Waiting for the other peer to come back…");
    return; // the "peer-joined" / "joined-room" handlers take over when it does
  }
  log("Re-establishing the connection…");
  setStatus(webrtcStatus, "Reconnecting…", "connecting");
  setupPeerConnection();
  if (isInitiator) {
    await createOffer();
  } else {
    sendSignal("reconnect-request");
  }
}

async function handleReconnectRequest() {
  if (!isInitiator) return;
  // We already have a fresh offer out; nothing more to do.
  if (pc && !pcHasConnected && pc.signalingState === "have-local-offer") return;
  log("Peer asked to re-establish the connection…");
  teardownConnection();
  setupPeerConnection();
  await createOffer();
}

// Demo helper: drop the connection on purpose, exactly like a real network failure would.
interruptBtn.addEventListener("click", () => {
  if (!dataChannel || dataChannel.readyState !== "open") return;
  log("Simulating a connection interruption…", "error");
  const gen = connGen;
  try { dataChannel.close(); } catch { /* ignore */ }
  handleConnectionLoss("simulated interruption", gen);
});

// =====================================================================
// RESUME: handshake after the data channel re-opens
// =====================================================================
//   sender   --resume-offer {id, name, size, checksum, ...}-->  receiver
//   receiver --resume-from  {id, nextChunk}------------------>  sender
//   sender continues with chunk number `nextChunk` (0-based), then file-end.
//   receiver --file-received {id, verified}----------------->  sender (when complete)

function sendControl(obj) {
  if (dataChannel && dataChannel.readyState === "open") {
    dataChannel.send(JSON.stringify(obj));
    return true;
  }
  return false;
}

function offerResumeIfNeeded() {
  if (!outgoing || outgoing.status === "sending") return;
  log("Connection re-established — negotiating resume point…");
  sendControl({
    type: "resume-offer",
    id: outgoing.id,
    name: outgoing.name,
    size: outgoing.size,
    mimeType: outgoing.mimeType,
    checksum: outgoing.checksum,
    chunkSize: CHUNK_SIZE,
    totalChunks: outgoing.totalChunks,
  });
}

// Receiver: the sender wants to continue transfer `msg.id`. Say how much we already hold.
function handleResumeOffer(msg) {
  if (completedIds.has(msg.id)) {
    // We already have the whole file; our confirmation was probably lost in the drop.
    sendControl({ type: "file-received", id: msg.id, verified: completedIds.get(msg.id) });
    return;
  }
  if (incoming && incoming.id === msg.id && incoming.finalizing) return; // already complete; file-received follows shortly
  if (!incoming || incoming.id !== msg.id) {
    // We hold nothing for this transfer: start it from the beginning.
    startIncoming(msg);
    log("No saved progress for this transfer — starting from chunk 1.");
  }
  incoming.interrupted = false;

  const confirmed = incoming.chunks.length; // chunks are contiguous, so this is the last confirmed chunk number
  const percent = incoming.size ? Math.floor((incoming.receivedBytes / incoming.size) * 100) : 0;
  recvPercentEl.textContent = `${percent}%`;
  recvProgressFill.style.width = `${percent}%`;
  recvSizeEl.textContent = `${formatBytes(incoming.receivedBytes)} / ${formatBytes(incoming.size)}`;
  recvSpeedEl.textContent = "Resuming…";
  recvSpeedEl.classList.remove("note-warn");
  incoming.lastUpdate = performance.now();
  incoming.lastBytes = incoming.receivedBytes;

  log(`Last confirmed chunk: ${confirmed}. Asking sender to resume from chunk ${confirmed + 1}.`, "ok");
  sendControl({ type: "resume-from", id: msg.id, nextChunk: confirmed });
}

// Sender: the receiver told us where to continue.
function handleResumeFrom(msg) {
  if (!outgoing || outgoing.id !== msg.id) return;
  if (outgoing.status === "sending") return; // duplicate answer; already running

  const next = Math.max(0, Math.min(Number(msg.nextChunk) || 0, outgoing.totalChunks));
  outgoing.nextChunk = next; // rewind/advance to exactly what the receiver confirmed
  log(`Receiver confirmed chunk ${next}. Resuming "${outgoing.name}" from chunk ${next + 1} of ${outgoing.totalChunks}.`, "ok");

  const offset = Math.min(next * CHUNK_SIZE, outgoing.size);
  const percent = outgoing.size ? Math.floor((offset / outgoing.size) * 100) : 0;
  sendPercentEl.textContent = `${percent}%`;
  sendProgressFill.style.width = `${percent}%`;
  sendSizeEl.textContent = `${formatBytes(offset)} / ${formatBytes(outgoing.size)}`;
  sendSpeedEl.textContent = "Resuming…";
  sendSpeedEl.classList.remove("note-warn");
  sendProgressBlock.hidden = false;
  sendBtn.disabled = true;

  pumpOutgoing(outgoing).catch((err) => {
    log(`Error sending file: ${err.message}`, "error");
    sendBtn.disabled = false;
  });
}

// Sender: the receiver has the complete file.
function handleFileReceived(msg) {
  if (!outgoing || outgoing.id !== msg.id) return;
  log(`Receiver confirmed "${outgoing.name}" arrived complete${msg.verified === true ? " (integrity verified)" : ""}.`, "ok");
  outgoing = null;
}

// ---- Sending ----

fileInput.addEventListener("change", () => {
  selectedFile = fileInput.files[0] || null;
  if (selectedFile) {
    fileMeta.textContent = `${selectedFile.name} — ${formatBytes(selectedFile.size)}`;
    sendBtn.disabled = !(dataChannel && dataChannel.readyState === "open");
  } else {
    fileMeta.textContent = "";
    sendBtn.disabled = true;
  }
});

sendBtn.addEventListener("click", () => {
  if (selectedFile && dataChannel && dataChannel.readyState === "open") {
    sendFile(selectedFile);
  }
});

async function sha256Hex(arrayBuffer) {
  const hashBuffer = await crypto.subtle.digest("SHA-256", arrayBuffer);
  return Array.from(new Uint8Array(hashBuffer))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

async function sendFile(file) {
  sendBtn.disabled = true;
  const transferId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  sendProgressBlock.hidden = false;
  sendFilenameEl.textContent = file.name;
  sendPercentEl.textContent = "0%";
  sendProgressFill.style.width = "0%";
  sendSizeEl.textContent = `0 / ${formatBytes(file.size)}`;
  sendSpeedEl.textContent = "";
  sendSpeedEl.classList.remove("note-warn");

  // Compute a checksum up front so the receiver can verify integrity.
  // For very large files this costs one extra full read; acceptable for a demo.
  let checksum = null;
  try {
    const wholeBuffer = await file.arrayBuffer();
    checksum = await sha256Hex(wholeBuffer);
  } catch (err) {
    log(`Could not compute checksum (continuing without it): ${err.message}`, "error");
  }

  // Everything needed to continue this transfer later lives in `outgoing`.
  const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
  const transfer = {
    id: transferId,
    file,
    name: file.name,
    size: file.size,
    mimeType: file.type || "application/octet-stream",
    checksum,
    totalChunks,
    nextChunk: 0, // index of the next chunk to send; everything before it has been sent
    status: "sending",
  };
  outgoing = transfer;

  if (!dataChannel || dataChannel.readyState !== "open") {
    // The connection dropped before we could even start; the resume handshake
    // will start the transfer as soon as we are reconnected.
    transfer.status = "interrupted";
    log("Connection dropped before the transfer started — it will start once reconnected.", "error");
    setNote(sendSpeedEl, "⚠ Interrupted — reconnecting…");
    return;
  }

  dataChannel.send(
    JSON.stringify({
      type: "file-start",
      id: transferId,
      name: file.name,
      size: file.size,
      mimeType: transfer.mimeType,
      checksum,
      chunkSize: CHUNK_SIZE,
      totalChunks,
    })
  );

  pumpOutgoing(transfer).catch((err) => {
    log(`Error sending file: ${err.message}`, "error");
    sendBtn.disabled = false;
  });
}

// Waits until the data channel's send buffer has drained. Also returns if the
// connection is torn down meanwhile (see teardownConnection), so the send loop
// can never hang on a dead channel.
function waitForDrain(dc) {
  return new Promise((resolve) => {
    const done = () => {
      dc.removeEventListener("bufferedamountlow", done);
      dc.removeEventListener("close", done);
      if (pendingDrainResolve === done) pendingDrainResolve = null;
      resolve();
    };
    pendingDrainResolve = done;
    dc.addEventListener("bufferedamountlow", done);
    dc.addEventListener("close", done);
  });
}

// Sends chunks `transfer.nextChunk … totalChunks-1`, then file-end. Used both for
// the first attempt and for every resume. If the connection is replaced while it
// runs, it simply stops; `transfer.nextChunk` is then corrected by the resume handshake.
async function pumpOutgoing(transfer) {
  const gen = connGen;
  const dc = dataChannel;
  const { file } = transfer;
  transfer.status = "sending";

  const stale = () => gen !== connGen || outgoing !== transfer;

  const startTime = performance.now();
  let lastUpdate = startTime;
  let lastOffset = Math.min(transfer.nextChunk * CHUNK_SIZE, file.size);

  try {
    while (transfer.nextChunk < transfer.totalChunks) {
      // Backpressure: pause sending if the send buffer is too full.
      if (dc.bufferedAmount > MAX_BUFFERED_AMOUNT) {
        await waitForDrain(dc);
        if (stale()) return;
      }

      const offsetBefore = transfer.nextChunk * CHUNK_SIZE;
      const slice = file.slice(offsetBefore, offsetBefore + CHUNK_SIZE);
      const buffer = await slice.arrayBuffer();
      if (stale()) return;

      dc.send(buffer);
      transfer.nextChunk++;
      const offset = offsetBefore + buffer.byteLength;

      const now = performance.now();
      if (now - lastUpdate > 150 || offset === file.size) {
        const percent = Math.floor((offset / file.size) * 100);
        const elapsedSec = (now - lastUpdate) / 1000;
        const speed = elapsedSec > 0 ? (offset - lastOffset) / elapsedSec : 0;

        sendPercentEl.textContent = `${percent}%`;
        sendProgressFill.style.width = `${percent}%`;
        sendSizeEl.textContent = `${formatBytes(offset)} / ${formatBytes(file.size)}`;
        sendSpeedEl.textContent = `${formatBytes(speed)}/s`;
        sendSpeedEl.classList.remove("note-warn");

        lastUpdate = now;
        lastOffset = offset;
      }
    }

    if (stale()) return;
    dc.send(JSON.stringify({ type: "file-end", id: transfer.id }));
    // Keep the transfer around until the receiver confirms it is complete, so that
    // a drop right at the very end can still be resumed.
    transfer.status = "awaiting-confirm";
    log(`Sent "${file.name}" (${formatBytes(file.size)}).`, "ok");
    sendBtn.disabled = false;
  } catch (err) {
    // If the channel died under us, the close/failure handlers deal with it and the
    // transfer resumes after reconnecting. Anything else is a genuine error.
    if (stale() || dc.readyState !== "open") return;
    throw err;
  }
}

// ---- Receiving ----

// Starts receiving a new transfer (also used when a resume is offered for a
// transfer we hold nothing of).
function startIncoming(msg) {
  incoming = {
    id: msg.id,
    name: msg.name,
    size: msg.size,
    mimeType: msg.mimeType,
    checksum: msg.checksum || null,
    chunks: [],
    receivedBytes: 0,
    startTime: performance.now(),
    lastUpdate: performance.now(),
    lastBytes: 0,
    interrupted: false,
  };
  recvProgressBlock.hidden = false;
  recvFilenameEl.textContent = msg.name;
  recvPercentEl.textContent = "0%";
  recvProgressFill.style.width = "0%";
  recvSizeEl.textContent = `0 / ${formatBytes(msg.size)}`;
  recvSpeedEl.textContent = "";
  recvSpeedEl.classList.remove("note-warn");
  log(`Incoming file: "${msg.name}" (${formatBytes(msg.size)}).`);
}

function handleIncomingMessage(data) {
  if (typeof data === "string") {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch {
      log("Received malformed control message.", "error");
      return;
    }

    if (msg.type === "file-start") {
      startIncoming(msg);
    } else if (msg.type === "file-end") {
      finalizeIncomingFile();
    } else if (msg.type === "resume-offer") {
      handleResumeOffer(msg);
    } else if (msg.type === "resume-from") {
      handleResumeFrom(msg);
    } else if (msg.type === "file-received") {
      handleFileReceived(msg);
    }
    return;
  }

  // Binary chunk (ArrayBuffer)
  if (!incoming) {
    log("Received a data chunk with no active transfer — ignoring.", "error");
    return;
  }

  incoming.chunks.push(data);
  incoming.receivedBytes += data.byteLength;

  const now = performance.now();
  if (now - incoming.lastUpdate > 150 || incoming.receivedBytes === incoming.size) {
    const percent = Math.floor((incoming.receivedBytes / incoming.size) * 100);
    const elapsedSec = (now - incoming.lastUpdate) / 1000;
    const speed = elapsedSec > 0 ? (incoming.receivedBytes - incoming.lastBytes) / elapsedSec : 0;

    recvPercentEl.textContent = `${percent}%`;
    recvProgressFill.style.width = `${percent}%`;
    recvSizeEl.textContent = `${formatBytes(incoming.receivedBytes)} / ${formatBytes(incoming.size)}`;
    recvSpeedEl.textContent = `${formatBytes(speed)}/s`;
    recvSpeedEl.classList.remove("note-warn");

    incoming.lastUpdate = now;
    incoming.lastBytes = incoming.receivedBytes;
  }
}

async function finalizeIncomingFile() {
  if (!incoming || incoming.finalizing) return; // nothing to finish, or already finishing (e.g. duplicate file-end)
  incoming.finalizing = true;
  const { id, name, mimeType, chunks, checksum } = incoming;

  const blob = new Blob(chunks, { type: mimeType });
  let verified = null;

  if (checksum) {
    try {
      const buffer = await blob.arrayBuffer();
      const actualHash = await sha256Hex(buffer);
      verified = actualHash === checksum;
    } catch (err) {
      log(`Checksum verification failed to run: ${err.message}`, "error");
    }
  }

  const url = URL.createObjectURL(blob);

  const row = document.createElement("div");
  row.className = "received-file";

  const label = document.createElement("span");
  let verifyText = "";
  if (verified === true) verifyText = " ✅ verified";
  else if (verified === false) verifyText = " ⚠️ checksum mismatch";
  label.textContent = `${name} (${formatBytes(blob.size)})${verifyText}`;

  const link = document.createElement("a");
  link.href = url;
  link.download = name;
  link.textContent = "Download";

  row.appendChild(label);
  row.appendChild(link);
  receivedFilesEl.prepend(row);

  log(
    `File "${name}" received and ready to download.${
      verified === true ? " Integrity verified (SHA-256)." : verified === false ? " WARNING: integrity check failed." : ""
    }`,
    verified === false ? "error" : "ok"
  );

  // Tell the sender the file is complete, and remember it so a late resume
  // request for the same transfer is answered instead of downloading it twice.
  completedIds.set(id, verified);
  sendControl({ type: "file-received", id, verified });

  incoming = null;
}