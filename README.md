# Peer-to-Peer File Sharing with WebRTC

A browser-based peer-to-peer file-sharing application built with **WebRTC**, **Flask**, and **Flask-SocketIO**. It transfers files directly between browsers through a WebRTC data channel, with chunked sending, SHA-256 integrity verification, backpressure handling, and in-memory resume after temporary connection interruptions.

> **Architecture in one line:** Flask-SocketIO coordinates the peers and relays WebRTC signaling; once a data channel is established, file bytes are sent browser-to-browser rather than through the Flask server.

## Features

- **Peer-to-peer transfer:** uses `RTCPeerConnection` and `RTCDataChannel` for browser-to-browser data transfer.
- **Room-based pairing:** two clients join the same room; the server limits a room to two participants.
- **WebRTC signaling:** exchanges SDP offers/answers and ICE candidates over Socket.IO.
- **Chunked file transfer:** slices the selected file into 16 KiB application chunks.
- **Backpressure:** monitors `RTCDataChannel.bufferedAmount` and pauses sending when the outgoing queue grows too large.
- **Integrity verification:** calculates a SHA-256 checksum and verifies the reconstructed file at the receiver.
- **Connection recovery and resume:** when the pages remain open, a reconnect can establish a new WebRTC connection and negotiate a restart position based on the receiver's retained chunk count.
- **Transfer status and logs:** shows connection/transfer state, progress and diagnostic messages in the browser.

## Technology stack

| Technology | Purpose |
|---|---|
| Python + Flask | Serves the web application |
| Flask-SocketIO / Socket.IO | Room coordination and signaling events |
| JavaScript | Browser-side connection and file-transfer logic |
| WebRTC `RTCPeerConnection` | Negotiates the peer connection |
| WebRTC `RTCDataChannel` | Carries binary chunks and JSON control messages |
| ICE + STUN | Discovers/tests connectivity candidates for a usable path |
| SCTP over DTLS | Data-channel transport and encrypted transport protection |
| Web Crypto API | SHA-256 integrity check |

## Project structure

```text
p2p-file-sharing-webrtc/
├── app.py                  # Flask app and Socket.IO signaling server
├── requirements.txt        # Python dependencies
├── templates/
│   └── index.html          # User interface
└── static/
    ├── css/
    │   └── style.css       # Styling
    └── js/
        └── app.js          # WebRTC setup, chunk transfer and resume logic
```

## Run locally

Python 3.10+ is recommended.

```bash
git clone https://github.com/<your-username>/p2p-file-sharing-webrtc.git
cd p2p-file-sharing-webrtc

python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Open `http://localhost:5000` in two browser tabs, enter the same room ID in both, and join from each tab. Select a file in one tab and send it to the other.

To test on two devices on the same Wi-Fi, run the server on one machine and open `http://<server-lan-ip>:5000` from both devices. Ensure the host firewall allows incoming connections to port `5000` on the local network.

## How the connection is established

1. Peer A joins a room and becomes the initial offerer.
2. Peer B joins the same room; the server notifies the existing peer.
3. A creates an SDP offer and relays it to B through Socket.IO.
4. B applies the offer, creates an SDP answer and sends it back through Socket.IO.
5. The peers exchange ICE candidates through Socket.IO. ICE connectivity checks look for a usable endpoint pair.
6. WebRTC establishes its transport and opens the data channel.
7. File bytes are then transferred through `RTCDataChannel`. Flask-SocketIO remains responsible for signaling, not the file payload.

## File-transfer protocol

The application uses small JSON messages for transfer control and binary `ArrayBuffer` messages for file data. The control flow includes message types such as `file-start`, `file-end`, and `file-received`. The sender provides metadata including the transfer ID, filename, MIME type, size, chunk size, total chunk count and SHA-256 checksum.

The receiver accumulates chunks in order, builds a `Blob` after `file-end`, checks the resulting SHA-256 hash against the sender's value, and exposes a download link. A checksum detects content mismatches; it is not a user-authentication mechanism.

### Chunking and backpressure

The application uses 16 KiB chunks. This is application-level chunking to track progress and choose a resume offset; the SCTP/DTLS/IP layers may perform their own fragmentation or packetization as required.

The sender monitors `RTCDataChannel.bufferedAmount`. When its outgoing queue exceeds the configured 8 MiB high-water mark, sending waits until `bufferedamountlow` indicates that the queue has drained to the configured 1 MiB threshold. This prevents unbounded growth of the sender's outgoing queue.

## Resume after connection interruption

The current implementation supports connection-level recovery **while the relevant browser pages remain open**. The transfer state is held in JavaScript memory, not persistent storage.

1. A data-channel or peer-connection failure triggers connection-loss handling. A temporary ICE `disconnected` state is given a short grace period before being treated as a loss.
2. The old `RTCPeerConnection` and data channel are torn down. A connection-generation counter (`connGen`) helps stale callbacks and asynchronous sending loops detect that their connection is obsolete.
3. Socket.IO reconnects when possible and the page rejoins its room with a page-level client ID. The server can replace a stale room entry for that same client.
4. The peers negotiate a fresh WebRTC connection and open a new data channel.
5. The sender sends `resume-offer` with transfer metadata. The receiver reports `resume-from` with its saved chunk count.
6. The sender computes the byte offset from that chunk index and sends the remaining data. The receiver reconstructs and verifies the completed file, then sends `file-received`.

The receiver's chunk count is used at resume time because the sender may have queued data that had not reached the receiver when the old connection failed. The implementation does not send a separate application-level acknowledgement for every chunk; the receiver's saved count is reconciled during the resume handshake.

### Resume limitations

- Refreshing or closing the receiver page clears its in-memory chunks, so transfer progress cannot be restored after a page reload.
- Refreshing or closing the sender page loses the in-memory `File` reference and transfer state.
- The receiver stores chunks in memory until reconstruction, and checksum verification reads the reassembled content. Very large files may use substantial browser memory.
- The application is designed for one active transfer at a time; concurrent transfers are not robustly isolated.
- A checksum mismatch is reported but does not currently trigger automatic retransmission/repair.

Persistent resume across page refreshes would require storing receiver chunks and transfer metadata in browser storage such as IndexedDB and restoring access to the sender's original file.

## Networking notes and known limitations

- The application configures STUN but does **not** configure a TURN relay. Some restrictive NATs/firewalls may prevent direct connectivity. A TURN service can be added as a relay fallback.
- Exposing the Flask signaling server through a tunnel makes the signaling URL reachable; it does not guarantee that a WebRTC path will be established.
- The current source is a classroom/demo implementation, not a production-hardened service. It uses permissive CORS, debug mode, a development secret key, and does not implement user authentication or strong room authorization. Do not expose it publicly without hardening these settings.
- The room registry is stored in process memory, so multi-process or multi-server deployments would need shared coordination.

## Demo checklist

1. Open the app in two tabs or two devices and join the same room.
2. Select a sufficiently large test file and start a transfer.
3. While it is in progress, use **Simulate Interruption** or briefly disconnect one device's network. Keep both pages open.
4. Observe signaling reconnection, the new WebRTC data channel, the resume handshake, and continued transfer.
5. Confirm the receiver completes checksum verification and can download the file.

The simulated interruption closes the data channel intentionally. It demonstrates the recovery logic, but it is not identical to every type of real network failure. Test the real-network scenario separately before relying on it in a presentation.

## Possible improvements

- Configure TURN fallback for restrictive networks.
- Persist receiver chunks and transfer metadata in IndexedDB for resume after refresh.
- Enforce a single active transfer or implement a queue keyed by transfer ID.
- Add authentication, room authorization, secure random IDs, stricter signaling validation and production-safe configuration.
- Stream hashing and storage to reduce peak memory use for large files.
- Add automated tests for signaling, interruption and resume edge cases.
