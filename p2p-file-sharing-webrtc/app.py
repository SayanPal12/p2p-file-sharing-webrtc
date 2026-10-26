"""
Peer-to-Peer File Sharing System Using WebRTC
-----------------------------------------------
Flask + Flask-SocketIO signaling server.

Responsibilities of this server:
    - Serve the web application (HTML/CSS/JS)
    - Manage rooms (create/join/leave)
    - Forward WebRTC signaling messages (SDP offer/answer, ICE candidates)
    - Handle disconnects
    - Let a peer that dropped off (network blip, Wi-Fi switch, ...) re-join its
      room right away so an interrupted file transfer can be RESUMED

This server NEVER sees or stores the actual file being transferred.
Once the WebRTC connection is established, the file travels directly
between the two browsers over an RTCDataChannel.
"""

from flask import Flask, render_template, request
from flask_socketio import SocketIO, join_room, leave_room, rooms

app = Flask(__name__)
app.config["SECRET_KEY"] = "dev-secret-key-change-me"

# threading async_mode keeps deployment simple (no eventlet/gevent needed).
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# room_id -> set of socket ids (sid) currently in that room.
# We cap each room at 2 participants since this is a 1-to-1 P2P demo.
active_rooms = {}

# sid -> client_id. A client_id is a random token the browser generates once per
# page load and sends with every join-room. If the same client comes back with a
# NEW socket id (e.g. after a network drop) while its OLD socket id is still
# lingering in the room (the server only notices a dead socket after the ping
# timeout), we recognise it by client_id and swap the stale entry out instead of
# rejecting the returning peer with "room is full". This is what makes
# resuming an interrupted transfer fast.
client_ids = {}


def _room_of(sid):
    """Return the room id a given socket is currently in, if any."""
    for room_id, members in active_rooms.items():
        if sid in members:
            return room_id
    return None


@app.route("/")
def index():
    return render_template("index.html")


@socketio.on("connect")
def handle_connect():
    print(f"[connect] {request.sid}")


@socketio.on("disconnect")
def handle_disconnect():
    sid = request.sid
    client_ids.pop(sid, None)
    room_id = _room_of(sid)
    if room_id:
        active_rooms[room_id].discard(sid)
        leave_room(room_id)
        socketio.emit("peer-left", {"sid": sid}, room=room_id)
        print(f"[disconnect] {sid} left room {room_id}")
        if not active_rooms[room_id]:
            del active_rooms[room_id]
    else:
        print(f"[disconnect] {sid} (was not in a room)")


@socketio.on("join-room")
def handle_join_room(data):
    """
    data: { "room": "ABC123", "client_id": "<random token>" }   (client_id is optional)
    """
    room_id = (data or {}).get("room", "").strip()
    client_id = (data or {}).get("client_id")
    sid = request.sid

    if not room_id:
        socketio.emit("room-error", {"message": "Room ID cannot be empty."}, room=sid)
        return

    members = active_rooms.setdefault(room_id, set())

    # Re-join support: if this same client is already in the room under an old
    # (now dead) socket id, drop that stale entry so the peer can come back.
    if client_id:
        for stale_sid in [m for m in members if m != sid and client_ids.get(m) == client_id]:
            members.discard(stale_sid)
            client_ids.pop(stale_sid, None)
            leave_room(room_id, sid=stale_sid)
            print(f"[join-room] replaced stale socket {stale_sid} for returning client in {room_id}")

    if len(members) >= 2:
        socketio.emit(
            "room-error",
            {"message": f"Room '{room_id}' is already full (max 2 peers)."},
            room=sid,
        )
        return

    is_initiator = len(members) == 0  # first person in the room initiates the offer
    members.add(sid)
    if client_id:
        client_ids[sid] = client_id
    join_room(room_id)

    print(f"[join-room] {sid} joined {room_id} (initiator={is_initiator})")

    # Tell the joining peer whether they should create the offer.
    socketio.emit(
        "joined-room",
        {"room": room_id, "initiator": is_initiator, "peers": len(members)},
        room=sid,
    )

    # Tell any existing peer that someone new arrived.
    if not is_initiator:
        socketio.emit("peer-joined", {"sid": sid}, room=room_id, include_self=False)


@socketio.on("signal")
def handle_signal(data):
    """
    Generic signaling relay. data: { "room": "...", "type": "offer|answer|ice-candidate", "payload": {...} }
    We simply forward the payload to the other peer in the room.
    """
    room_id = (data or {}).get("room")
    if not room_id or room_id not in active_rooms:
        return

    sid = request.sid
    # Forward to everyone else in the room (there's only ever one other peer).
    socketio.emit(
        "signal",
        {"type": data.get("type"), "payload": data.get("payload"), "from": sid},
        room=room_id,
        include_self=False,
    )


@socketio.on("leave-room")
def handle_leave_room(data):
    sid = request.sid
    client_ids.pop(sid, None)
    room_id = (data or {}).get("room") or _room_of(sid)
    if room_id and room_id in active_rooms:
        active_rooms[room_id].discard(sid)
        leave_room(room_id)
        socketio.emit("peer-left", {"sid": sid}, room=room_id)
        if not active_rooms[room_id]:
            del active_rooms[room_id]


if __name__ == "__main__":
    # host="0.0.0.0" so you can also test across devices on the same Wi-Fi.
    socketio.run(app, host="0.0.0.0", port=5000, debug=True, allow_unsafe_werkzeug=True)