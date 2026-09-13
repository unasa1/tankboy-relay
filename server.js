// TankBoy 실시간 대전용 릴레이 서버.
// 게임 로직(물리, 데미지, 지형)은 전혀 모른다 — 두 플레이어를 짝지어주고(매칭),
// 짝이 된 방(room) 안에서 한쪽이 보낸 메시지를 그대로 다른 쪽에게 전달만 하는
// "그냥 전달자(dumb relay)"다. 실제 판정은 두 클라이언트(Godot)가 알아서 한다.
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// 매칭 대기열: 아직 상대를 못 찾은 소켓들.
let queue = [];
// 짝이 된 소켓 -> { opponent: WebSocket, roomId: string }
const rooms = new Map();

function send(ws, obj) {
	if (ws && ws.readyState === WebSocket.OPEN) {
		ws.send(JSON.stringify(obj));
	}
}

function leaveRoom(ws, notifyOpponent) {
	const info = rooms.get(ws);
	if (info) {
		rooms.delete(ws);
		if (notifyOpponent) {
			const oppInfo = rooms.get(info.opponent);
			if (oppInfo && oppInfo.opponent === ws) {
				rooms.delete(info.opponent);
			}
			send(info.opponent, { type: "opponent_left" });
		}
	}
}

wss.on("connection", (ws) => {
	ws.isAlive = true;
	ws.on("pong", () => {
		ws.isAlive = true;
	});

	ws.on("message", (raw) => {
		let msg;
		try {
			msg = JSON.parse(raw.toString());
		} catch (e) {
			return;
		}
		if (!msg || typeof msg.type !== "string") return;

		if (msg.type === "find_match") {
			queue = queue.filter((s) => s !== ws && s.readyState === WebSocket.OPEN);
			if (queue.length > 0) {
				const opponent = queue.shift();
				const roomId = Math.random().toString(36).slice(2);
				rooms.set(ws, { opponent: opponent, roomId: roomId });
				rooms.set(opponent, { opponent: ws, roomId: roomId });
				// 먼저 대기열에 있던 쪽(opponent)이 방을 만든 "호스트"(player_index 0) —
				// 호스트가 지형/스폰 위치 같은 매치 시작 정보를 계산해서 넘겨준다.
				send(opponent, { type: "match_found", player_index: 0, room_id: roomId });
				send(ws, { type: "match_found", player_index: 1, room_id: roomId });
			} else {
				queue.push(ws);
			}
			return;
		}

		if (msg.type === "cancel_match") {
			queue = queue.filter((s) => s !== ws);
			return;
		}

		if (msg.type === "leave") {
			leaveRoom(ws, true);
			return;
		}

		// 그 외 메시지(match_init, fire, turn_result, rematch_request 등)는
		// 전부 같은 방의 상대에게 그대로 전달한다.
		const info = rooms.get(ws);
		if (info) {
			send(info.opponent, msg);
		}
	});

	ws.on("close", () => {
		queue = queue.filter((s) => s !== ws);
		leaveRoom(ws, true);
	});
});

// 무료 호스팅은 오래 아무 통신이 없으면 연결을 끊는 경우가 많아서, 주기적으로
// ping을 보내 연결을 유지하고 죽은 소켓은 정리한다.
setInterval(() => {
	wss.clients.forEach((ws) => {
		if (ws.isAlive === false) {
			leaveRoom(ws, true);
			return ws.terminate();
		}
		ws.isAlive = false;
		ws.ping();
	});
}, 25000);

console.log("TankBoy relay server listening on port " + PORT);
