# src/dev/nakurity/intermediary.py
import asyncio
import json
import traceback
import uuid

from typing import Dict, Any, Optional

import websockets
from websockets.server import WebSocketServerProtocol
from ..utils.loadconfig import load_config

from .client import NakurityClient
from .server import NakurityBackend

from neuro_api.server import RegisterActionsData



"""
Intermediary WebSocket server:
- Accepts connections from Neuro-OS (type="neuro-os") and other integrations (type="integration")
- First message must be registration JSON:
  {"type": "neuro-os" | "integration", "name": "<human-name>"}
- Subsequent messages are routed as JSON blobs. Binary payloads should be base64-encoded by clients.
"""

import pickle
from pathlib import Path

cfg = load_config()

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765

QUEUE_FILE = Path(cfg.get("intermediary", {}).get("relay_queue", "relay_message_queue.pkl"))
AUTH_TOKEN = cfg.get("intermediary", {}).get("auth_token", "super-secret-token")

class Intermediary:
    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
        self.host = host
        self.port = port
        # name -> websocket for integrations
        self.integrations: Dict[str, WebSocketServerProtocol] = {}

        # neuro-os watchers (could be multiple monitoring UIs)
        self.watchers: Dict[str, WebSocketServerProtocol] = {}

        # internal routing hooks (can be replaced by Neuro-OS)
        # `forward_to_neuro` should be an async callable taking (payload: dict) -> optional response
        #self.forward_to_neuro = None  # set by server layer

        # persistent queues, help manage the chokepoint where all integration messages
        # goes pass neuro-relay. It is a relay integration after all.
        self.queue = asyncio.Queue()
        self._load_persisted_queue()

        # waits until other asyncio tasks are ready
        self._ready_event: Optional[asyncio.Event] = None
        
        # Intermediary saves an action registry and converts them into a unified action context
        # for neuro to use, that context will contain all actions from all connected integrations
        # from the Nakurity Backend.
        self.action_registry: Dict[str, Dict[str, Any]] = {}

        #self.nakurity_outbound_client: NakurityLink

        self.nakurity_client: NakurityClient
        self.nakurity_backend: NakurityBackend

        self.queue_client = asyncio.Queue() #messages from client
        self.queue_backend = asyncio.Queue() #messages from backend
        #self.queue_watchers = asyncio.Queue() #data from watchers HANDLED WITHOUT A QUEUE 

        self.traffic = asyncio.Queue()
        self._bg_task = None



    #interfaces
    async def client_to_intermediary(self, payload: dict):
        #called by client: client forwards data to intermediary
        await self.queue_client.put(payload)

    async def backend_to_intermediary(self, payload: dict):
        print("[INTERMEDIARY] backend_to_intermediary")
        #called by backend: backend forwards data to intermediary
        await self.queue_backend.put(payload)      


    #forward register actions to client
    async def register_actions(self, actions_schema: dict):
        await self.nakurity_client.register_actions(actions_schema)



    #queue processing
    async def process_queue_client(self):
        while True:
            if True: #not self.queue_client.empty():
                item = await self.queue_client.get()
                try:
                    if hasattr(self, 'nakurity_backend'):
                        await self.nakurity_backend.intermediary_to_backend(msg=item) #forward data to backend #TODO: processing
                    else:    
                        while not hasattr(self, 'nakurity_backend'):
                            await asyncio.sleep(0.5)

                except Exception:
                    print("[INTERMEDIARY] could not forward message to backend:")
                    traceback.print_exc()
                
                    while not hasattr(self, 'nakurity_backend'):
                        await asyncio.sleep(0.5)


    async def process_queue_backend(self):
        while True:
            if True: #not self.queue_backend.empty():
                item = await self.queue_backend.get()
                print("[INTERMEDIARY] got message from backend")
                try:
                    if hasattr(self, 'nakurity_client'):
                        await self.nakurity_client.intermediary_to_client(msg=item) #forward data to client #TODO: processing
                    else:    
                        while not hasattr(self, 'nakurity_client'):
                            await asyncio.sleep(0.5)

                except Exception:
                    print("[INTERMEDIARY] could not forward message to client:")
                    traceback.print_exc()

                    while not hasattr(self, 'nakurity_client'):
                        await asyncio.sleep(0.5)

                    await asyncio.sleep(1.0)    

            
    """
    async def process_queue_watchers(self):
        while True:
            item = await self.queue_watchers.get()

            # Check for direct-to-neuro message (enhanced privilege)
            if item.get("direct_to_neuro") and item.get("payload"):
                print(f"[Intermediary] {watcher_name} sending direct message to Neuro backend")
                # Forward directly to Nakurity Client → Neuro Backend
                try:
                    #if hasattr(self, "nakurity_outbound_client") and self.nakurity_outbound_client:
                    if True:
                        await self.send_event({
                            "event": "neuroos_direct_message",
                            "from": watcher_name,
                            "data": item["payload"]
                        })
                        await ws.send(json.dumps({"status": "forwarded_to_neuro"}))
                    else:
                        await ws.send(json.dumps({"error": "neuro backend not available"}))
                except Exception:
                    traceback.print_exc()
                    await ws.send(json.dumps({"error": "failed to forward to neuro backend"}))
                continue

            # Regular integration targeting
            target = item.get("target")
            cmd = item.get("cmd")
            if target and cmd and target in self.integrations:
                try:
                    await self.integrations[target].send(json.dumps({
                        "from_watcher": watcher_name,
                        "cmd": cmd
                    }))
                    await ws.send(json.dumps({"status": "sent"}))
                except Exception:
                    await ws.send(json.dumps({"error": "failed to deliver to integration"}))
            else:
                await ws.send(json.dumps({"error": "invalid target/cmd"}))
    """


    def _load_persisted_queue(self):
        if QUEUE_FILE.exists():
            try:
                items = pickle.loads(QUEUE_FILE.read_bytes())
                for item in items:
                    self.queue.put_nowait(item)
                print(f"[Intermediary] Restored {len(items)} queued messages.")
            except Exception:
                print("[Intermediary] Failed to load persisted queue.")

    async def persist_queue(self):
        items = []
        qcopy = asyncio.Queue()
        while not self.queue.empty():
            item = await self.queue.get()
            items.append(item)
            qcopy.put_nowait(item)
        self.queue = qcopy
        QUEUE_FILE.write_bytes(pickle.dumps(items))



    async def collect_registered_actions(self) -> dict:
        """Return a unified action schema for all integrations."""
        unified = {}
        for integration, actions in self.action_registry.items():
            for act_name, schema in actions.items():
                # Prefix action name with integration namespace
                unified[f"{integration}.{act_name}"] = schema
        return unified



    async def _register(self, ws: WebSocketServerProtocol) -> Optional[Dict[str, Any]]:
        raw = await ws.recv()
        try:
            meta = json.loads(raw)
        except Exception:
            await ws.send(json.dumps({"error": "registration must be JSON"}))
            return None

        token = meta.get("auth_token")
        # Check auth: either regular token or neuro-os special token
        neuro_os_token = cfg.get("dependency-authentication", {}).get("neuro-os", {}).get("auth_token")
        if token != AUTH_TOKEN and token != neuro_os_token:
            await ws.send(json.dumps({"error": "invalid auth token"}))
            await ws.close()
            return None

        typ = meta.get("type")
        name = meta.get("name", "unknown")

        if typ == "integration":
            self.integrations[name] = ws
            await self._notify_watchers({
                "event": "integration_connected",
                "name": name
            })
            return {"type": "integration", "name": name}
        elif typ == "neuro-os":
            self.watchers[name] = ws
            # Check if Neuro OS has special auth token for enhanced privileges
            has_special_privileges = (token == neuro_os_token)
            await self._notify_watchers({
                "event": "neuroos_connected",
                "name": name,
                "privileges": "enhanced" if has_special_privileges else "standard"
            })
            return {"type": "neuro-os", "name": name, "privileges": has_special_privileges}
        else:
            await ws.send(json.dumps({"error": "unknown registration type"}))
            return None

    async def _notify_watchers(self, message: dict):
        data = json.dumps(message)
        for name, w in list(self.watchers.items()):
            try:
                await w.send(data)
            except Exception:
                # watcher likely disconnected
                self.watchers.pop(name, None)

    async def _handle_integration_msg(self, origin_name: str, ws: WebSocketServerProtocol):
        """
        Integration -> Relay
        Integration sends JSON or plain text messages. We forward to Neuro via on_forward_to_neuro if present.
        Expected integration message shape:
            {"action":"name", "params": {...}, "meta": {...}}
        """
        async for raw in ws:
            # handle binary frames (e.g., file uploads)
            if isinstance(raw, (bytes, bytearray)):
                filename = f"upload_{origin_name}.bin"
                with open(filename, "wb") as f:
                    f.write(raw)
                print(f"[Relay:{origin_name}] received binary frame ({len(raw)} bytes)")
                await self._notify_watchers({
                    "event": "binary_received",
                    "from": origin_name,
                    "size": len(raw),
                    "file": filename
                })
                continue

            try:
                payload = json.loads(raw)
            except Exception:
                # treat as raw text
                payload = {"action": "raw_text", "raw": raw}
            # notify watchers
            await self._notify_watchers({
                "event": "integration_message",
                "from": origin_name,
                "payload": payload
            })

            # Check for action registration
            if payload.get("event") == "register_actions":
                schema = payload.get("actions", {})
                self.action_registry[origin_name] = schema
                print(f"[Intermediary] Registered actions from {origin_name}: {list(schema.keys())}")
                await self._notify_watchers({
                    "event": "integration_registered_actions",
                    "from": origin_name,
                    "actions": list(schema.keys())
                })
                # keep registration local for multiplexing; do not forward to real Neuro here
                continue
            
            print(f"[Intermediary] forwarding payload from {origin_name} to Nakurity Client → Neuro Backend: {payload!r}")

            # Forward to the real Neuro backend via Nakurity Client (outbound)
            try:
                if True:
                    await self.send_event({
                        "event": "integration_message",
                        "from": origin_name,
                        "data": payload
                    })
                    print(f"[Intermediary] sent to Nakurity Client")
            except Exception:
                traceback.print_exc()
                await ws.send(json.dumps({"error": "failed to forward to neuro backend"}))

    async def _handle_watcher_msg(self, watcher_name: str, ws: WebSocketServerProtocol):
        """
        Watcher (Neuro-OS) -> Relay
        Watchers may send commands to integrations through the relay:
          {"target":"spotify", "cmd":{"action":"play","params":{...}}}
        Enhanced Neuro-OS can also send messages directly to Neuro backend:
          {"direct_to_neuro": true, "payload": {...}}
        """
        async for raw in ws:
            try:
                payload = json.loads(raw)
            except Exception:
                await ws.send(json.dumps({"error": "watcher messages must be JSON"}))
                continue

            # Check for direct-to-neuro message (enhanced privilege)
            if payload.get("direct_to_neuro") and payload.get("payload"):
                print(f"[Intermediary] {watcher_name} sending direct message to Neuro backend")
                # Forward directly to Nakurity Client → Neuro Backend
                try:
                    #if hasattr(self, "nakurity_outbound_client") and self.nakurity_outbound_client:
                    if True:
                        await self.send_event({
                            "event": "neuroos_direct_message",
                            "from": watcher_name,
                            "data": payload["payload"]
                        })
                        await ws.send(json.dumps({"status": "forwarded_to_neuro"}))
                    else:
                        await ws.send(json.dumps({"error": "neuro backend not available"}))
                except Exception:
                    traceback.print_exc()
                    await ws.send(json.dumps({"error": "failed to forward to neuro backend"}))
                continue

            # Regular integration targeting
            target = payload.get("target")
            cmd = payload.get("cmd")
            if target and cmd and target in self.integrations:
                try:
                    await self.integrations[target].send(json.dumps({
                        "from_watcher": watcher_name,
                        "cmd": cmd
                    }))
                    await ws.send(json.dumps({"status": "sent"}))
                except Exception:
                    await ws.send(json.dumps({"error": "failed to deliver to integration"}))
            else:
                await ws.send(json.dumps({"error": "invalid target/cmd"}))

    async def _handler(self, ws: WebSocketServerProtocol):
        reg = await self._register(ws)
        if not reg:
            # registration failed; close
            await ws.close()
            return

        typ = reg["type"]
        name = reg["name"]

        try:
            if typ == "integration":
                await self._handle_integration_msg(name, ws)
            else:
                await self._handle_watcher_msg(name, ws)
        except websockets.ConnectionClosed:
            pass
        finally:
            # cleanup
            if typ == "integration":
                self.integrations.pop(name, None)
                await self._notify_watchers({"event": "integration_disconnected", "name": name})
            else:
                self.watchers.pop(name, None)
                await self._notify_watchers({"event": "neuroos_disconnected", "name": name})

    async def start(self):
        """
        Start the intermediary WebSocket server and set an internal 'ready' event when bound.
        """
        print("[INTERMEDIARY] start...")
        asyncio.create_task(self.retry_queue())
        asyncio.create_task(self.process_queue_backend())
        asyncio.create_task(self.process_queue_client())
        print("[INTERMEDIARY] async tasks created...")

        # ensure we have an event users can await to know when server is ready
        self._ready_event = asyncio.Event()

        try:
            # start the server explicitly (not using `async with` so we can set ready event immediately)
            server = await websockets.serve(self._handler, self.host, self.port)
            # websockets.serve returns a Serve object and it has .wait_closed(); the server is now bound
            print(f"[Intermediary] listening on ws://{self.host}:{self.port}")
            self._ready_event.set()

            # keep serving until server is closed
            asyncio.create_task(server.wait_closed())
        except OSError as e:
            print(f"[Intermediary] Failed to start on ws://{self.host}:{self.port} -> {e}")
            self._ready_event.set()  # unblock waiters even if failed
            raise
        except Exception as e:
            print(f"[Intermediary] Unexpected error: {e}")
            traceback.print_exc()
            self._ready_event.set()
            raise

    #async def send_outbound(self, payload: dict):
    #    self.nakurity_outbound_client

    # Helpers to send messages to integrations from the server layer
    async def send_to_integration(self, name: str, payload: dict):
        ws = self.integrations.get(name)
        if not ws:
            # queue if not connected
            await self.queue.put((name, payload))
            await self.persist_queue()
            print(f"[Intermediary] Queued message for {name}")
            return
        await ws.send(json.dumps(payload))

    async def retry_queue(self):
        """Periodically retry sending queued messages."""
        while True:
            if not self.queue.empty():
                name, payload = await self.queue.get()
                if name in self.integrations:
                    try:
                        await self.integrations[name].send(json.dumps(payload))
                        print(f"[Intermediary] Resent queued message to {name}")
                    except Exception:
                        # put back for later
                        await self.queue.put((name, payload))
                else:
                    await self.queue.put((name, payload))
            await asyncio.sleep(5)

    async def broadcast(self, payload: dict):
        for name, ws in list(self.integrations.items()):
            try:
                await ws.send(json.dumps(payload))
            except Exception:
                self.integrations.pop(name, None)


    async def wait_until_ready(self, timeout: float = 5.0):
        """Await until the server is ready (bounded) or raise on timeout."""
        if self._ready_event is None:
            # start() hasn't been called yet; immediate return or create+wait might be used
            self._ready_event = asyncio.Event()
        await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)



    async def register_actions(self, data: dict):
        actions = data.get("actions", {})
        traffic_id = uuid.uuid4()
        await self.traffic.put({
            "traffic_id": str(traffic_id),
            "type": "register_actions",
            "payload": actions
        })


    async def send_event(self, data: dict):
        event = data.get("event")
        payload = data.get("data", {})
        from_integration = data.get("from", "unknown")
        traffic_id = uuid.uuid4()
        await self.traffic.put({
            "traffic_id": str(traffic_id),
            "type": "event",
            "event": event,
            "from": from_integration,
            "payload": payload
        })

    # ---------------------- #
    #   Background Handling  #
    # ---------------------- #
    async def _handle_traffic(self):
        """Continuously handle traffic items queued by API routes."""
        while True:
            item = await self.traffic.get()
            try:
                print(f"[Nakurity Link] Processing traffic item {item['traffic_id']} ({item['type']})")
                if not self.nakurity_client:
                    # no outbound client yet; requeue and wait a bit  
                    print(f"[Nakurity Link] No client available, requeuing traffic item {item['traffic_id']}")
                    await asyncio.sleep(2.0)  # Increased delay
                    await self.traffic.put(item)
                    continue

                if item["type"] == "register_actions":
                    # forward action schema to real Neuro via NakurityClient
                    # Payload is the actions list directly - pass it to register_actions
                    payload = item.get("payload", [])
                    if isinstance(payload, list):
                        # Convert list to format expected by register_actions (which expects actions in data.actions)
                        await self.nakurity_client.intermediary_to_client(
                            #__import__("json").dumps({
                            {
                                "command": "actions/register",
                                "game": self.nakurity_client.name,
                                "data": {"actions": payload}
                            }
                            #}).encode()
                        )
                        print(f"[Nakurity Link] Forwarded {len(payload)} actions to Neuro backend")
                    else:
                        print(f"[Nakurity Link] ERROR: Expected list of actions, got {type(payload)}")
                elif item["type"] == "event":
                    # wrap generic integration event and forward to Neuro via context command
                    event_type = item.get("event")
                    payload_data = item.get("payload", {})
                    from_integration = item.get("from", "unknown")
                    
                    # Create a context message that Neuro can understand
                    if event_type == "integration_message":
                        # For integration messages, extract the actual command/action
                        if payload_data.get("op") == "choose_force_action":
                            # Convert choose_force_action to actions/force format
                            actions = payload_data.get("actions", [])
                            action_names = [a.get("name", "") for a in actions if "name" in a]
                            payload = {
                                "command": "actions/force",
                                "game": self.nakurity_client.name,
                                "data": {
                                    "state": __import__("json").dumps(payload_data.get("state", {})),
                                    "query": payload_data.get("query", "Choose an action"),
                                    "action_names": action_names,
                                    "ephemeral_context": bool(payload_data.get("ephemeral_context"))
                                }
                            }
                        else:
                            # For other integration messages, send as context with better formatting
                            if payload_data.get("command") == "startup":
                                game_title = payload_data.get("game", "unknown-game")
                                message = f"🎮 Integration '{from_integration}' connected with game '{game_title}'"
                            elif payload_data.get("status") == "ready":
                                game_title = payload_data.get("game", from_integration)
                                message = f"✅ Integration '{game_title}' is ready via relay"
                            else:
                                message = f"📨 Message from integration '{from_integration}': {__import__('json').dumps(payload_data)}"
                            
                            payload = {
                                "command": "context",
                                "game": self.nakurity_client.name,
                                "data": {
                                    "message": message,
                                    "silent": True  # Keep integration messages quieter
                                }
                            }
                    else:
                        # For other event types, send as context with better formatting
                        if event_type == "integration_connected":
                            message = f"🔌 Integration '{from_integration}' connected to relay"
                        elif event_type == "integration_disconnected":
                            message = f"🔌 Integration '{from_integration}' disconnected from relay"
                        elif event_type == "action_test":
                            message = f"🧪 Testing action '{payload_data.get('action', 'unknown')}' from integration '{from_integration}'"
                        else:
                            message = f"📡 Event '{event_type}' from integration '{from_integration}': {__import__('json').dumps(payload_data)}"
                        
                        payload = {
                            "command": "context",
                            "game": self.nakurity_client.name,
                            "data": {
                                "message": message,
                                "silent": True  # Keep events quieter in production
                            }
                        }
                    
                    await self.nakurity_client.intermediary_to_client(
                        __import__("json").dumps(payload).encode()
                    )
                await asyncio.sleep(0)  # yield control
            except Exception as e:
                print(f"[Nakurity Link] error handling traffic item {item.get('traffic_id', 'unknown')}:", e)
                # For critical errors, we might want to requeue the item
                if "connection" in str(e).lower() or "websocket" in str(e).lower():
                    print(f"[Nakurity Link] Connection error detected, requeuing item {item.get('traffic_id', 'unknown')}")
                    await asyncio.sleep(1.0)
                    await self.traffic.put(item)
            finally:
                self.traffic.task_done()

    # ---------------------- #
    #     Server Control     #
    # ---------------------- #
    async def start_link(self):
        # spawn background processor
        self._bg_task = asyncio.create_task(self._handle_traffic())
        print("[Nakurity Link] started")

    async def stop_link(self):
        if self._bg_task:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass
        print("[Nakurity Link] stopped.")


