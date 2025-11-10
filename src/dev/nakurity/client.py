# src/dev/nakurity/client.py
import json
import websockets
import asyncio
from neuro_api.api import AbstractNeuroAPI, NeuroAction
#from .intermediary import Intermediary

"""
A Neuro client that connects out to a real Neuro backend.
"""

class NakurityClient(AbstractNeuroAPI):
    def __init__(self, websocket, intermediary):
        self.websocket = websocket
        self.name = "neuro-relay"
        super().__init__(self.name)
        # router_forward_cb is expected to be an async callable that accepts a dict
        # e.g. intermediary._handle_intermediary_forward
        #self.router_forward_cb = router_forward_cb
        self._reader_task: asyncio.Task | None = None
        
        #self._recv_q = asyncio.Queue()

        self.intermediary = intermediary

        self.queue_intermediary = asyncio.Queue() #queue of messages from intermediary

        #ratelimit settings
        self.load = 0
        self.maxload = 20
        self.ratelimit_reset_time = 3
        self.ratelimit_cooldown_time = 7

        print("[Nakurity Client] has initialized.")

    async def intermediary_to_client(self, msg: dict):
        #called by intermediary: intermediary forwards data to client
        await self.queue_intermediary.put(msg)


    async def process_queue_intermediary(self):
        while True:
            if self.load <= self.maxload:
                item = await self.queue_intermediary.get()
                self.load += 1
                print("[CLIENT] debug load: ", self.load, "/", self.maxload)
                await self.send_to_neuro((json.dumps(item).encode()))
            else:
                await asyncio.sleep(0.5)
                    
                


    async def ratelimiter(self): #DOES NOT AFFECT ACTION RESULTS. should it? idk
        while True:
            if self.load > self.maxload: #sent more than "maxload" messages in under "ratelimit_reset_time" seconds. Dont send any for "ratelimit_cooldown_time" seconds
                await asyncio.sleep(self.ratelimit_cooldown_time) 
                self.load = 0

            self.load = 0
            await asyncio.sleep(self.ratelimit_reset_time)
            # i dont like this mechanism 





    async def initialize(self):
        # Send required startup to set game/title on backend
        try:
            asyncio.create_task(self.process_queue_intermediary()) # create task for processing messages from intermediary
            asyncio.create_task(self.ratelimiter())

            await self.send_startup_command()
            print("[Nakurity Client] Startup command sent successfully")
        except Exception as e:
            print(f"[Nakurity Client] Warning: Failed to send startup command: {e}")
            print("[Nakurity Client] Will continue - startup may be sent later if reconnected")
        # Optional steps disabled to avoid schema mismatches with dev backends
        # actions_schema = await self.collect_registered_actions()
        # if actions_schema:
        #     await self.register_actions(actions_schema)


    async def send_to_neuro(self, command_bytes: bytes):
        """Send formatted neuro command bytes to the real neuro backend."""
        await self.send_command_data(command_bytes)



    async def handle_action(self, action: NeuroAction):
        # Actions from real Neuro backend flow back to intermediary → integrations
        print(f"[Nakurity Client] ========================================")
        print(f"[Nakurity Client] RECEIVED ACTION FROM TONY (Neuro Backend)")
        print(f"[Nakurity Client]   action.name: {action.name}")
        print(f"[Nakurity Client]   action.id_: {action.id_}")
        print(f"[Nakurity Client]   action.data: {action.data}")
        print(f"[Nakurity Client] Forwarding to router_forward_cb...")
        print(f"[Nakurity Client] ========================================")
        
        try:
            """
            result = await self.router_forward_cb({
                "from_neuro_backend": True,
                "action": action.name,
                "data": action.data or "{}", #json.loads(action.data or "{}"),
                "id": action.id_
            })
            """
            result = await self.intermediary.client_to_intermediary({
                "from_neuro_backend": True,
                "action": action.name,
                "data": action.data or "{}", #json.loads(action.data or "{}"),
                "id": action.id_
            })

            print(f"[Nakurity Client] router_forward_cb returned: {result}")
            
            # Send success result back to Tony
            # The integration should have executed the action
            if result and result.get("forwarded_to"):
                await self.send_action_result(action.id_, True, f"Action forwarded to {result['forwarded_to']}")
            elif result and "error" in result:
                await self.send_action_result(action.id_, False, f"Error: {result['error']}")
            else:
                await self.send_action_result(action.id_, True, "Action executed")
        except Exception as e:
            print(f"[Nakurity Client] ERROR handling action: {e}")
            import traceback
            traceback.print_exc()
            await self.send_action_result(action.id_, False, f"Relay error: {str(e)}")




    async def collect_registered_actions(self):
        """Ask the intermediary (via router callback) for available actions."""
        try:
            resp = await self.router_forward_cb({"query": "get_registered_actions"})
            return resp.get("actions", {}) if isinstance(resp, dict) else {}
        except Exception as e:
            print(f"[Nakurity Client] failed to collect actions: {e}")
            return {}



    async def register_actions(self, actions_schema: dict):
        """Register integration actions with Neuro backend."""
        # Convert actions_schema dict to proper Action format
        actions_list = []
        for action_name, action_info in actions_schema.items():
            if isinstance(action_info, dict):
                action = {
                    "name": action_name,
                    "description": action_info.get("description", f"Action: {action_name}"),
                    "schema": action_info.get("schema")
                }
            else:
                # Simple string description
                action = {
                    "name": action_name,
                    "description": str(action_info) if action_info else f"Action: {action_name}",
                    "schema": None
                }
            actions_list.append(action)
        
        if not actions_list:
            print("[Nakurity Client] No actions to register")
            return
            
        payload = {
            "command": "actions/register",
            "game": self.name,
            "data": {"actions": actions_list}
        }
        print(f"[Nakurity Client] Registering {len(actions_list)} actions with Neuro backend")
        await self.send_to_neuro(json.dumps(payload).encode())


    async def _read_loop(self):
        try:
            while True:
                await self.read_message()
        except websockets.exceptions.ConnectionClosed:
            print("[Nakurity Client] connection closed")
            # Signal that reconnection is needed
            if hasattr(self, '_reconnect_callback'):
                asyncio.create_task(self._reconnect_callback())
        except Exception as e:
            print("[Nakurity Client] read loop exception:", e)
            # Signal that reconnection is needed
            if hasattr(self, '_reconnect_callback'):
                asyncio.create_task(self._reconnect_callback())

    async def write_to_websocket(self, data: str):
        await self.websocket.send(data)

    async def read_from_websocket(self) -> str:
        result = await self.websocket.recv()
        return result
    


    async def on_connect(self):
        print("[Nakurity Client] connected")

    async def on_disconnect(self):
        print("[Nakurity Client] disconnected")







async def connect_outbound(uri: str, intermediary, max_retries: int = 10, retry_delay: float = 2.0):
    """
    Connect to the real neuro backend and return NakurityClient instance with retry logic.
    This function will create a background read loop for the websocket.
    """
    for attempt in range(max_retries):
        print(f"[Nakurity Client] trying to connect to Neuro Backend (attempt {attempt + 1}/{max_retries})")
        try:
            ws = await websockets.connect(uri)
            print(f"[Nakurity Client] successfully connected to {uri}")
            break
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"[Nakurity Client] failed to connect after {max_retries} attempts: {uri}")
                print(f"[Nakurity Client] final error: {e}")
                return None
            else:
                delay = retry_delay * (2 ** min(attempt, 6))  # Exponential backoff, max 128s
                print(f"[Nakurity Client] connection failed: {e}")
                print(f"[Nakurity Client] retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)

    try:
        print("[Nakurity Client] starting connection to neuro backend", uri)
        c = NakurityClient(ws, intermediary)
        await c.initialize()
        # start background read loop
        loop = asyncio.get_event_loop()
        c._reader_task = loop.create_task(c._read_loop())
        return c
    except Exception as e:
        print("[Nakurity Client] has failed during initialize!")
        print(e)
        try:
            await ws.close()
        except Exception:
            pass
        return None


#========================================================================================================================================
#========================================================================================================================================
#========================================================================================================================================
#UNUSED. YET...




