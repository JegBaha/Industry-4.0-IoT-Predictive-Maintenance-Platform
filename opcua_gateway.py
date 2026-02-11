"""OPC UA Gateway — Server (simulation) and Client with PackML Companion Spec.

Provides an embedded OPC UA server that exposes SmartFact machine data
using the PackML Companion Specification node structure, and a client
that can connect to external OPC UA servers (e.g., Bosch Nexeed, Beckhoff).

Dependencies: asyncua (pip install asyncua)
"""
import asyncio
import json
import logging
import threading
import time
from typing import Optional

log = logging.getLogger(__name__)

# Try importing asyncua — graceful fallback if not installed
try:
    from asyncua import Server, Client, ua
    from asyncua.common.subscription import SubHandler
    ASYNCUA_AVAILABLE = True
except ImportError:
    ASYNCUA_AVAILABLE = False
    log.warning("asyncua not installed. OPC UA features will run in simulation-only mode.")


# ═══════════════════════════════════════════════════════════════════════════
# OPC UA Server (Simulation / Embedded)
# ═══════════════════════════════════════════════════════════════════════════

class OPCUASimServer:
    """Embedded OPC UA Server exposing SmartFact data with PackML nodes.

    Address Space Structure:
        Objects/
        └── Factory/
            ├── Line1/
            │   └── MX100/
            │       ├── PackML/
            │       │   ├── StateCurrent (String)
            │       │   ├── StateCommand (String, writable)
            │       │   └── UnitMode (Int32)
            │       └── Sensors/
            │           ├── Temperature (Float)
            │           ├── Vibration (Float)
            │           ├── Current (Float)
            │           └── Throughput (Int32)
            └── Line2/
                └── MX200/ ...
    """

    def __init__(self, config: dict):
        self.config = config
        self.endpoint = config.get("server_url", "opc.tcp://0.0.0.0:4840")
        self.namespace_uri = config.get("namespace", "urn:smartfact:server")
        self._server: Optional[object] = None
        self._nodes: dict = {}  # {machine_code: {field: node}}
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def _setup(self):
        if not ASYNCUA_AVAILABLE:
            log.info("OPC UA Server running in simulation mode (asyncua not installed)")
            return

        self._server = Server()
        await self._server.init()
        self._server.set_endpoint(self.endpoint)
        self._server.set_server_name("SmartFact OPC UA Server")

        idx = await self._server.register_namespace(self.namespace_uri)

        # Build address space
        factory_node = await self._server.nodes.objects.add_object(idx, "Factory")

        machines = {
            "MX100": {"line": "Line1", "line_node": None},
            "MX200": {"line": "Line2", "line_node": None},
        }

        for machine_code, info in machines.items():
            line_name = info["line"]
            # Create line node if not exists
            if info["line_node"] is None:
                line_node = await factory_node.add_object(idx, line_name)
                info["line_node"] = line_node
            else:
                line_node = info["line_node"]

            machine_node = await line_node.add_object(idx, machine_code)

            # PackML nodes
            packml_node = await machine_node.add_object(idx, "PackML")
            state_node = await packml_node.add_variable(idx, "StateCurrent", "Stopped")
            cmd_node = await packml_node.add_variable(idx, "StateCommand", "")
            await cmd_node.set_writable()
            mode_node = await packml_node.add_variable(idx, "UnitMode", 1)  # 1=Production

            # Sensor nodes
            sensor_node = await machine_node.add_object(idx, "Sensors")
            temp_node = await sensor_node.add_variable(idx, "Temperature", 0.0)
            vib_node = await sensor_node.add_variable(idx, "Vibration", 0.0)
            cur_node = await sensor_node.add_variable(idx, "Current", 0.0)
            thr_node = await sensor_node.add_variable(idx, "Throughput", 0)

            self._nodes[machine_code] = {
                "state": state_node,
                "command": cmd_node,
                "mode": mode_node,
                "temperature": temp_node,
                "vibration": vib_node,
                "current": cur_node,
                "throughput": thr_node,
            }

        log.info(f"OPC UA Server address space created with {len(machines)} machines")

    async def _run_loop(self):
        await self._setup()
        if self._server:
            async with self._server:
                log.info(f"OPC UA Server started at {self.endpoint}")
                self._running = True
                while self._running:
                    await asyncio.sleep(0.1)
        else:
            # Simulation mode — just keep running
            self._running = True
            while self._running:
                await asyncio.sleep(1)

    def update_node(self, machine_code: str, field: str, value):
        """Thread-safe node value update."""
        if machine_code not in self._nodes or field not in self._nodes[machine_code]:
            return
        node = self._nodes[machine_code][field]
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(node.write_value(value), self._loop)

    def run(self):
        """Start server in a new event loop (call from thread)."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run_loop())
        finally:
            self._loop.close()

    def stop(self):
        self._running = False


# ═══════════════════════════════════════════════════════════════════════════
# OPC UA Client (Subscriber)
# ═══════════════════════════════════════════════════════════════════════════

class OPCUASubHandler:
    """Subscription handler that publishes received values to MQTT."""

    def __init__(self, mqtt_publish_fn=None):
        self.mqtt_publish_fn = mqtt_publish_fn
        self.last_values: dict = {}

    def datachange_notification(self, node, val, data):
        node_str = str(node)
        self.last_values[node_str] = {
            "value": val,
            "timestamp": time.time(),
            "source_timestamp": str(data.monitored_item.Value.SourceTimestamp) if data else None,
        }
        log.debug(f"OPC UA data change: {node_str} = {val}")

        if self.mqtt_publish_fn:
            try:
                self.mqtt_publish_fn(node_str, val)
            except Exception:
                log.exception("MQTT publish from OPC UA failed")


class OPCUAClient:
    """OPC UA Client that connects to an external server, browses the
    namespace, and subscribes to configured nodes."""

    def __init__(self, config: dict):
        self.config = config
        self.server_url = config.get("server_url", "opc.tcp://localhost:4840")
        self.subscription_interval = config.get("subscription_interval", 500)
        self._client: Optional[object] = None
        self._handler = OPCUASubHandler()
        self._running = False
        self._subscriptions = []

    async def connect(self):
        if not ASYNCUA_AVAILABLE:
            log.warning("Cannot connect OPC UA client — asyncua not installed")
            return False

        self._client = Client(self.server_url)

        username = self.config.get("username")
        password = self.config.get("password")
        if username and password:
            self._client.set_user(username)
            self._client.set_password(password)

        await self._client.connect()
        log.info(f"OPC UA Client connected to {self.server_url}")
        return True

    async def browse(self, node=None) -> list[dict]:
        """Browse the server namespace."""
        if not self._client:
            return []
        target = node or self._client.nodes.objects
        children = await target.get_children()
        result = []
        for child in children:
            name = await child.read_browse_name()
            node_class = await child.read_node_class()
            result.append({
                "node_id": str(child.nodeid),
                "name": name.Name,
                "class": str(node_class),
            })
        return result

    async def subscribe_nodes(self, node_ids: list[str]):
        """Subscribe to a list of node IDs for data change notifications."""
        if not self._client:
            return
        sub = await self._client.create_subscription(self.subscription_interval, self._handler)
        nodes = [self._client.get_node(nid) for nid in node_ids]
        handles = await sub.subscribe_data_change(nodes)
        self._subscriptions.append((sub, handles))
        log.info(f"Subscribed to {len(nodes)} OPC UA nodes")

    async def disconnect(self):
        if self._client:
            await self._client.disconnect()
            log.info("OPC UA Client disconnected")

    def get_last_values(self) -> dict:
        return dict(self._handler.last_values)


# ═══════════════════════════════════════════════════════════════════════════
# Gateway Manager (Thread-based, matches TwinCAT gateway pattern)
# ═══════════════════════════════════════════════════════════════════════════

class OPCUAGateway:
    """Manages OPC UA Server (sim) and Client lifecycle.

    Usage:
        gw = OPCUAGateway(config)
        gw.start()  # Starts server + optionally client in threads
        gw.stop()
    """

    def __init__(self, config: dict):
        self.config = config
        self.server = OPCUASimServer(config)
        self.client = OPCUAClient(config)
        self._server_thread: Optional[threading.Thread] = None
        self._running = False

    def start(self):
        """Start the OPC UA server in a daemon thread."""
        if self._running:
            return
        self._running = True
        self._server_thread = threading.Thread(target=self.server.run, daemon=True, name="opcua-server")
        self._server_thread.start()
        log.info("OPC UA Gateway started")

    def stop(self):
        self._running = False
        self.server.stop()
        log.info("OPC UA Gateway stopped")

    def update_machine_data(self, machine_code: str, data: dict):
        """Update OPC UA server nodes with latest sensor data."""
        for field, value in data.items():
            self.server.update_node(machine_code, field, value)

    @property
    def is_running(self) -> bool:
        return self._running and self._server_thread is not None and self._server_thread.is_alive()

    def get_status(self) -> dict:
        return {
            "running": self.is_running,
            "server_endpoint": self.config.get("server_url", ""),
            "simulation_mode": self.config.get("simulation_mode", True),
            "asyncua_available": ASYNCUA_AVAILABLE,
        }


# ═══════════════════════════════════════════════════════════════════════════
# Standalone runner
# ═══════════════════════════════════════════════════════════════════════════

def load_config(path: str = "opcua_gateway_config.json") -> dict:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {
            "server_url": "opc.tcp://0.0.0.0:4840",
            "namespace": "urn:smartfact:server",
            "simulation_mode": True,
            "subscription_interval": 500,
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = load_config()
    gw = OPCUAGateway(config)
    try:
        gw.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        gw.stop()
