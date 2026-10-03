#!/usr/bin/env python3
"""Test-only TCP outages and RiseDev compute crashes; no Oracle lifecycle control."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time

# Contract: keep this test-owned fixture constant synchronized with
# TEST_ORACLE_PROXY_PORT in oracle_test_env.slt.part, not service profiles.
# Private loopback listener for serial fault scenarios, not Oracle's own port.
TEST_ORACLE_PROXY_PORT = 1522


# Public operations used directly by the SLTs.
def control_oracle_proxy(action: str):
    """Control a test-owned TCP proxy without stopping Oracle itself.

    RW's CDC source and backfill reader connect to TEST_ORACLE_PROXY_PORT on
    loopback; fixture SQL connects directly to Oracle. The proxy forwards bytes
    in both directions to ORACLE_HOST:ORACLE_PORT, using the same environment settings
    as the fixture. It runs this module as a separate worker process so it
    survives both compute crashes and the end of an SLT system-command block.
    Later calls send commands over a local Unix socket, control.sock, and read
    JSON replies containing the worker PID, forwarding state, and counters.

    Actions:
        start: Stop any previous test proxy on this port, launch a new worker
            using ORACLE_HOST/ORACLE_PORT, and wait for its control socket.
            Worker output goes to worker.log beside the control socket.
        disconnect: Disable forwarding, reset the rejection counter, and close
            all active connections, including pending upstream connections.
            New connections are rejected until reconnect is requested.
        wait-rejected: Poll until at least one connection attempt is rejected,
            proving the outage was exercised rather than merely sleeping.
        reconnect: Allow new connections. Old connections are not restored;
            the connector must reconnect through its normal retry mechanism.
        status: Return the worker's current state and counters without changing them.
        stop: Close connections, stop the worker, and wait for socket cleanup.
            An already-stopped worker is accepted. Keep worker.log and its
            directory for diagnostics; cleanup removes them explicitly.

    Unsupported actions raise ValueError. Waits return as soon as their condition
    is met; timeouts are waiting limits, not required delays. The worker also
    exits after one hour if an abandoned test never calls stop.
    """
    directory = (
        Path(tempfile.gettempdir()) / f"rw-oracle-proxy-{TEST_ORACLE_PROXY_PORT}"
    )
    control = directory / "control.sock"

    def request(command):
        with socket.socket(socket.AF_UNIX) as client:
            client.settimeout(10)
            client.connect(str(control))
            client.sendall((command + "\n").encode())
            with client.makefile("rb") as response:
                result = json.loads(response.readline())
        if "error" in result:
            raise RuntimeError(result["error"])
        return result

    if action == "start":
        # Remove an abandoned proxy through its own control socket, never a saved PID.
        control_oracle_proxy("stop")
        directory.mkdir(mode=0o700, exist_ok=True)
        with (directory / "worker.log").open("w") as log:
            worker = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "__run_proxy",
                    str(control),
                    os.environ["ORACLE_HOST"],
                    os.environ["ORACLE_PORT"],
                ],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=True,
            )
        try:

            def started():
                if worker.poll() is not None:
                    raise RuntimeError(
                        f"proxy exited: {(directory / 'worker.log').read_text()}"
                    )
                return request("status") if control.exists() else None

            result = _wait(started, "Oracle proxy startup", timeout=10)
        except BaseException:
            worker.terminate()
            worker.wait(timeout=10)
            raise
    elif action == "stop":
        try:
            result = request("stop")
        except (FileNotFoundError, ConnectionRefusedError):
            control.unlink(missing_ok=True)
            return
        _wait(lambda: not control.exists(), "Oracle proxy shutdown", timeout=10)
    elif action == "wait-rejected":
        result = _wait(
            lambda: (state if (state := request("status"))["rejected"] > 0 else None),
            "a connector reconnect attempt during the Oracle outage",
        )
    elif action in ("disconnect", "reconnect", "status"):
        result = request(action)
    else:
        raise ValueError(f"unknown proxy action: {action}")
    print(f"Oracle proxy {action}: {result}", flush=True)
    return result


def control_compute(action: str):
    """Check, crash, or restart this checkout's single RiseDev compute process.

    Debezium runs inside compute's JVM, so killing compute also loses the CDC
    engine. Frontend, meta, Oracle, the proxy, and Hummock storage remain alive.
    Process discovery matches the executable role, this checkout's config path,
    and ancestry under the compute tmux pane, rather than using a broad pkill.

    RiseDev's risedev_commands.yaml supplies the original per-window environment.
    Restart uses tmux respawn-pane with those settings explicitly restored:
    respawn-pane alone retains the command but loses new-window -e values such
    as CONNECTOR_LIBS_PATH. The oracle-test-compute.json file records only the
    node name and killed PID; it is NOT a RisingWave checkpoint or CDC offset.

    Actions:
        check: Require a running compute and a matching meta process, reject
            process-local hummock+memory storage, and return the compute PID.
            This does not check Oracle connectivity or CDC readiness.
        crash: Perform the same checks, save the node/PID record, send SIGKILL
            to the actual compute process, and wait for it to exit. Return the
            killed PID, leaving the tmux logging wrapper available for restart.
        start: If compute is absent, require the saved crash record, relaunch
            in its existing pane, and wait for a different PID and RPC listener.
            If already running, accept it unless it is the recorded killed PID.
            Remove the crash record and return the running PID. Do not wait for
            CDC readiness: compute-first recovery keeps Oracle unreachable here.

    Unsupported actions raise ValueError. Waits return as soon as their condition
    is met; timeouts are waiting limits, not required delays.
    """
    if action not in ("check", "crash", "start"):
        raise ValueError(f"unknown compute action: {action}")
    # RiseDev records per-window -e options here. respawn-pane alone preserves
    # the command but NOT these environment variables (notably CONNECTOR_LIBS_PATH).
    import yaml

    config = Path(os.environ["PREFIX_CONFIG"]).resolve()
    launch_commands = yaml.safe_load((config / "risedev_commands.yaml").read_text())
    checkpoint = config / "oracle-test-compute.json"
    panes = _run(
        "tmux",
        "-L",
        "risedev",
        "list-panes",
        "-s",
        "-t",
        "risedev",
        "-F",
        "#{window_name} #{pane_id} #{pane_pid}",
    ).stdout.splitlines()
    computes = [line.split() for line in panes if line.startswith("compute-node-")]
    if len(computes) != 1:
        raise RuntimeError(
            f"restart test requires exactly one RiseDev compute pane: {computes}"
        )
    node, pane_id, pane_pid = computes[0]
    launch = shlex.split(launch_commands[node].replace("\\\n", ""))
    wrapper = next(
        i for i, arg in enumerate(launch) if Path(arg).name == "run_command.sh"
    )
    environment = []
    for i, arg in enumerate(launch[:wrapper]):
        if arg == "-e":
            environment.extend(launch[i : i + 2])

    def current_process():
        return _node_process(_processes(), int(pane_pid), "compute-node", config)

    current = current_process()

    def check_process_and_storage():
        if current is None:
            raise RuntimeError(
                f"no running compute in {node}; use start to restore a test crash"
            )
        # Reject process-local storage before destroying any checkpoint data.
        metas = [line.split() for line in panes if line.startswith("meta-node-")]
        if len(metas) != 1:
            raise RuntimeError(f"restart test requires one RiseDev meta pane: {metas}")
        meta = _node_process(_processes(), int(metas[0][2]), "meta-node", config)
        if meta is None or "--state-store" not in meta[1]:
            raise RuntimeError(
                "cannot verify the running meta node's state-store backend"
            )
        backend = meta[1][meta[1].index("--state-store") + 1]
        if backend.startswith("hummock+memory"):
            raise RuntimeError(
                "compute crash requires persistent storage; start the Oracle profile with MinIO"
            )

    if action == "check":
        check_process_and_storage()
    elif action == "crash":
        check_process_and_storage()
        checkpoint.write_text(json.dumps({"node": node, "pid": current[0]}))
        os.kill(current[0], signal.SIGKILL)
        _wait(
            lambda: current_process() is None,
            f"{node} PID {current[0]} to exit",
            timeout=15,
        )
    elif action == "start":
        if current is None:
            saved = json.loads(checkpoint.read_text())
            if saved["node"] != node:
                raise RuntimeError(f"compute launch target changed: {saved} -> {node}")
            # Reuse the original command and explicitly restore RiseDev's environment.
            # This replaces only its logging wrapper: the compute was already SIGKILLed.
            _run(
                "tmux",
                "-L",
                "risedev",
                "respawn-pane",
                "-k",
                "-t",
                pane_id,
                *environment,
            )

            # The same pane now has a new process tree.
            def restarted():
                pane = _run(
                    "tmux",
                    "-L",
                    "risedev",
                    "display-message",
                    "-p",
                    "-t",
                    pane_id,
                    "#{pane_pid}",
                ).stdout.strip()
                process = _node_process(_processes(), int(pane), "compute-node", config)
                if process is None or process[0] == saved["pid"]:
                    return None
                args = process[1]
                host, port = args[args.index("--listen-addr") + 1].rsplit(":", 1)
                try:
                    with socket.create_connection((host, int(port)), timeout=1):
                        return process
                except OSError:
                    return None

            current = _wait(restarted, f"new {node} process/RPC listener")
        elif checkpoint.exists():
            saved = json.loads(checkpoint.read_text())
            if current[0] == saved["pid"]:
                raise RuntimeError(f"crashed compute PID {current[0]} is still running")
        checkpoint.unlink(missing_ok=True)
    else:
        raise ValueError(f"unknown compute action: {action}")
    print(f"compute {action}: node={node}, pid={current[0]}", flush=True)
    return current[0]


def cleanup() -> None:
    checkpoint = (
        Path(os.environ["PREFIX_CONFIG"]).resolve() / "oracle-test-compute.json"
    )
    if checkpoint.exists():
        control_compute("start")
    control_oracle_proxy("stop")
    directory = (
        Path(tempfile.gettempdir()) / f"rw-oracle-proxy-{TEST_ORACLE_PROXY_PORT}"
    )
    if directory.exists():
        (directory / "worker.log").unlink(missing_ok=True)
        directory.rmdir()


# Module-private lifecycle helpers.
def _run(*args):
    return subprocess.run(args, text=True, capture_output=True, check=True, timeout=120)


def _wait(predicate, description, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := predicate():
            return result
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for {description}")


def _processes():
    result = {}
    for line in _run("ps", "-axo", "pid=,ppid=,stat=,args=").stdout.splitlines():
        fields = line.strip().split(None, 3)
        if len(fields) != 4:
            continue
        pid, parent, status, command = fields
        if status.startswith("Z"):
            continue
        try:
            args = shlex.split(command)
        except ValueError:
            # ps does not preserve quoting in arbitrary, unrelated process arguments.
            continue
        result[int(pid)] = (int(parent), args)
    return result


def _node_process(processes, pane_pid, role, config):
    matches = []
    for pid, (parent, args) in processes.items():
        if not args:
            continue
        executable = Path(args[0]).name
        if not (
            executable == role or (executable == "risingwave" and args[1:2] == [role])
        ):
            continue
        if "--config-path" not in args:
            continue
        if (
            Path(args[args.index("--config-path") + 1]).resolve()
            != config / "risingwave.toml"
        ):
            continue
        ancestors = set()
        while parent in processes and parent not in ancestors and parent != pane_pid:
            ancestors.add(parent)
            parent = processes[parent][0]
        if parent == pane_pid or pid == pane_pid:
            matches.append((pid, args))
    if len(matches) > 1:
        raise RuntimeError(f"multiple {role} processes in one RiseDev pane: {matches}")
    return matches[0] if matches else None


class _Proxy:
    def __init__(self, upstream_host, upstream_port):
        self.upstream = (upstream_host, upstream_port)
        self.enabled = True
        self.connections = set()
        self.forwarded = 0
        self.rejected = 0
        self.stopped = asyncio.Event()

    async def forward(self, reader, writer):
        task = asyncio.current_task()
        self.connections.add(task)
        upstream_writer = None
        pumps = []
        try:
            if not self.enabled:
                self.rejected += 1
                return
            upstream_reader, upstream_writer = await asyncio.wait_for(
                asyncio.open_connection(*self.upstream), timeout=10
            )
            self.forwarded += 1

            async def copy(source, destination):
                while data := await source.read(65536):
                    destination.write(data)
                    await destination.drain()

            pumps = [
                asyncio.create_task(copy(reader, upstream_writer)),
                asyncio.create_task(copy(upstream_reader, writer)),
            ]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        except (OSError, asyncio.TimeoutError) as error:
            print(f"proxy upstream error: {error}", flush=True)
        finally:
            for pump in pumps:
                pump.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            writer.transport.abort()
            if upstream_writer is not None:
                upstream_writer.transport.abort()
            self.connections.discard(task)

    async def disconnect(self):
        self.enabled = False
        self.rejected = 0
        connections = list(self.connections)
        for task in connections:
            task.cancel()
        await asyncio.gather(*connections, return_exceptions=True)

    async def control(self, reader, writer):
        try:
            command = (
                (await asyncio.wait_for(reader.readline(), timeout=10)).decode().strip()
            )
            if command in ("disconnect", "stop"):
                await self.disconnect()
            elif command == "reconnect":
                self.enabled = True
            elif command != "status":
                raise ValueError(f"unknown proxy command: {command}")
            result = {
                "pid": os.getpid(),
                "enabled": self.enabled,
                "active": len(self.connections),
                "forwarded": self.forwarded,
                "rejected": self.rejected,
            }
            writer.write((json.dumps(result) + "\n").encode())
            await writer.drain()
            if command == "stop":
                self.stopped.set()
        finally:
            writer.close()
            await writer.wait_closed()

    async def serve(self, control):
        server = await asyncio.start_server(
            self.forward, "127.0.0.1", TEST_ORACLE_PROXY_PORT
        )
        try:
            commands = await asyncio.start_unix_server(self.control, path=str(control))
            async with server, commands:
                # An abandoned SLT must not leave a proxy running indefinitely.
                await asyncio.wait_for(self.stopped.wait(), timeout=3600)
        finally:
            server.close()
            await self.disconnect()
            control.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("cleanup", help="restore compute and remove proxy artifacts")
    worker_parser = subparsers.add_parser("__run_proxy", help="internal proxy worker")
    worker_parser.add_argument("control", type=Path)
    worker_parser.add_argument("upstream_host")
    worker_parser.add_argument("upstream_port", type=int)
    args = parser.parse_args()
    if args.command == "cleanup":
        cleanup()
    elif args.command == "__run_proxy":
        asyncio.run(_Proxy(args.upstream_host, args.upstream_port).serve(args.control))
    else:
        raise ValueError(f"unknown fault command: {args.command}")
