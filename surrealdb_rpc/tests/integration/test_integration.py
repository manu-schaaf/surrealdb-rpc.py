import shutil
import subprocess

import pytest

from surrealdb_rpc.client import SurrealDBError
from surrealdb_rpc.client.websocket import SurrealDBWebsocketClient
from surrealdb_rpc.tests.integration.queries import Queries


class SurrealDB:
    def __init__(
        self,
        name: str = "surrealdb-rpc-test",
        port: int = 18000,
        user: str = "root",
        password: str = "root",
    ):
        self.process = None
        self.name = name
        self.port = port

        if not bool(user and password):
            raise ValueError("User and password may not be empty")

        self.user = user
        self.password = password

        if shutil.which("surreal") is not None:
            self._cmd = [
                "surreal",
                "start",
                "--log",
                "debug",
                "--user",
                self.user,
                "--pass",
                self.password,
                "--bind",
                f"127.0.0.1:{self.port}",
            ]
        elif (cmd := shutil.which("docker") or shutil.which("podman")) is not None:
            self._cmd = [
                cmd,
                "run",
                "--rm",
                "--name",
                "surrealdb-test",
                "-p",
                f"{self.port}:8000",
                "--pull",
                "always",
                "surrealdb/surrealdb:latest",
                "start",
                "--log",
                "debug",
                "--user",
                self.user,
                "--pass",
                self.password,
            ]
        else:
            raise RuntimeError("Neither surreal nor docker or podman exists")

    def start(self):
        self.process = subprocess.Popen(
            self._cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            return self

        stdout = self.stdout()
        stderr = self.stderr()
        raise RuntimeError(
            "\n".join(
                filter(
                    bool,
                    [
                        "Failed to start SurrealDB",
                        f"stdout: {stdout}" if stdout else "",
                        f"stderr: {stderr}" if stderr else "",
                    ],
                )
            )
        )

    def stdout(self):
        return (
            self.process and self.process.stdout and self.process.stdout.read().decode()
        )

    def stderr(self):
        return (
            self.process and self.process.stderr and self.process.stderr.read().decode()
        )

    def terminate(self):
        if self.process is None:
            return

        self.process.terminate()

        try:
            self.process.wait(1)
        except subprocess.TimeoutExpired:
            self.process.kill()
        else:
            return

        try:
            self.process.wait(5)
        except subprocess.TimeoutExpired:
            raise RuntimeError("Failed to stop SurrealDB!")

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()


@pytest.fixture(scope="module")
def connection():
    db = SurrealDB().start()
    try:
        with SurrealDBWebsocketClient(
            host="localhost",
            port=18000,
            ns="test",
            db="test",
            user="root",
            password="root",
        ) as connection:
            yield connection
    except SurrealDBError as e:
        db.terminate()
        print(db.stderr())
        raise e
    finally:
        db.terminate()


@pytest.mark.skipif(
    shutil.which("surreal") is None
    and shutil.which("docker") is None
    and shutil.which("podman") is None,
    reason="Neither surreal nor docker or podman exists",
)
class TestWebsocketClient(Queries):
    pass
