import logging
import os
import shutil
import subprocess
from typing import Final

import pytest
import requests

from surrealdb_rpc.client import SurrealDBError
from surrealdb_rpc.client.websocket import SurrealDBWebsocketClient
from surrealdb_rpc.tests.integration.queries import Queries

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

_HOST: Final[str] = os.getenv("SURREAL_BIND", "localhost:18000").split(":", 1)[0]
_PORT: Final[int] = int(
    os.getenv("SURREAL_BIND", "localhost:18000").split(":", 1)[1] or "18000"
)
_USER: Final[str] = os.getenv("SURREAL_USER", "root")
_PASSWORD: Final[str] = os.getenv("SURREAL_PASS", "root")
_DATABASE: Final[str] = os.getenv("SURREAL_DATABASE", "test")
_NAMESPACE: Final[str] = os.getenv("SURREAL_NAMESPACE", "test")
_REMOVE_AFTER: Final[bool] = bool(os.getenv("SURREAL_DATABASE_REMOVE_AFTER", "true"))
_STARTUP_TIMEOUT: Final[int] = int(os.environ.get("SURREAL_STARTUP_TIMEOUT", "5"))
_SHUTDOWN_TIMEOUT: Final[int] = int(os.environ.get("SURREAL_SHUTDOWN_TIMEOUT", "5"))


class SurrealDB:
    def __init__(
        self,
        name: str = "surrealdb-rpc-test",
        host: str = _HOST,
        port: int = _PORT,
        user: str = _USER,
        password: str = _PASSWORD,
    ):
        self.process = None
        self.name = name
        self.host = host
        self.port = port

        if not bool(user and password):
            raise ValueError("User and password may not be empty")

        self.user = user
        self.password = password

        if shutil.which("surreal") is not None:
            LOGGER.info("Starting SurrealDB using executable")
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
                f"{self.host}:{self.port}",
            ]
        elif (cmd := shutil.which("docker") or shutil.which("podman")) is not None:
            LOGGER.info(f"Starting SurrealDB using {cmd}")
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
            raise RuntimeError(
                "Cannot find surreal, docker, or podman to start a SurrealDB instance for integration testing"
            )

    def start(self):
        self.process = subprocess.Popen(
            self._cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        try:
            self.process.wait(timeout=_STARTUP_TIMEOUT)
        except subprocess.TimeoutExpired:
            return self

        stdout = self.stdout()
        stderr = self.stderr()
        raise RuntimeError(
            "\n".join(
                filter(
                    bool,
                    [
                        "Failed to start SurrealDB instance using " + self._cmd[0],
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

    def terminate(self) -> bool:
        if self.process is None:
            return True

        self.process.terminate()
        try:
            self.process.wait(_SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            pass
        else:
            return True

        self.process.kill()
        try:
            self.process.wait(_SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            msg = f"Failed to terminate & kill SurrealDB with PID {self.process.pid} after timeout started using {self._cmd[0]}"
            if err := self.stderr():
                err = "  ".join(err.splitlines(True))
                msg += f", stderr:\n  {err}"
            raise RuntimeError(msg)

        return True

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()


def _is_db_already_running():
    try:
        response = requests.get(f"http://{_HOST}:{_PORT}/version")
        response.raise_for_status()
        return response.text.startswith("surrealdb")
    except Exception:
        return False


@pytest.fixture(scope="module")
def connection():
    db = None
    try:
        if not _is_db_already_running():
            db = SurrealDB(
                host=_HOST,
                port=_PORT,
                user=_USER,
                password=_PASSWORD,
            )
            db.start()
        else:
            LOGGER.info(
                f"Using SurrealDB instance running at {_HOST}:{_PORT} for integration tests"
            )

        with SurrealDBWebsocketClient(
            host=_HOST,
            port=_PORT,
            ns=_NAMESPACE,
            db=_DATABASE,
            user=_USER,
            password=_PASSWORD,
        ) as connection:
            yield connection

            if _REMOVE_AFTER:
                connection.query(f"REMOVE DATABASE IF EXISTS {_DATABASE}")
    except SurrealDBError as e:
        msg = "Caught a SurrealDB error"
        if db and (err := db.stderr()):
            err = "  ".join(err.splitlines(True))
            msg += f", stderr:\n  {err}"
        raise RuntimeError(msg) from e
    finally:
        return db is None or db.terminate()


def _should_skip_test() -> bool:
    """Skip the test if no integration test DB is running and we cannot start one"""
    return not (
        _is_db_already_running()
        or shutil.which("surreal")
        or shutil.which("docker")
        or shutil.which("podman")
    )


@pytest.mark.skipif(
    _should_skip_test(),
    reason="No integration test DB is running and we cannot start one",
)
@pytest.mark.integration
class TestWebsocketClient(Queries):
    pass
