import subprocess

from surrealdb_rpc.client.websocket import SurrealDBError
from surrealdb_rpc.tests.utils import _test_base_queries


class DockerDB:
    def __init__(
        self,
        name: str = "surrealdb-test",
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

    def start(self):
        self.process = subprocess.Popen(
            [
                "docker",
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
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
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
                        "Failed to start Docker container",
                        f"stdout: {stdout}" if stdout else "",
                        f"stderr: {stderr}" if stderr else "",
                    ],
                )
            )
        )

    def stdout(self):
        return self.process and self.process.stdout.read().decode()

    def stderr(self):
        return self.process and self.process.stderr.read().decode()

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
            raise RuntimeError("Failed to stop Docker container!")

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()


def test_docker_db():
    db = DockerDB().start()
    try:
        _test_base_queries(db.port, db.user, db.password)
    except SurrealDBError as e:
        db.terminate()
        print(db.stderr())
        raise e
    finally:
        db.terminate()
