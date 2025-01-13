import subprocess

from surrealdb_rpc.client.websocket import SurrealDBClient, SurrealDBError
from surrealdb_rpc.data_model import RecordId


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
            # shell=True,
            # preexec_fn=os.setsid,
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
        with SurrealDBClient(
            host="localhost",
            port=18000,
            ns="test",
            db="test",
            user="root",
            password="root",
        ) as connection:
            example_id = RecordId.new("example", "123")
            response_create = connection.create(
                # specify RecordId as string (will create a TextRecordId)
                "example:123",
                # specify fields as kwargs
                text="Some value",
                # lists for arrays
                array=[1, 2, 3],
                # regular dicts for objects
                object={"key": "value"},
                # RecordId object with automatic ID escaping
                reference=RecordId.new("other", {"foo": {"bar": "baz"}}),
            )
            # SurrealDBClient.create returns the created record
            assert response_create["id"] == example_id

            # Fetch a single record by ID
            response_select = connection.select("example:123")
            assert response_select == response_create

            # Run a SurrealQL query
            response_query = connection.query(
                'SELECT * FROM example WHERE text = "Some value"'
            )
            # Returns a result for each statement in the query
            assert len(response_query) == 1
            first_response = response_query[0]
            # Retrieve actual result with the "result" key
            first_response_result = first_response["result"]
            # `SELECT` returns a list of records
            assert len(first_response_result) == 1
            assert first_response_result[0] == response_create
    except SurrealDBError as e:
        db.terminate()
        print(db.stderr())
        raise e
    finally:
        db.terminate()
