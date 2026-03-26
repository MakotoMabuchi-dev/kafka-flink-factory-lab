#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import venv
from tempfile import NamedTemporaryFile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
KAFKA_DIR = REPO_ROOT / "kafka-test"
FLINK_DIR = REPO_ROOT / "flink-test"
TRINO_DIR = REPO_ROOT / "trino-test"
FLINK_SQL_DIR = FLINK_DIR / "sql"
TEMP_VENV_DIR = REPO_ROOT / ".lab-venv"
RUNTIME_DIR = REPO_ROOT / ".lab-runtime"
PRODUCER_LOG = RUNTIME_DIR / "producer.log"
PRODUCER_PID_FILE = RUNTIME_DIR / "producer.pid"
SHARED_NETWORK = "stream-shared"

KAFKA_CONTAINER = "kafka"
FLINK_JOBMANAGER = "flink-jobmanager"
TASKMANAGER = "flink-taskmanager"
MINIO_CONTAINER = "minio"
MC_CONTAINER = "mc"
ICEBERG_REST_CONTAINER = "iceberg-rest"
TRINO_CONTAINER = "trino"

DEFAULT_SQL_JOB = FLINK_SQL_DIR / "job.sql"
DEFAULT_SUMMARY_SQL_JOB = FLINK_SQL_DIR / "job_summary.sql"
DEFAULT_ICEBERG_SQL_JOB = FLINK_SQL_DIR / "job_summary_iceberg.sql"
DEFAULT_ICEBERG_READ_SQL = FLINK_SQL_DIR / "read_iceberg_summary.sql"
DEFAULT_TRINO_READ_QUERY = "SELECT * FROM iceberg.lab.product_summary_iceberg LIMIT 20"
DEFAULT_TRINO_HISTORY_QUERY = 'SELECT * FROM iceberg.lab."product_summary_iceberg$history"'
DEFAULT_TRINO_SNAPSHOTS_QUERY = 'SELECT * FROM iceberg.lab."product_summary_iceberg$snapshots"'


def log(message: str) -> None:
    print(f"[INFO] {message}")


def warn(message: str) -> None:
    print(f"[WARN] {message}", file=sys.stderr)


def fail(message: str) -> None:
    print(f"[ERROR] {message}", file=sys.stderr)
    raise SystemExit(1)


def run(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
    check: bool = True,
    stdin: int | None = subprocess.DEVNULL,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
        stdin=stdin,
        text=True,
        capture_output=capture_output,
        check=check,
    )


def ensure_command(name: str) -> None:
    if shutil.which(name) is None:
        fail(f"Required command not found: {name}")


def venv_python_path(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python3"


def wait_for_docker_daemon() -> None:
    ensure_command("docker")
    log("Waiting for Docker daemon...")

    while True:
        result = run(["docker", "info"], capture_output=True, check=False)
        if result.returncode == 0:
            break
        time.sleep(2)

    log("Docker daemon is ready.")


def ensure_shared_network() -> None:
    result = run(
        ["docker", "network", "inspect", SHARED_NETWORK],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        log(f"Creating shared Docker network: {SHARED_NETWORK}")
        run(["docker", "network", "create", SHARED_NETWORK])


def ensure_bind_mount_dirs() -> None:
    (FLINK_DIR / "minio-data").mkdir(parents=True, exist_ok=True)
    (FLINK_DIR / "iceberg-rest-data").mkdir(parents=True, exist_ok=True)


def ensure_trino_config() -> None:
    if not TRINO_DIR.exists():
        fail(f"Trino directory not found: {TRINO_DIR}")
    compose_file = TRINO_DIR / "docker-compose.yml"
    if not compose_file.exists():
        fail(f"Trino compose file not found: {compose_file}")


def wait_for_container(container_name: str, retries: int = 30) -> None:
    for _ in range(retries):
        result = run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            check=False,
        )
        running = set(result.stdout.splitlines())
        if container_name in running:
            return
        time.sleep(2)
    fail(f"Container '{container_name}' did not start in time.")


def container_is_running(container_name: str) -> bool:
    result = run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True,
        check=False,
    )
    return container_name in set(result.stdout.splitlines())


def ensure_container_running(container_name: str) -> None:
    if not container_is_running(container_name):
        fail(f"Container '{container_name}' is not running.")


def wait_for_kafka_ready(retries: int = 30) -> None:
    log("Waiting for Kafka broker...")
    for _ in range(retries):
        result = run(
            [
                "docker",
                "exec",
                KAFKA_CONTAINER,
                "kafka-topics",
                "--list",
                "--bootstrap-server",
                "localhost:29092",
            ],
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            log("Kafka broker is ready.")
            return
        time.sleep(2)
    fail("Kafka broker did not become ready in time.")


def wait_for_trino_ready(retries: int = 30) -> None:
    log("Waiting for Trino CLI...")
    for _ in range(retries):
        result = run(
            ["docker", "exec", TRINO_CONTAINER, "trino", "--execute", "SELECT 1"],
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            log("Trino is ready.")
            return
        time.sleep(2)
    fail("Trino did not become ready in time.")


def start_stack() -> None:
    wait_for_docker_daemon()
    ensure_shared_network()
    ensure_bind_mount_dirs()

    log("Starting Kafka stack...")
    run(["docker", "compose", "up", "-d"], cwd=KAFKA_DIR)

    log("Starting Flink stack...")
    run(["docker", "compose", "up", "-d", "--build"], cwd=FLINK_DIR)

    log("Waiting for core containers...")
    for container in (
        KAFKA_CONTAINER,
        FLINK_JOBMANAGER,
        TASKMANAGER,
        MINIO_CONTAINER,
        MC_CONTAINER,
        ICEBERG_REST_CONTAINER,
    ):
        wait_for_container(container)

    wait_for_kafka_ready()
    verify_warehouse_bucket()
    create_topics()

    log("Startup completed.")
    print("Flink UI: http://localhost:8081")
    print("MinIO Console: http://localhost:9001")
    print("Iceberg REST catalog: http://localhost:8181")


def stop_stack(*, quiet: bool = False) -> None:
    if not quiet:
        log("Stopping Flink, MinIO, and Iceberg REST stack...")
    run(["docker", "compose", "down"], cwd=FLINK_DIR, check=False)

    if not quiet:
        log("Stopping Kafka stack...")
    run(["docker", "compose", "down"], cwd=KAFKA_DIR, check=False)

    if not quiet:
        log("Stop completed.")


def start_trino() -> None:
    wait_for_docker_daemon()
    ensure_shared_network()
    ensure_trino_config()

    log("Starting Trino...")
    run(["docker", "compose", "up", "-d"], cwd=TRINO_DIR)

    wait_for_container(TRINO_CONTAINER)
    wait_for_trino_ready()

    log("Trino startup completed.")
    print("Trino UI: http://localhost:8080")


def stop_trino(*, quiet: bool = False) -> None:
    ensure_trino_config()
    if not quiet:
        log("Stopping Trino...")
    run(["docker", "compose", "down"], cwd=TRINO_DIR, check=False)
    if not quiet:
        log("Trino stopped.")


def create_topics() -> None:
    log("Creating Kafka topics...")
    for topic in ("sensor-a", "sensor-b", "process-events"):
        run(
            [
                "docker",
                "exec",
                KAFKA_CONTAINER,
                "kafka-topics",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--bootstrap-server",
                "localhost:29092",
            ],
            check=True,
        )


def list_topics() -> None:
    run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "kafka-topics",
            "--list",
            "--bootstrap-server",
            "localhost:29092",
        ]
    )


def verify_warehouse_bucket(retries: int = 30) -> None:
    log("Verifying MinIO warehouse bucket...")
    for _ in range(retries):
        result = run(
            ["docker", "exec", MC_CONTAINER, "/usr/bin/mc", "ls", "local/warehouse"],
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            log("MinIO bucket 'warehouse' is ready.")
            return
        time.sleep(2)

    fail("MinIO bucket 'warehouse' is not available.")


def resolve_sql_path(raw_path: str | None, default_path: Path) -> Path:
    if raw_path is None:
        return default_path

    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


def copy_sql_to_container(local_path: Path, container_path: str) -> None:
    if not local_path.exists():
        fail(f"SQL file not found: {local_path}")
    run(["docker", "cp", str(local_path), f"{FLINK_JOBMANAGER}:{container_path}"])


def execute_sql_file(
    local_path: Path,
    container_path: str,
    *,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    ensure_container_running(FLINK_JOBMANAGER)
    copy_sql_to_container(local_path, container_path)
    return run(
        ["docker", "exec", FLINK_JOBMANAGER, "./bin/sql-client.sh", "-f", container_path],
        capture_output=capture_output,
    )


def execute_sql_text(
    sql_text: str,
    container_path: str,
    *,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    RUNTIME_DIR.mkdir(exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", suffix=".sql", dir=RUNTIME_DIR, delete=False) as temp_file:
        temp_file.write(sql_text)
        temp_path = Path(temp_file.name)

    try:
        return execute_sql_file(temp_path, container_path, capture_output=capture_output)
    finally:
        temp_path.unlink(missing_ok=True)


def apply_sql(sql_path: Path) -> None:
    log(f"Applying Flink SQL job: {sql_path}")
    execute_sql_file(sql_path, "/opt/flink/job.sql")


def read_iceberg(sql_path: Path) -> None:
    log(f"Reading Iceberg summary with: {sql_path}")
    if sql_path != DEFAULT_ICEBERG_READ_SQL:
        execute_sql_file(sql_path, "/opt/flink/read_iceberg_summary.sql")
        return

    inspect_sql = """SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG lakehouse WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://warehouse/',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key-id' = 'minioadmin',
    's3.secret-access-key' = 'minioadmin',
    's3.path-style-access' = 'true',
    'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lakehouse.lab;
USE CATALOG lakehouse;
USE lab;
SHOW TABLES;
"""
    inspect_result = execute_sql_text(
        inspect_sql,
        "/opt/flink/read_iceberg_inspect.sql",
        capture_output=True,
    )
    if inspect_result.stdout:
        print(inspect_result.stdout, end="")

    if "product_summary_iceberg" not in inspect_result.stdout:
        warn("Iceberg table 'product_summary_iceberg' does not exist yet. Run 'run iceberg' first.")
        return

    execute_sql_file(sql_path, "/opt/flink/read_iceberg_summary.sql")


def load_iceberg_catalog_entries() -> list[sqlite3.Row]:
    catalog_db = FLINK_DIR / "iceberg-rest-data" / "catalog.db"
    if not catalog_db.exists():
        return []

    with sqlite3.connect(catalog_db) as connection:
        connection.row_factory = sqlite3.Row
        return list(
            connection.execute(
                """
                SELECT
                    catalog_name,
                    table_namespace,
                    table_name,
                    metadata_location,
                    previous_metadata_location,
                    iceberg_type
                FROM iceberg_tables
                ORDER BY table_namespace, table_name
                """
            )
        )


def s3_uri_to_mc_path(s3_uri: str) -> str | None:
    if not s3_uri.startswith("s3://"):
        return None
    return f"local/{s3_uri.removeprefix('s3://')}"


def read_minio_text(mc_path: str) -> str | None:
    if not container_is_running(MC_CONTAINER):
        return None

    result = run(
        ["docker", "exec", MC_CONTAINER, "/usr/bin/mc", "cat", mc_path],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout


def list_minio_objects(mc_prefix: str) -> list[str]:
    if not container_is_running(MC_CONTAINER):
        return []

    result = run(
        ["docker", "exec", MC_CONTAINER, "/usr/bin/mc", "find", mc_prefix],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return []
    return [line for line in result.stdout.splitlines() if line.strip()]


def inspect_iceberg() -> None:
    minio_data_dir = FLINK_DIR / "minio-data"
    catalog_db = FLINK_DIR / "iceberg-rest-data" / "catalog.db"

    print("=== Iceberg Persistence ===")
    print(f"MinIO data dir: {minio_data_dir}")
    print(f"  exists: {'yes' if minio_data_dir.exists() else 'no'}")
    print(f"Catalog DB: {catalog_db}")
    print(f"  exists: {'yes' if catalog_db.exists() else 'no'}")

    try:
        entries = load_iceberg_catalog_entries()
    except sqlite3.Error as exc:
        warn(f"Failed to read Iceberg catalog DB: {exc}")
        return

    print()
    print("=== Iceberg Catalog ===")
    if not entries:
        print("No Iceberg tables are registered yet. Run 'run iceberg' first.")
        return

    for entry in entries:
        table_prefix = f"local/warehouse/{entry['table_namespace']}/{entry['table_name']}"
        print(f"Table: {entry['table_namespace']}.{entry['table_name']} ({entry['iceberg_type']})")
        print(f"Catalog: {entry['catalog_name']}")
        print(f"Metadata: {entry['metadata_location']}")
        if entry["previous_metadata_location"]:
            print(f"Previous metadata: {entry['previous_metadata_location']}")

        metadata_location = entry["metadata_location"] or ""
        metadata_path = s3_uri_to_mc_path(metadata_location)
        metadata_text = read_minio_text(metadata_path) if metadata_path else None
        if metadata_text is not None:
            try:
                metadata = json.loads(metadata_text)
            except json.JSONDecodeError as exc:
                warn(f"Failed to parse Iceberg metadata JSON: {exc}")
            else:
                properties = metadata.get("properties", {})
                snapshots = metadata.get("snapshots", [])
                current_snapshot_id = metadata.get("current-snapshot-id")
                current_snapshot = next(
                    (item for item in snapshots if item.get("snapshot-id") == current_snapshot_id),
                    None,
                )
                current_summary = current_snapshot.get("summary", {}) if current_snapshot else {}

                print(
                    "Format: "
                    f"v{metadata.get('format-version')} / "
                    f"{properties.get('write.format.default', 'unknown')}"
                )
                print(f"Current snapshot: {current_snapshot_id}")
                print(f"Snapshots: {len(snapshots)}")
                if current_summary:
                    print(
                        "Current totals: "
                        f"records={current_summary.get('total-records', '?')} "
                        f"data_files={current_summary.get('total-data-files', '?')} "
                        f"delete_files={current_summary.get('total-delete-files', '?')}"
                    )
                    print(
                        "Last commit: "
                        f"operation={current_summary.get('operation', '?')} "
                        f"checkpoint={current_summary.get('flink.max-committed-checkpoint-id', '?')} "
                        f"job={current_summary.get('flink.job-id', '?')}"
                    )

        objects = list_minio_objects(table_prefix)
        if objects:
            print("Objects:")
            for object_path in objects:
                print(f"  {object_path}")
        else:
            print("Objects: not available yet.")

        print()

    print("Hint: use 'read-iceberg' to print the stored rows.")


def trino_is_ready() -> bool:
    if not container_is_running(TRINO_CONTAINER):
        return False
    result = run(
        ["docker", "exec", TRINO_CONTAINER, "trino", "--execute", "SELECT 1"],
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def show_trino_status() -> None:
    print("=== Trino ===")
    if trino_is_ready():
        print("RUNNING (ready, http://localhost:8080)")
        return
    if container_is_running(TRINO_CONTAINER):
        print("STARTING")
        return
    print("STOPPED")


def run_trino_query(sql_text: str, *, check: bool = True) -> int:
    ensure_container_running(TRINO_CONTAINER)
    result = run(
        ["docker", "exec", TRINO_CONTAINER, "trino", "--execute", sql_text],
        check=False,
    )
    if check and result.returncode != 0:
        fail("Trino query failed.")
    return result.returncode


def open_trino_shell() -> int:
    ensure_container_running(TRINO_CONTAINER)
    result = run(
        ["docker", "exec", "-it", TRINO_CONTAINER, "trino", "--catalog", "iceberg", "--schema", "lab"],
        check=False,
        stdin=None,
    )
    return result.returncode


def watch_topic(topic: str, *, from_beginning: bool = False, max_messages: int | None = None) -> None:
    if topic == "all":
        list_topics()
        return

    command = [
        "docker",
        "exec",
        KAFKA_CONTAINER,
        "kafka-console-consumer",
        "--topic",
        topic,
        "--bootstrap-server",
        "localhost:29092",
    ]
    if from_beginning:
        command.append("--from-beginning")
    if max_messages is not None:
        command.extend(["--max-messages", str(max_messages), "--timeout-ms", "5000"])
    run(command)


def flink_list(capture_output: bool = False) -> subprocess.CompletedProcess[str]:
    return run(
        ["docker", "exec", FLINK_JOBMANAGER, "./bin/flink", "list"],
        capture_output=capture_output,
        check=False,
    )


def cancel_running_jobs() -> None:
    result = flink_list(capture_output=True)
    if result.returncode != 0:
        return

    job_ids: list[str] = []
    for line in result.stdout.splitlines():
        if " : " in line and "(RUNNING)" in line:
            parts = line.split(" : ")
            if len(parts) >= 2:
                job_ids.append(parts[1].strip())

    if not job_ids:
        return

    log("Canceling running Flink jobs...")
    for job_id in job_ids:
        run(["docker", "exec", FLINK_JOBMANAGER, "./bin/flink", "cancel", job_id], check=False)


def setup_temp_python_env() -> Path:
    RUNTIME_DIR.mkdir(exist_ok=True)
    if not TEMP_VENV_DIR.exists():
        log("Creating temporary Python virtual environment...")
        venv.EnvBuilder(with_pip=True).create(str(TEMP_VENV_DIR))

    python_path = venv_python_path(TEMP_VENV_DIR)
    check_result = run(
        [str(python_path), "-c", "import kafka"],
        capture_output=True,
        check=False,
    )
    if check_result.returncode == 0:
        return python_path

    log("Installing Python dependencies...")
    run(
        [
            str(python_path),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--retries",
            "0",
            "-r",
            str(REPO_ROOT / "requirements.txt"),
        ]
    )
    return python_path


class LabConsole:
    def __init__(self) -> None:
        self.cleanup_done = False
        self.current_difficulty = "IDEAL"
        self.producer_process: subprocess.Popen[str] | None = None
        self.python_path: Path | None = None
        self.stack_started = False
        self.console_input = sys.stdin
        self.console_output = sys.stdout
        self.producer_log_handle: os.TextIOWrapper | None = None

    def cleanup(self) -> None:
        if self.cleanup_done:
            return
        self.cleanup_done = True

        print()
        log("Cleaning up lab environment...")
        self.stop_producer()
        stop_trino(quiet=True)
        stop_stack(quiet=True)
        shutil.rmtree(FLINK_DIR / "minio-data", ignore_errors=True)
        shutil.rmtree(FLINK_DIR / "iceberg-rest-data", ignore_errors=True)
        shutil.rmtree(TEMP_VENV_DIR, ignore_errors=True)
        shutil.rmtree(RUNTIME_DIR, ignore_errors=True)
        self.close_console_streams()
        log("Lab environment removed.")

    def setup_console_streams(self) -> None:
        if os.name == "nt":
            input_path = "CONIN$"
            output_path = "CONOUT$"
        else:
            input_path = "/dev/tty"
            output_path = "/dev/tty"

        try:
            self.console_input = open(input_path, "r", encoding="utf-8", errors="replace")
            self.console_output = open(output_path, "w", encoding="utf-8", errors="replace", buffering=1)
        except OSError:
            self.console_input = sys.stdin
            self.console_output = sys.stdout

        if not self.console_input.isatty():
            fail("The console command requires an interactive terminal.")

    def close_console_streams(self) -> None:
        if self.console_input not in {sys.stdin, None}:
            self.console_input.close()
        if self.console_output not in {sys.stdout, None}:
            self.console_output.close()

    def prompt(self, message: str) -> str:
        self.console_output.write(message)
        self.console_output.flush()
        raw_value = self.console_input.readline()
        if raw_value == "":
            raise EOFError
        return raw_value.rstrip("\n")

    def set_difficulty(self, raw_value: str) -> bool:
        normalized = raw_value.strip().upper()
        allowed = {"IDEAL", "BASIC_DISTURBANCE", "REALISTIC", "HARSH"}
        if normalized not in allowed:
            warn("Unknown difficulty. Use IDEAL, BASIC_DISTURBANCE, REALISTIC, or HARSH.")
            return False
        self.current_difficulty = normalized
        return True

    def start_producer(self) -> bool:
        self.stop_producer()
        if self.python_path is None:
            try:
                self.python_path = setup_temp_python_env()
            except subprocess.CalledProcessError as exc:
                warn("Python producer dependencies could not be prepared. Producer commands are unavailable.")
                warn(f"Failed command: {' '.join(exc.cmd)}")
                return False

        log(f"Starting producer with difficulty {self.current_difficulty}...")
        env = os.environ.copy()
        env["FACTORY_DIFFICULTY"] = self.current_difficulty
        RUNTIME_DIR.mkdir(exist_ok=True)
        for _ in range(5):
            self.producer_log_handle = PRODUCER_LOG.open("w", encoding="utf-8")
            self.producer_process = subprocess.Popen(
                [str(self.python_path), str(REPO_ROOT / "python-producer" / "send_factory_data.py")],
                stdin=subprocess.DEVNULL,
                stdout=self.producer_log_handle,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
            PRODUCER_PID_FILE.write_text(str(self.producer_process.pid), encoding="utf-8")
            time.sleep(2)

            if self.producer_process.poll() is None:
                return True

            self.close_producer_log_handle()
            self.producer_process = None
            if PRODUCER_PID_FILE.exists():
                PRODUCER_PID_FILE.unlink()
            time.sleep(2)

        warn(f"Producer failed to start. See {PRODUCER_LOG}")
        return False

    def close_producer_log_handle(self) -> None:
        if self.producer_log_handle is not None:
            self.producer_log_handle.close()
            self.producer_log_handle = None

    def stop_producer(self) -> None:
        if self.producer_process is None:
            self.close_producer_log_handle()
            return
        if self.producer_process.poll() is None:
            self.producer_process.terminate()
            try:
                self.producer_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.producer_process.kill()
                self.producer_process.wait(timeout=5)
        self.producer_process = None
        if PRODUCER_PID_FILE.exists():
            PRODUCER_PID_FILE.unlink()
        self.close_producer_log_handle()

    def show_status(self) -> None:
        print()
        print("=== Containers ===")
        run(["docker", "ps", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"], check=False)
        print()
        print("=== Flink Jobs ===")
        flink_list()
        print()
        print("=== Producer ===")
        if self.producer_process and self.producer_process.poll() is None:
            print(f"RUNNING ({self.current_difficulty})")
        else:
            print("STOPPED")
        print()
        show_trino_status()

    def show_producer_log(self) -> None:
        self.print_tail(PRODUCER_LOG)

    def print_tail(self, path: Path) -> None:
        if not path.exists():
            log("Log file not found yet.")
            return
        lines = path.read_text(encoding="utf-8").splitlines()
        for line in lines[-30:]:
            print(line)

    def print_console_help(self) -> None:
        print()
        print("Available commands:")
        print("  help")
        print("    Show this help.")
        print("  status")
        print("    Show containers, Flink jobs, and producer status.")
        print("  topics")
        print("    List Kafka topics.")
        print("  watch <topic> [count]")
        print("    Show Kafka messages from the beginning. Example: watch process-events 5")
        print("  run detail")
        print("    Run the detail print job. Check the result with flink-log.")
        print("  run summary")
        print("    Run the summary print job. Check the result with flink-log.")
        print("  run iceberg")
        print("    Run the Iceberg summary job.")
        print("  flink-log")
        print("    Show the latest TaskManager log. print sink output appears here.")
        print("  producer-log")
        print("    Show the latest producer log.")
        print("  producer-start [difficulty]")
        print("    Start the producer. Example: producer-start REALISTIC")
        print("  producer-stop")
        print("    Stop the producer.")
        print("  difficulty")
        print("    Show the current producer difficulty.")
        print("  difficulty <level>")
        print("    Restart the producer with IDEAL, BASIC_DISTURBANCE, REALISTIC, or HARSH.")
        print("  jobs")
        print("    Show Flink jobs.")
        print("  cancel-jobs")
        print("    Cancel all running Flink jobs.")
        print("  iceberg-files")
        print("    Show files stored in MinIO for Iceberg.")
        print("  iceberg-log")
        print("    Show the latest Iceberg REST log.")
        print("  inspect-iceberg")
        print("    Show Iceberg catalog metadata, snapshots, and MinIO objects.")
        print("  read-iceberg")
        print("    Query product_summary_iceberg from Iceberg.")
        print("  trino-status")
        print("    Show whether the Trino container is stopped, starting, or ready.")
        print("  trino-start")
        print("    Start the Trino container connected to the shared lab network.")
        print("  trino-stop")
        print("    Stop the Trino container.")
        print("  trino-shell")
        print("    Open the interactive Trino CLI with catalog=iceberg schema=lab.")
        print("  trino-read")
        print("    Query product_summary_iceberg through Trino.")
        print("  trino-history")
        print('    Query iceberg.lab."product_summary_iceberg$history" through Trino.')
        print("  trino-snapshots")
        print('    Query iceberg.lab."product_summary_iceberg$snapshots" through Trino.')
        print("  trino-query <SQL>")
        print("    Run arbitrary SQL through Trino. Quote the SQL when it contains spaces or $.")
        print("  exit")
        print("    Stop and remove the lab environment.")

    def handle_watch_command(self, args: list[str]) -> None:
        topic = args[0] if args else "process-events"
        count = 5
        if len(args) >= 2:
            try:
                count = int(args[1])
            except ValueError:
                warn("Message count must be an integer.")
                return
        watch_topic(topic, from_beginning=True, max_messages=count)

    def handle_run_command(self, args: list[str]) -> None:
        if not args:
            warn("Specify one of: detail, summary, iceberg")
            return

        job_name = args[0].lower()
        if job_name == "detail":
            cancel_running_jobs()
            apply_sql(DEFAULT_SQL_JOB)
            return
        if job_name == "summary":
            cancel_running_jobs()
            apply_sql(DEFAULT_SUMMARY_SQL_JOB)
            return
        if job_name == "iceberg":
            cancel_running_jobs()
            apply_sql(DEFAULT_ICEBERG_SQL_JOB)
            return

        warn("Unknown job. Use: run detail, run summary, or run iceberg.")

    def execute_command(self, parts: list[str]) -> bool:
        command = parts[0].lower()
        args = parts[1:]

        if command in {"exit", "quit"}:
            return False
        if command in {"help", "?"}:
            self.print_console_help()
            return True
        if command == "status":
            self.show_status()
            return True
        if command == "topics":
            list_topics()
            return True
        if command == "watch":
            self.handle_watch_command(args)
            return True
        if command == "run":
            self.handle_run_command(args)
            return True
        if command in {"flink-log", "taskmanager-log"}:
            run(["docker", "logs", "--tail", "80", TASKMANAGER], check=False)
            return True
        if command == "producer-log":
            self.show_producer_log()
            return True
        if command == "producer-start":
            if args and not self.set_difficulty(args[0]):
                return True
            self.start_producer()
            return True
        if command == "producer-stop":
            self.stop_producer()
            return True
        if command == "difficulty":
            if not args:
                print(self.current_difficulty)
                return True
            if self.set_difficulty(args[0]):
                self.start_producer()
            return True
        if command == "jobs":
            flink_list()
            return True
        if command == "cancel-jobs":
            cancel_running_jobs()
            return True
        if command == "iceberg-files":
            run(["docker", "exec", MC_CONTAINER, "/usr/bin/mc", "find", "local/warehouse"], check=False)
            return True
        if command == "iceberg-log":
            run(["docker", "logs", "--tail", "80", ICEBERG_REST_CONTAINER], check=False)
            return True
        if command == "inspect-iceberg":
            inspect_iceberg()
            return True
        if command == "read-iceberg":
            read_iceberg(DEFAULT_ICEBERG_READ_SQL)
            return True
        if command == "trino-status":
            show_trino_status()
            return True
        if command == "trino-start":
            start_trino()
            return True
        if command == "trino-stop":
            stop_trino()
            return True
        if command == "trino-shell":
            open_trino_shell()
            return True
        if command == "trino-read":
            run_trino_query(DEFAULT_TRINO_READ_QUERY, check=False)
            return True
        if command == "trino-history":
            run_trino_query(DEFAULT_TRINO_HISTORY_QUERY, check=False)
            return True
        if command == "trino-snapshots":
            run_trino_query(DEFAULT_TRINO_SNAPSHOTS_QUERY, check=False)
            return True
        if command == "trino-query":
            if not args:
                warn("Specify SQL after 'trino-query'.")
                return True
            run_trino_query(" ".join(args), check=False)
            return True

        warn("Unknown command. Type 'help' to see the available commands.")
        return True

    def interactive_shell(self) -> None:
        print()
        print("Kafka Flink Factory Lab Console is ready.")
        print(f"Producer difficulty: {self.current_difficulty}")
        print("Type 'help' to see the available commands.")
        print("Type 'exit' to stop and remove the environment.")

        while True:
            try:
                raw_command = self.prompt("lab> ").strip()
            except EOFError:
                self.console_output.write("\n")
                self.console_output.flush()
                warn("Input stream was closed. Exiting the lab console.")
                return

            if not raw_command:
                continue

            try:
                parts = shlex.split(raw_command)
            except ValueError as exc:
                warn(f"Invalid command syntax: {exc}")
                continue

            if not self.execute_command(parts):
                return

    def run(self) -> None:
        self.setup_console_streams()

        stop_trino(quiet=True)
        stop_stack(quiet=True)
        shutil.rmtree(FLINK_DIR / "minio-data", ignore_errors=True)
        shutil.rmtree(FLINK_DIR / "iceberg-rest-data", ignore_errors=True)
        shutil.rmtree(RUNTIME_DIR, ignore_errors=True)

        self.stack_started = True
        start_stack()
        start_trino()
        if not self.start_producer():
            warn("Console started without the Python producer. Use 'producer-start' to retry later.")
        self.print_console_help()
        self.interactive_shell()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Kafka Flink Factory Lab CLI",
        epilog=(
            "Examples:\n"
            "  python3 scripts/lab_cli.py console\n"
            "  python3 scripts/lab_cli.py start\n"
            "  python3 scripts/lab_cli.py trino-start\n"
            "  python3 scripts/lab_cli.py apply-sql --file flink-test/sql/job_summary_iceberg.sql\n"
            "  python3 scripts/lab_cli.py read-iceberg\n"
            "  python3 scripts/lab_cli.py trino-query SELECT count(*) FROM iceberg.lab.product_summary_iceberg"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("start", help="Start Kafka, Flink, MinIO, and Iceberg REST")
    subparsers.add_parser("stop", help="Stop Kafka, Flink, MinIO, and Iceberg REST")
    subparsers.add_parser("create-topics", help="Create Kafka topics")
    subparsers.add_parser("status", help="Show running Docker containers and Flink jobs")
    subparsers.add_parser("cancel-jobs", help="Cancel all running Flink jobs")

    apply_sql_parser = subparsers.add_parser("apply-sql", help="Apply a Flink SQL file")
    apply_sql_parser.add_argument("--file", dest="sql_file")

    subparsers.add_parser("inspect-iceberg", help="Show Iceberg catalog metadata and storage files")

    read_parser = subparsers.add_parser("read-iceberg", help="Read Iceberg summary")
    read_parser.add_argument("--file", dest="sql_file")

    subparsers.add_parser("trino-start", help="Start Trino")
    subparsers.add_parser("trino-stop", help="Stop Trino")
    subparsers.add_parser("trino-status", help="Show whether Trino is stopped, starting, or ready")
    subparsers.add_parser("trino-shell", help="Open the interactive Trino CLI")
    subparsers.add_parser("trino-read", help="Query product_summary_iceberg through Trino")
    subparsers.add_parser("trino-history", help='Query iceberg.lab."product_summary_iceberg$history"')
    subparsers.add_parser("trino-snapshots", help='Query iceberg.lab."product_summary_iceberg$snapshots"')

    trino_query_parser = subparsers.add_parser("trino-query", help="Run SQL through Trino")
    trino_query_parser.add_argument("sql", nargs=argparse.REMAINDER)

    watch_parser = subparsers.add_parser("watch-topic", help="Read Kafka topic messages")
    watch_parser.add_argument("topic", nargs="?", default="all")
    watch_parser.add_argument("--from-beginning", action="store_true")
    watch_parser.add_argument("--max-messages", type=int)

    producer_parser = subparsers.add_parser("producer", help="Run the Python producer in foreground")
    producer_parser.add_argument(
        "--difficulty",
        choices=["IDEAL", "BASIC_DISTURBANCE", "REALISTIC", "HARSH"],
        default="IDEAL",
    )

    subparsers.add_parser("console", help="Run the interactive lab console")
    return parser


def show_status() -> None:
    run(["docker", "ps", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"], check=False)
    print()
    flink_list()
    print()
    show_trino_status()


def run_foreground_producer(difficulty: str) -> None:
    env = os.environ.copy()
    env["FACTORY_DIFFICULTY"] = difficulty
    run([sys.executable, str(REPO_ROOT / "python-producer" / "send_factory_data.py")], env=env)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        print()
        print("Suggested first command: python3 scripts/lab_cli.py console")
        return

    if args.command == "start":
        start_stack()
        return

    if args.command == "stop":
        stop_stack()
        return

    if args.command == "create-topics":
        create_topics()
        return

    if args.command == "status":
        show_status()
        return

    if args.command == "cancel-jobs":
        cancel_running_jobs()
        return

    if args.command == "apply-sql":
        sql_path = resolve_sql_path(args.sql_file, DEFAULT_SQL_JOB)
        apply_sql(sql_path)
        return

    if args.command == "inspect-iceberg":
        inspect_iceberg()
        return

    if args.command == "read-iceberg":
        sql_path = resolve_sql_path(args.sql_file, DEFAULT_ICEBERG_READ_SQL)
        read_iceberg(sql_path)
        return

    if args.command == "trino-start":
        start_trino()
        return

    if args.command == "trino-stop":
        stop_trino()
        return

    if args.command == "trino-status":
        show_trino_status()
        return

    if args.command == "trino-shell":
        raise SystemExit(open_trino_shell())

    if args.command == "trino-read":
        raise SystemExit(run_trino_query(DEFAULT_TRINO_READ_QUERY, check=False))

    if args.command == "trino-history":
        raise SystemExit(run_trino_query(DEFAULT_TRINO_HISTORY_QUERY, check=False))

    if args.command == "trino-snapshots":
        raise SystemExit(run_trino_query(DEFAULT_TRINO_SNAPSHOTS_QUERY, check=False))

    if args.command == "trino-query":
        if not args.sql:
            fail("Provide SQL after 'trino-query'.")
        raise SystemExit(run_trino_query(" ".join(args.sql), check=False))

    if args.command == "watch-topic":
        watch_topic(args.topic, from_beginning=args.from_beginning, max_messages=args.max_messages)
        return

    if args.command == "producer":
        run_foreground_producer(args.difficulty)
        return

    if args.command == "console":
        console = LabConsole()
        try:
            console.run()
        except KeyboardInterrupt:
            print()
            log("Interrupted.")
        finally:
            console.cleanup()
        return

    fail(f"Unknown command: {args.command}")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.default_int_handler)
    main()
