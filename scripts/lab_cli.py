#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
import venv
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
KAFKA_DIR = REPO_ROOT / "kafka-test"
FLINK_DIR = REPO_ROOT / "flink-test"
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

DEFAULT_SQL_JOB = FLINK_SQL_DIR / "job.sql"
DEFAULT_SUMMARY_SQL_JOB = FLINK_SQL_DIR / "job_summary.sql"
DEFAULT_ICEBERG_SQL_JOB = FLINK_SQL_DIR / "job_summary_iceberg.sql"
DEFAULT_ICEBERG_READ_SQL = FLINK_SQL_DIR / "read_iceberg_summary.sql"


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
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
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


def ensure_container_running(container_name: str) -> None:
    result = run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True,
        check=False,
    )
    if container_name not in set(result.stdout.splitlines()):
        fail(f"Container '{container_name}' is not running.")


def start_stack() -> None:
    wait_for_docker_daemon()
    ensure_shared_network()

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


def create_topics() -> None:
    log("Creating Kafka topics...")
    for topic in ("sensor-a", "sensor-b", "process-events"):
        run(
            [
                "docker",
                "exec",
                "-i",
                KAFKA_CONTAINER,
                "kafka-topics",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--bootstrap-server",
                "localhost:29092",
            ],
            check=False,
        )


def list_topics() -> None:
    run(
        [
            "docker",
            "exec",
            "-i",
            KAFKA_CONTAINER,
            "kafka-topics",
            "--list",
            "--bootstrap-server",
            "localhost:29092",
        ]
    )


def verify_warehouse_bucket() -> None:
    log("Verifying MinIO warehouse bucket...")
    result = run(
        ["docker", "exec", MC_CONTAINER, "/usr/bin/mc", "ls", "local/warehouse"],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        fail("MinIO bucket 'warehouse' is not available.")
    log("MinIO bucket 'warehouse' is ready.")


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


def execute_sql_file(local_path: Path, container_path: str) -> None:
    ensure_container_running(FLINK_JOBMANAGER)
    copy_sql_to_container(local_path, container_path)
    run(["docker", "exec", "-i", FLINK_JOBMANAGER, "./bin/sql-client.sh", "-f", container_path])


def apply_sql(sql_path: Path) -> None:
    log(f"Applying Flink SQL job: {sql_path}")
    execute_sql_file(sql_path, "/opt/flink/job.sql")


def read_iceberg(sql_path: Path) -> None:
    log(f"Reading Iceberg summary with: {sql_path}")
    execute_sql_file(sql_path, "/opt/flink/read_iceberg_summary.sql")


def watch_topic(topic: str, *, from_beginning: bool = False, max_messages: int | None = None) -> None:
    if topic == "all":
        list_topics()
        return

    command = [
        "docker",
        "exec",
        "-i",
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
    log("Installing Python dependencies...")
    run([str(python_path), "-m", "pip", "install", "--upgrade", "pip"])
    run([str(python_path), "-m", "pip", "install", "-r", str(REPO_ROOT / "requirements.txt")])
    return python_path


class LabConsole:
    def __init__(self) -> None:
        self.cleanup_done = False
        self.current_difficulty = "IDEAL"
        self.producer_process: subprocess.Popen[str] | None = None
        self.python_path: Path | None = None

    def cleanup(self) -> None:
        if self.cleanup_done:
            return
        self.cleanup_done = True

        print()
        log("Cleaning up lab environment...")
        self.stop_producer()
        stop_stack(quiet=True)
        shutil.rmtree(FLINK_DIR / "minio-data", ignore_errors=True)
        shutil.rmtree(FLINK_DIR / "iceberg-rest-data", ignore_errors=True)
        shutil.rmtree(TEMP_VENV_DIR, ignore_errors=True)
        shutil.rmtree(RUNTIME_DIR, ignore_errors=True)
        log("Lab environment removed.")

    def select_difficulty(self) -> None:
        print()
        print("Select producer difficulty:")
        print("  1) IDEAL")
        print("  2) BASIC_DISTURBANCE")
        print("  3) REALISTIC")
        print("  4) HARSH")
        choice = input("> ").strip()

        mapping = {
            "1": "IDEAL",
            "2": "BASIC_DISTURBANCE",
            "3": "REALISTIC",
            "4": "HARSH",
            "": "IDEAL",
        }
        self.current_difficulty = mapping.get(choice, "IDEAL")
        if choice not in mapping:
            warn("Unknown selection. Using IDEAL.")

    def start_producer(self) -> None:
        self.stop_producer()
        assert self.python_path is not None

        log(f"Starting producer with difficulty {self.current_difficulty}...")
        env = os.environ.copy()
        env["FACTORY_DIFFICULTY"] = self.current_difficulty
        RUNTIME_DIR.mkdir(exist_ok=True)
        log_file = PRODUCER_LOG.open("w", encoding="utf-8")
        self.producer_process = subprocess.Popen(
            [str(self.python_path), str(REPO_ROOT / "python-producer" / "send_factory_data.py")],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        PRODUCER_PID_FILE.write_text(str(self.producer_process.pid), encoding="utf-8")
        time.sleep(2)

        if self.producer_process.poll() is not None:
            fail(f"Producer failed to start. See {PRODUCER_LOG}")

    def stop_producer(self) -> None:
        if self.producer_process is None:
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

    def show_producer_log(self) -> None:
        if PRODUCER_LOG.exists():
            print(PRODUCER_LOG.read_text(encoding="utf-8").splitlines()[-30:])
        else:
            log("Producer log not found yet.")

    def print_tail(self, path: Path) -> None:
        if not path.exists():
            log("Log file not found yet.")
            return
        lines = path.read_text(encoding="utf-8").splitlines()
        for line in lines[-30:]:
            print(line)

    def menu(self) -> None:
        while True:
            print()
            print("========================================")
            print("Kafka Flink Factory Lab Console")
            print("========================================")
            print("1) Show system status")
            print("2) Restart producer with different difficulty")
            print("3) Show latest producer log")
            print("4) Run detail print job")
            print("5) Run summary print job")
            print("6) Run Iceberg summary job")
            print("7) Read Iceberg summary")
            print("8) Show sample Kafka messages")
            print("9) Show MinIO Iceberg files")
            print("10) Show latest TaskManager log")
            print("11) Show latest Iceberg REST log")
            print("0) Exit and remove the environment")
            choice = input("> ").strip()
            print()

            if choice == "1":
                self.show_status()
            elif choice == "2":
                self.select_difficulty()
                self.start_producer()
            elif choice == "3":
                self.print_tail(PRODUCER_LOG)
            elif choice == "4":
                cancel_running_jobs()
                apply_sql(DEFAULT_SQL_JOB)
            elif choice == "5":
                cancel_running_jobs()
                apply_sql(DEFAULT_SUMMARY_SQL_JOB)
            elif choice == "6":
                cancel_running_jobs()
                apply_sql(DEFAULT_ICEBERG_SQL_JOB)
            elif choice == "7":
                read_iceberg(DEFAULT_ICEBERG_READ_SQL)
            elif choice == "8":
                topic = input("Select topic [sensor-a / sensor-b / process-events, default: process-events]\n> ").strip() or "process-events"
                raw_count = input("How many messages? [default: 5]\n> ").strip() or "5"
                watch_topic(topic, from_beginning=True, max_messages=int(raw_count))
            elif choice == "9":
                run(["docker", "exec", MC_CONTAINER, "/usr/bin/mc", "find", "local/warehouse"], check=False)
            elif choice == "10":
                run(["docker", "logs", "--tail", "80", TASKMANAGER], check=False)
            elif choice == "11":
                run(["docker", "logs", "--tail", "80", ICEBERG_REST_CONTAINER], check=False)
            elif choice == "0":
                return
            else:
                warn("Unknown menu.")

    def run(self) -> None:
        self.python_path = setup_temp_python_env()
        start_stack()
        self.select_difficulty()
        self.start_producer()
        self.menu()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kafka Flink Factory Lab CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("start", help="Start Kafka, Flink, MinIO, and Iceberg REST")
    subparsers.add_parser("stop", help="Stop Kafka, Flink, MinIO, and Iceberg REST")
    subparsers.add_parser("create-topics", help="Create Kafka topics")
    subparsers.add_parser("status", help="Show running Docker containers and Flink jobs")
    subparsers.add_parser("cancel-jobs", help="Cancel all running Flink jobs")

    apply_sql_parser = subparsers.add_parser("apply-sql", help="Apply a Flink SQL file")
    apply_sql_parser.add_argument("--file", dest="sql_file")

    read_parser = subparsers.add_parser("read-iceberg", help="Read Iceberg summary")
    read_parser.add_argument("--file", dest="sql_file")

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


def run_foreground_producer(difficulty: str) -> None:
    env = os.environ.copy()
    env["FACTORY_DIFFICULTY"] = difficulty
    run([sys.executable, str(REPO_ROOT / "python-producer" / "send_factory_data.py")], env=env)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

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

    if args.command == "read-iceberg":
        sql_path = resolve_sql_path(args.sql_file, DEFAULT_ICEBERG_READ_SQL)
        read_iceberg(sql_path)
        return

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
