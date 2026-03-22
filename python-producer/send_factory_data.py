#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Role:
    工場設備を模した擬似データを Kafka に送信する学習用スクリプト。

Do:
    - 1つの「真の内部状態」をベースに、3種類の Kafka イベントを生成する
        1. sensor-a       : 工程1側センサー
        2. sensor-b       : 工程2側センサー
        3. process-events : 製品シリアルごとの工程開始/終了イベント
    - 3スレッドで並列送信する
    - 難易度設定に応じて、停止・欠損・遅延などを段階的に追加する

Don't:
    - 実設備や PLC と通信しない
    - 現実の全異常を再現しない
    - 3つのデータが互いに矛盾するような独立乱数生成はしない

Design Policy:
    - まず「真の設備状態 / 製品進行」を内部で管理し、
      そこから各 topic 用イベントを派生させる
    - process-events を時系列の基準軸とする
    - sensor-a / sensor-b は製品シリアルを持たない独立センサーデータだが、
      工程区間や設備状態と矛盾しない値を生成する
    - 難易度設定で徐々に現実的な乱れを追加できるようにする

Topics:
    - sensor-a
    - sensor-b
    - process-events

Kafka 接続:
    - Mac から Kafka へ localhost:29092 で接続する前提

各 topic のサンプル:
    sensor-a:
        {
          "sensor_id": "A",
          "event_time_str": "2026-03-22T10:00:01.123456",
          "value_temp": 31.2,
          "value_vibration": 0.42,
          "unit": "machine-01",
          "status": "RUNNING"
        }

    sensor-b:
        {
          "sensor_code": "B-01",
          "ts_str": "2026-03-22T10:00:02.456789",
          "pressure": 102.4,
          "current": 8.7,
          "line": "machine-01",
          "status": "RUNNING"
        }

    process-events:
        {
          "serial_no": "SN00000123",
          "process": "process_1",
          "event_type": "start",
          "event_time_str": "2026-03-22T10:00:00.000000",
          "equipment_id": "machine-01",
          "reason": null
        }
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional

from kafka import KafkaProducer


# =========================================
# 基本設定
# =========================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"

TOPIC_SENSOR_A = "sensor-a"
TOPIC_SENSOR_B = "sensor-b"
TOPIC_PROCESS_EVENTS = "process-events"

EQUIPMENT_ID = "machine-01"

STOP_EVENT = threading.Event()


# =========================================
# 難易度設定
# =========================================

class Difficulty(str, Enum):
    """
    擬似データの難易度。
    """
    IDEAL = "IDEAL"
    BASIC_DISTURBANCE = "BASIC_DISTURBANCE"
    REALISTIC = "REALISTIC"
    HARSH = "HARSH"


def resolve_difficulty() -> Difficulty:
    """
    環境変数があれば難易度を優先し、なければ IDEAL を返す。
    """
    raw_value = os.environ.get("FACTORY_DIFFICULTY", Difficulty.IDEAL.value).strip().upper()
    try:
        return Difficulty(raw_value)
    except ValueError:
        print(f"[WARN] Unknown FACTORY_DIFFICULTY='{raw_value}'. Falling back to IDEAL.")
        return Difficulty.IDEAL


CURRENT_DIFFICULTY = resolve_difficulty()


@dataclass
class DifficultyProfile:
    """
    難易度に応じた乱れパラメータ群。
    """
    # 設備停止が起きる確率（工程ごと）
    stop_probability_process_1: float
    stop_probability_process_2: float

    # 停止時の追加時間（秒）
    stop_duration_min: float
    stop_duration_max: float

    # センサーイベントを落とす確率
    sensor_a_drop_probability: float
    sensor_b_drop_probability: float

    # センサー値のノイズ強度
    sensor_noise_multiplier: float

    # 送信遅延の最大秒数（イベント生成時刻は正しいが送信が遅れる）
    send_delay_max_seconds: float


def get_difficulty_profile(difficulty: Difficulty) -> DifficultyProfile:
    """
    難易度に対応するパラメータを返す。
    """
    if difficulty == Difficulty.IDEAL:
        return DifficultyProfile(
            stop_probability_process_1=0.0,
            stop_probability_process_2=0.0,
            stop_duration_min=0.0,
            stop_duration_max=0.0,
            sensor_a_drop_probability=0.0,
            sensor_b_drop_probability=0.0,
            sensor_noise_multiplier=1.0,
            send_delay_max_seconds=0.0,
        )

    if difficulty == Difficulty.BASIC_DISTURBANCE:
        return DifficultyProfile(
            stop_probability_process_1=0.10,
            stop_probability_process_2=0.12,
            stop_duration_min=1.0,
            stop_duration_max=3.0,
            sensor_a_drop_probability=0.03,
            sensor_b_drop_probability=0.04,
            sensor_noise_multiplier=1.2,
            send_delay_max_seconds=0.3,
        )

    if difficulty == Difficulty.REALISTIC:
        return DifficultyProfile(
            stop_probability_process_1=0.18,
            stop_probability_process_2=0.22,
            stop_duration_min=1.5,
            stop_duration_max=5.0,
            sensor_a_drop_probability=0.07,
            sensor_b_drop_probability=0.10,
            sensor_noise_multiplier=1.5,
            send_delay_max_seconds=0.8,
        )

    return DifficultyProfile(
        stop_probability_process_1=0.30,
        stop_probability_process_2=0.35,
        stop_duration_min=2.0,
        stop_duration_max=8.0,
        sensor_a_drop_probability=0.15,
        sensor_b_drop_probability=0.18,
        sensor_noise_multiplier=2.0,
        send_delay_max_seconds=1.5,
    )


PROFILE = get_difficulty_profile(CURRENT_DIFFICULTY)


# =========================================
# Kafka Producer
# =========================================

def create_producer() -> KafkaProducer:
    """
    Kafka Producer を生成する。
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
    )


# =========================================
# 時刻・送信ヘルパ
# =========================================

def now_iso() -> str:
    """
    現在時刻を ISO 文字列で返す。
    """
    return datetime.now().isoformat()


def maybe_send_with_delay(producer: KafkaProducer, topic: str, payload: dict[str, Any]) -> None:
    """
    難易度に応じて送信遅延を加えたうえで Kafka に送る。
    """
    if PROFILE.send_delay_max_seconds > 0:
        delay = random.uniform(0.0, PROFILE.send_delay_max_seconds)
        if delay > 0:
            time.sleep(delay)

    producer.send(topic, payload)
    print(f"[SEND] topic={topic} payload={payload}")


def sleep_with_stop(seconds: float) -> None:
    """
    Ctrl + C で止めやすい sleep。
    """
    end_time = time.time() + seconds
    while time.time() < end_time:
        if STOP_EVENT.is_set():
            break
        time.sleep(0.1)


# =========================================
# 真の内部状態
# =========================================

@dataclass
class ProcessWindow:
    """
    1製品・1工程の真の処理区間。
    """
    serial_no: str
    process_name: str
    start_time: datetime
    end_time: datetime
    planned_end_time: datetime
    stop_occurred: bool
    stop_duration_seconds: float
    status: str  # RUNNING / STOPPED / IDLE


class SharedState:
    """
    スレッド間で共有する真の内部状態。

    process-events スレッドが工程の進行を更新し、
    sensor-a / sensor-b スレッドはこの真の状態を参照して
    一貫したセンサーデータを生成する。
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.process_1_window: Optional[ProcessWindow] = None
        self.process_2_window: Optional[ProcessWindow] = None

    def set_window(self, process_name: str, window: ProcessWindow) -> None:
        with self.lock:
            if process_name == "process_1":
                self.process_1_window = window
            elif process_name == "process_2":
                self.process_2_window = window

    def clear_window(self, process_name: str) -> None:
        with self.lock:
            if process_name == "process_1":
                self.process_1_window = None
            elif process_name == "process_2":
                self.process_2_window = None

    def get_window(self, process_name: str) -> Optional[ProcessWindow]:
        with self.lock:
            if process_name == "process_1":
                return self.process_1_window
            if process_name == "process_2":
                return self.process_2_window
            return None

    def get_runtime_status(self, process_name: str) -> str:
        """
        現在の工程状態を返す。
        """
        window = self.get_window(process_name)
        if window is None:
            return "IDLE"

        now = datetime.now()
        if window.start_time <= now <= window.end_time:
            # 停止を含む工程でも、ここでは工程区間としてはアクティブ扱い。
            # センサー値の生成側で stop_occurred を参照して特性を変える。
            return "RUNNING"

        return "IDLE"


# =========================================
# 設備停止シミュレーション
# =========================================

def build_process_window(
    serial_no: str,
    process_name: str,
    base_duration_range: tuple[float, float],
) -> ProcessWindow:
    """
    難易度設定に基づいて、工程区間を生成する。

    ここが「真の基準ロジック」。
    process-events もセンサー値生成も、この区間情報を参照する。
    """
    start_time = datetime.now()
    planned_duration = random.uniform(*base_duration_range)
    planned_end_time = start_time + timedelta(seconds=planned_duration)

    if process_name == "process_1":
        stop_probability = PROFILE.stop_probability_process_1
    else:
        stop_probability = PROFILE.stop_probability_process_2

    stop_occurred = random.random() < stop_probability
    stop_duration_seconds = 0.0

    if stop_occurred:
        stop_duration_seconds = random.uniform(
            PROFILE.stop_duration_min,
            PROFILE.stop_duration_max,
        )

    actual_end_time = planned_end_time + timedelta(seconds=stop_duration_seconds)

    return ProcessWindow(
        serial_no=serial_no,
        process_name=process_name,
        start_time=start_time,
        end_time=actual_end_time,
        planned_end_time=planned_end_time,
        stop_occurred=stop_occurred,
        stop_duration_seconds=stop_duration_seconds,
        status="RUNNING",
    )


# =========================================
# process-events 送信
# =========================================

def process_events_thread(shared_state: SharedState) -> None:
    """
    serial_no を軸に、工程1→工程2の開始/終了イベントを送る。

    重要:
        このスレッドが全体の基準時系列を作る。
        センサーA/Bはこの真の工程区間を参照してイベントを送る。
    """
    producer = create_producer()
    serial_counter = 1

    try:
        while not STOP_EVENT.is_set():
            serial_no = f"SN{serial_counter:08d}"

            # -----------------------------
            # 工程1の真の区間生成
            # -----------------------------
            process_1_window = build_process_window(
                serial_no=serial_no,
                process_name="process_1",
                base_duration_range=(4.0, 7.0),
            )
            shared_state.set_window("process_1", process_1_window)

            maybe_send_with_delay(
                producer,
                TOPIC_PROCESS_EVENTS,
                {
                    "serial_no": serial_no,
                    "process": "process_1",
                    "event_type": "start",
                    "event_time_str": process_1_window.start_time.isoformat(),
                    "equipment_id": EQUIPMENT_ID,
                    "reason": None,
                },
            )

            # 工程1終了まで待つ
            sleep_with_stop((process_1_window.end_time - datetime.now()).total_seconds())

            maybe_send_with_delay(
                producer,
                TOPIC_PROCESS_EVENTS,
                {
                    "serial_no": serial_no,
                    "process": "process_1",
                    "event_type": "end",
                    "event_time_str": process_1_window.end_time.isoformat(),
                    "equipment_id": EQUIPMENT_ID,
                    "reason": "STOP_INCLUDED" if process_1_window.stop_occurred else None,
                },
            )

            shared_state.clear_window("process_1")

            # 工程1から工程2への搬送待ち
            transfer_wait = random.uniform(1.5, 3.5)
            sleep_with_stop(transfer_wait)

            # -----------------------------
            # 工程2の真の区間生成
            # -----------------------------
            process_2_window = build_process_window(
                serial_no=serial_no,
                process_name="process_2",
                base_duration_range=(5.0, 8.0),
            )
            shared_state.set_window("process_2", process_2_window)

            maybe_send_with_delay(
                producer,
                TOPIC_PROCESS_EVENTS,
                {
                    "serial_no": serial_no,
                    "process": "process_2",
                    "event_type": "start",
                    "event_time_str": process_2_window.start_time.isoformat(),
                    "equipment_id": EQUIPMENT_ID,
                    "reason": None,
                },
            )

            sleep_with_stop((process_2_window.end_time - datetime.now()).total_seconds())

            maybe_send_with_delay(
                producer,
                TOPIC_PROCESS_EVENTS,
                {
                    "serial_no": serial_no,
                    "process": "process_2",
                    "event_type": "end",
                    "event_time_str": process_2_window.end_time.isoformat(),
                    "equipment_id": EQUIPMENT_ID,
                    "reason": "STOP_INCLUDED" if process_2_window.stop_occurred else None,
                },
            )

            shared_state.clear_window("process_2")

            serial_counter += 1
            sleep_with_stop(random.uniform(0.5, 1.5))

    finally:
        producer.flush()
        producer.close()


# =========================================
# sensor-a 送信
# =========================================

def sensor_a_thread(shared_state: SharedState) -> None:
    """
    工程1側センサーAを送る。

    特徴:
        - 製品シリアルは持たない
        - 工程1の真の区間と設備状態を参照して値を作る
        - 難易度に応じて欠損を発生させる
    """
    producer = create_producer()

    try:
        while not STOP_EVENT.is_set():
            window = shared_state.get_window("process_1")

            # 欠損シミュレーション
            if random.random() < PROFILE.sensor_a_drop_probability:
                time.sleep(0.8)
                continue

            if window is None:
                # アイドル時
                temp = random.gauss(25.0, 0.8 * PROFILE.sensor_noise_multiplier)
                vibration = max(0.01, random.gauss(0.12, 0.04 * PROFILE.sensor_noise_multiplier))
                status = "IDLE"
            else:
                # 稼働時。停止込み工程なら値に少し異常傾向を入れる
                if window.stop_occurred:
                    temp = random.gauss(33.0, 1.8 * PROFILE.sensor_noise_multiplier)
                    vibration = max(0.01, random.gauss(0.55, 0.12 * PROFILE.sensor_noise_multiplier))
                    status = "RUNNING_WITH_DISTURBANCE"
                else:
                    temp = random.gauss(30.0, 1.1 * PROFILE.sensor_noise_multiplier)
                    vibration = max(0.01, random.gauss(0.35, 0.07 * PROFILE.sensor_noise_multiplier))
                    status = "RUNNING"

            payload = {
                "sensor_id": "A",
                "event_time_str": now_iso(),
                "value_temp": round(temp, 2),
                "value_vibration": round(vibration, 3),
                "unit": EQUIPMENT_ID,
                "status": status,
            }

            maybe_send_with_delay(producer, TOPIC_SENSOR_A, payload)
            time.sleep(0.8)

    finally:
        producer.flush()
        producer.close()


# =========================================
# sensor-b 送信
# =========================================

def sensor_b_thread(shared_state: SharedState) -> None:
    """
    工程2側センサーBを送る。

    特徴:
        - センサーAと異なる JSON フォーマット
        - 工程2の真の区間と設備状態を参照して値を作る
        - 難易度に応じて欠損を発生させる
    """
    producer = create_producer()

    try:
        while not STOP_EVENT.is_set():
            window = shared_state.get_window("process_2")

            # 欠損シミュレーション
            if random.random() < PROFILE.sensor_b_drop_probability:
                time.sleep(1.2)
                continue

            if window is None:
                pressure = random.gauss(94.0, 1.8 * PROFILE.sensor_noise_multiplier)
                current = max(0.1, random.gauss(6.5, 0.5 * PROFILE.sensor_noise_multiplier))
                status = "IDLE"
            else:
                if window.stop_occurred:
                    pressure = random.gauss(110.0, 4.2 * PROFILE.sensor_noise_multiplier)
                    current = max(0.1, random.gauss(10.0, 1.2 * PROFILE.sensor_noise_multiplier))
                    status = "RUNNING_WITH_DISTURBANCE"
                else:
                    pressure = random.gauss(102.0, 2.5 * PROFILE.sensor_noise_multiplier)
                    current = max(0.1, random.gauss(8.2, 0.7 * PROFILE.sensor_noise_multiplier))
                    status = "RUNNING"

            payload = {
                "sensor_code": "B-01",
                "ts_str": now_iso(),
                "pressure": round(pressure, 2),
                "current": round(current, 2),
                "line": EQUIPMENT_ID,
                "status": status,
            }

            maybe_send_with_delay(producer, TOPIC_SENSOR_B, payload)
            time.sleep(1.2)

    finally:
        producer.flush()
        producer.close()


# =========================================
# main
# =========================================

def main() -> None:
    """
    3スレッドを起動してデータ送信を継続する。
    """
    shared_state = SharedState()

    threads = [
        threading.Thread(
            target=sensor_a_thread,
            args=(shared_state,),
            daemon=True,
            name="sensor-a-thread",
        ),
        threading.Thread(
            target=sensor_b_thread,
            args=(shared_state,),
            daemon=True,
            name="sensor-b-thread",
        ),
        threading.Thread(
            target=process_events_thread,
            args=(shared_state,),
            daemon=True,
            name="process-events-thread",
        ),
    ]

    for thread in threads:
        thread.start()

    print("=== factory data producer started ===")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Difficulty: {CURRENT_DIFFICULTY.value}")
    print(f"Topics: {TOPIC_SENSOR_A}, {TOPIC_SENSOR_B}, {TOPIC_PROCESS_EVENTS}")
    print("Press Ctrl + C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping producer...")
        STOP_EVENT.set()

        for thread in threads:
            thread.join(timeout=3)

        print("Stopped.")


if __name__ == "__main__":
    main()
