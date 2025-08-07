"""
Kafka Producer 示例

本文件在原来的静态 `qc_loc` 模板基础上加入了 **动态模拟 C01 小车/吊具运动** 的逻辑，
实现 “门字形” 装卸船作业循环：

    1. 小车前进 (0 ~ 3s)      —— 水平位移从岸侧 → 海侧，速度为正
    2. 吊具提升 (30 ~ 60s)    —— 吊具高度从 0 → Hmax，状态为 "load"
    3. 小车后退 (60 ~ 90s)    —— 水平位移从海侧 → 岸侧，速度为负
    4. 吊具下降 (90 ~ 120s)   —— 吊具高度从 Hmax → 0，状态为 "unload"

每一次完整循环耗时 2min（120s），在循环结束后会随机生成新的
`car_length`（水平范围）与 `lift_high`（垂直范围）参数，以避免总是
出现相同的数值并更贴近真实作业。其他 10 条车位保持原始静态数据。

发送频率仍保持原先的 `QC_LOC_INTERVAL`（默认 2s），
`current_time` 字段会使用 `utils.time_utils.get_current_time()` 进行实时填充。
"""

import json
import threading
import time
import random  # 新增：用于生成每个循环的随机目标范围
from kafka import KafkaProducer
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    CURRENT_TIME_TOPIC,
    QC_LOC_INTERVAL,
    CURRENT_TIME_INTERVAL,
    QC_LOC_TOPIC, _ENV
)
from utils.time_utils import get_current_time


class KafkaProducerApp:
    """
    Kafka 生产者应用。

    - current_time 主题：每 1 秒发送一次当前时间。
    - qc_loc 主题：每 QC_LOC_INTERVAL 秒发送一次位置信息。
                 其中 id 为 C01 的记录会依据“门字形”轨迹动态变化。
    """

    def __init__(self):
        """
        初始化 Kafka 生产者、静态模板以及模拟参数。
        """
        # ---------- Kafka Producer ----------
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # ---------- 静态 qc_loc 消息模板 ----------
        # 只对 id 为 C01 的那条记录后续会被动态修改。
        self.qc_loc_template = {
            "current_time": "",
            "data": [
                {
                    "id": "C01",
                    "x": 1070.152,
                    "y": -1402.246,
                    "angle": 177.0,
                    "speed": 0.0,
                    "pitch": 0.0,
                    "car_length": 0.0,
                    "car_speed": 0.0,
                    "small_car_length": 0.0,
                    "small_car_speed": 0.0,
                    "lift_size": 40,
                    "lift_high": 0.0,
                    "lift_speed": 0.0,
                    "status": "stop",
                    "error_message": [],
                    "link": {
                        "yard": "",
                        "ship": "",
                        "noman_vehicle": "",
                        "builtin_vehicle": ""
                    },
                    "indicator_data": {
                        "Total_power_consumption": "0"
                    }
                },
                # -------------------------------------------------
                # 以下 9 条车辆保持原始的静态值（不参与模拟）
                # -------------------------------------------------
                {
                    "id": "C02",
                    "x": 1223.531,
                    "y": -1394.22,
                    "angle": 177.0,
                    "speed": 0.0,
                    "pitch": 0.0,
                    "car_length": 0.0,
                    "car_speed": 0.0,
                    "small_car_length": 0.0,
                    "small_car_speed": 0.0,
                    "lift_size": 40,
                    "lift_high": 0.0,
                    "lift_speed": 0.0,
                    "status": "stop",
                    "error_message": [],
                    "link": {
                        "yard": "",
                        "ship": "",
                        "noman_vehicle": "",
                        "builtin_vehicle": ""
                    },
                    "indicator_data": {
                        "Total_power_consumption": "0"
                    }
                },
                {
                    "id": "C03",
                    "x": 1263.401,
                    "y": -1392.972,
                    "angle": 177.0,
                    "speed": 0.0,
                    "pitch": 0.0,
                    "car_length": 0.0,
                    "car_speed": 0.0,
                    "small_car_length": 0.0,
                    "small_car_speed": 0.0,
                    "lift_size": 40,
                    "lift_high": 0.0,
                    "lift_speed": 0.0,
                    "status": "stop",
                    "error_message": [],
                    "link": {
                        "yard": "",
                        "ship": "",
                        "noman_vehicle": "",
                        "builtin_vehicle": ""
                    },
                    "indicator_data": {
                        "Total_power_consumption": "0"
                    }
                },
                {
                    "id": "C04",
                    "x": 1313.395,
                    "y": -1393.63,
                    "angle": 177.0,
                    "speed": 0.0,
                    "pitch": 0.0,
                    "car_length": 0.0,
                    "car_speed": 0.0,
                    "small_car_length": 0.0,
                    "small_car_speed": 0.0,
                    "lift_size": 40,
                    "lift_high": 0.0,
                    "lift_speed": 0.0,
                    "status": "stop",
                    "error_message": [],
                    "link": {
                        "yard": "",
                        "ship": "",
                        "noman_vehicle": "",
                        "builtin_vehicle": ""
                    },
                    "indicator_data": {
                        "Total_power_consumption": "0"
                    }
                },
                {
                    "id": "C05",
                    "x": 1441.98,
                    "y": -1386.107,
                    "angle": 177.0,
                    "speed": 0.0,
                    "pitch": 0.0,
                    "car_length": 0.0,
                    "car_speed": 0.0,
                    "small_car_length": 0.0,
                    "small_car_speed": 0.0,
                    "lift_size": 40,
                    "lift_high": 0.0,
                    "lift_speed": 0.0,
                    "status": "stop",
                    "error_message": [],
                    "link": {
                        "yard": "",
                        "ship": "",
                        "noman_vehicle": "",
                        "builtin_vehicle": ""
                    },
                    "indicator_data": {
                        "Total_power_consumption": "0"
                    }
                },
                {
                    "id": "C06",
                    "x": 1481.851,
                    "y": -1384.883,
                    "angle": 177.0,
                    "speed": 0.0,
                    "pitch": 0.0,
                    "car_length": 0.0,
                    "car_speed": 0.0,
                    "small_car_length": 0.0,
                    "small_car_speed": 0.0,
                    "lift_size": 40,
                    "lift_high": 0.0,
                    "lift_speed": 0.0,
                    "status": "stop",
                    "error_message": [],
                    "link": {
                        "yard": "",
                        "ship": "",
                        "noman_vehicle": "",
                        "builtin_vehicle": ""
                    },
                    "indicator_data": {
                        "Total_power_consumption": "0"
                    }
                },
                {
                    "id": "C07",
                    "x": 1527.76,
                    "y": -1383.324,
                    "angle": 177.0,
                    "speed": 0.0,
                    "pitch": 0.0,
                    "car_length": 0.0,
                    "car_speed": 0.0,
                    "small_car_length": 0.0,
                    "small_car_speed": 0.0,
                    "lift_size": 40,
                    "lift_high": 0.0,
                    "lift_speed": 0.0,
                    "status": "stop",
                    "error_message": [],
                    "link": {
                        "yard": "",
                        "ship": "",
                        "noman_vehicle": "",
                        "builtin_vehicle": ""
                    },
                    "indicator_data": {
                        "Total_power_consumption": "0"
                    }
                },
                {
                    "id": "C08",
                    "x": 1579.056,
                    "y": -1382.301,
                    "angle": 177.0,
                    "speed": 0.0,
                    "pitch": 0.0,
                    "car_length": 0.0,
                    "car_speed": 0.0,
                    "small_car_length": 0.0,
                    "small_car_speed": 0.0,
                    "lift_size": 40,
                    "lift_high": 0.0,
                    "lift_speed": 0.0,
                    "status": "stop",
                    "error_message": [],
                    "link": {
                        "yard": "",
                        "ship": "",
                        "noman_vehicle": "",
                        "builtin_vehicle": ""
                    },
                    "indicator_data": {
                        "Total_power_consumption": "0"
                    }
                },
                {
                    "id": "C09",
                    "x": 1752.217,
                    "y": -1372.727,
                    "angle": 177.0,
                    "speed": 0.0,
                    "pitch": 0.0,
                    "car_length": 0.0,
                    "car_speed": 0.0,
                    "small_car_length": 0.0,
                    "small_car_speed": 0.0,
                    "lift_size": 40,
                    "lift_high": 0.0,
                    "lift_speed": 0.0,
                    "status": "stop",
                    "error_message": [],
                    "link": {
                        "yard": "",
                        "ship": "",
                        "noman_vehicle": "",
                        "builtin_vehicle": ""
                    },
                    "indicator_data": {
                        "Total_power_consumption": "0"
                    }
                },
                {
                    "id": "C10",
                    "x": 1799.113,
                    "y": -1371.426,
                    "angle": 177.0,
                    "speed": 0.0,
                    "pitch": 0.0,
                    "car_length": 0.0,
                    "car_speed": 0.0,
                    "small_car_length": 0.0,
                    "small_car_speed": 0.0,
                    "lift_size": 40,
                    "lift_high": 0.0,
                    "lift_speed": 0.0,
                    "status": "stop",
                    "error_message": [],
                    "link": {
                        "yard": "",
                        "ship": "",
                        "noman_vehicle": "",
                        "builtin_vehicle": ""
                    },
                    "indicator_data": {
                        "Total_power_consumption": "0"
                    }
                }
            ]
        }

        # ---------- 控制标识 ----------
        self.running = False

        # ---------- 装卸模拟参数 ----------
        # 一个完整的装卸循环（两侧装、两侧卸）共 120 秒 = 2 分钟
        self.cycle_duration = 120.0          # 秒
        self.forward_duration = 30.0         # 小车前进（0 → 30s）
        self.lift_up_duration = 30.0         # 吊具提升（30 → 60s）
        self.backward_duration = 30.0        # 小车后退（60 → 90s）
        self.lift_down_duration = 30.0       # 吊具下降（90 → 120s）


        # 记录本轮循环的起始时间（秒时间戳）
        self.cycle_start_time = time.time()
        # 为本轮循环随机生成目标范围（避免每次都恰好 0/100、-20/38）
        self._generate_cycle_targets()

    # ----------------------------------------------------------------------
    # 生成本轮循环的随机目标值
    # ----------------------------------------------------------------------
    def _generate_cycle_targets(self):
        """
        随机生成本轮循环的水平位移范围（car_length）和最高吊具高度（lift_high）。
        取值范围均避开极端边界，以更贴近真实作业：
            car_length  : 5 ~ 25（岸侧）  <->  75 ~ 95（海侧）
            lift_high   : 10 ~ 35（米或对应单位）
        """
        self.min_car_length = round(random.uniform(5.0, 25.0), 5)   # 岸侧起始位置
        self.max_car_length = round(random.uniform(75.0, 95.0), 5)  # 海侧目标位置
        # 确保 min < max（防止极端随机导致相反）
        if self.min_car_length >= self.max_car_length:
            self.min_car_length, self.max_car_length = self.max_car_length, self.min_car_length

        # 吊具最高高度（正数, 0~38 中的安全区间）
        self.max_lift_high = round(random.uniform(10.0, 35.0), 5)

        # 为调试/观察提供日志
        print(f"[Cycle Init] min_car_length={self.min_car_length}, "
              f"max_car_length={self.max_car_length}, max_lift_high={self.max_lift_high}")

    # ----------------------------------------------------------------------
    # 发送当前时间（不变动）
    # ----------------------------------------------------------------------
    def send_current_time(self):
        """
        每 CURRENT_TIME_INTERVAL 秒向 CURRENT_TIME_TOPIC 发送一次当前时间。
        """
        while self.running:
            try:
                message = {"current_time": get_current_time()}
                future = self.producer.send(CURRENT_TIME_TOPIC, value=message)
                result = future.get(timeout=10)
                print(f"Sent current time message to {CURRENT_TIME_TOPIC}: {message}")
                print(f"Partition: {result.partition}, Offset: {result.offset}")
            except Exception as e:
                print(f"Error sending current time message: {e}")

            time.sleep(CURRENT_TIME_INTERVAL)

    # ----------------------------------------------------------------------
    # 发送 qc_loc（核心模拟逻辑）
    # ----------------------------------------------------------------------
    def send_qc_loc(self):
        """
        每 QC_LOC_INTERVAL 秒发送一次 qc_loc 数据。
        仅对 id 为 C01 的小车/吊具做动态模拟，实现 “门字形” 装卸轨迹。
        """
        while self.running:
            try:
                # ---------- 循环切换 ----------
                now = time.time()
                if now - self.cycle_start_time >= self.cycle_duration:
                    # 本轮循环结束，重新计时并随机化目标参数
                    self.cycle_start_time = now
                    self._generate_cycle_targets()

                # 已经进入本轮循环的相对时间（seconds）
                elapsed = now - self.cycle_start_time

                # ---------- 根据所在相位计算运动值 ----------
                if elapsed < self.forward_duration:
                    # 1️⃣ 小车向海侧前进
                    progress = elapsed / self.forward_duration
                    car_length = self.min_car_length + (self.max_car_length - self.min_car_length) * progress
                    car_speed = (self.max_car_length - self.min_car_length) / self.forward_duration   # 正值
                    lift_high = 0.0
                    lift_speed = 0.0
                    # status = "stop"
                    status = "load"
                elif elapsed < self.forward_duration + self.lift_up_duration:
                    # 2️⃣ 吊具提升（装货）
                    prog = (elapsed - self.forward_duration) / self.lift_up_duration
                    car_length = self.max_car_length
                    car_speed = 0.0
                    lift_high = self.max_lift_high * prog
                    lift_speed = self.max_lift_high / self.lift_up_duration
                    status = "load"
                elif elapsed < self.forward_duration + self.lift_up_duration + self.backward_duration:
                    # 3️⃣ 小车返回岸侧
                    prog = (elapsed - self.forward_duration - self.lift_up_duration) / self.backward_duration
                    car_length = self.max_car_length - (self.max_car_length - self.min_car_length) * prog
                    car_speed = -(self.max_car_length - self.min_car_length) / self.backward_duration   # 负值
                    lift_high = self.max_lift_high
                    lift_speed = 0.0
                    # status = "stop"
                    status = "unload"
                else:
                    # 4️⃣ 吊具下降（卸货）
                    prog = (elapsed - self.forward_duration - self.lift_up_duration - self.backward_duration) / self.lift_down_duration
                    car_length = self.min_car_length
                    car_speed = 0.0
                    lift_high = self.max_lift_high * (1.0 - prog)
                    lift_speed = -self.max_lift_high / self.lift_down_duration
                    status = "unload"

                # 保留 5 位小数，和模板中保持一致的精度
                car_length = round(car_length, 5)
                car_speed = round(car_speed, 5)
                lift_high = round(lift_high, 5)
                lift_speed = round(lift_speed, 5)

                # ---------- 更新 C01 条目 ----------
                for entry in self.qc_loc_template["data"]:
                    if entry["id"] == "C01":
                        entry["car_length"] = car_length
                        entry["car_speed"] = car_speed
                        entry["lift_high"] = lift_high
                        entry["lift_speed"] = lift_speed
                        entry["status"] = status
                        # 如有需要，可同步更新关联的视觉字段（如 angle、speed），这里保持不变
                        break

                # ---------- 填充消息时间 ----------
                self.qc_loc_template["current_time"] = get_current_time()

                # ---------- 发送到 Kafka ----------
                future = self.producer.send(QC_LOC_TOPIC, value=self.qc_loc_template)
                result = future.get(timeout=10)

                print(f"[qc_loc] Sent to {QC_LOC_TOPIC} | C01 status={status} | "
                      f"car_len={car_length} m | lift_h={lift_high} m")
                print(f"Partition: {result.partition}, Offset: {result.offset}")

            except Exception as e:
                print(f"Error sending qc_loc message: {e}")

            time.sleep(QC_LOC_INTERVAL)

    # ----------------------------------------------------------------------
    # 启动/停止生产者
    # ----------------------------------------------------------------------
    def start(self):
        """
        启动生产者，开启两个后台线程：
        - 发送当前时间
        - 发送 qc_loc（含动态模拟）
        """
        self.running = True

        # 线程 1：发送当前时间
        current_time_thread = threading.Thread(target=self.send_current_time)
        current_time_thread.daemon = True
        current_time_thread.start()

        # 线程 2：发送 qc_loc（带动态模拟）
        qc_loc_thread = threading.Thread(target=self.send_qc_loc)
        qc_loc_thread.daemon = True
        qc_loc_thread.start()

        print("Kafka producer started. Press Ctrl+C to stop.")
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping Kafka producer...")
            self.stop()

    def stop(self):
        """
        停止生产者，关闭线程循环并安全关闭 Kafka 连接。
        """
        self.running = False
        if hasattr(self, "producer"):
            self.producer.close()
        print("Kafka producer stopped.")


# ----------------------------------------------------------------------
# 程序入口
# ----------------------------------------------------------------------
if __name__ == "__main__":
    print("APP_ENV:" + _ENV)
    app = KafkaProducerApp()
    app.start()