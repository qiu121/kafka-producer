import json
import threading
import time

from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    CURRENT_TIME_TOPIC,
    QC_LOC_INTERVAL,
    CURRENT_TIME_INTERVAL,
    QC_LOC_TOPIC
)
from utils.time_utils import get_current_time


class KafkaProducerApp:
    def __init__(self):
        """
        初始化Kafka生产者
        """
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # qc_loc消息模板
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

        # 控制线程运行的标志
        self.running = False

    def send_current_time(self):
        """
        发送当前时间到current_time topic
        每1秒发送一次
        """
        while self.running:
            try:
                # 创建消息
                message = {
                    "current_time": get_current_time()
                }

                # 发送消息
                future = self.producer.send(CURRENT_TIME_TOPIC, value=message)

                # 等待消息发送确认
                result = future.get(timeout=10)

                print(f"Sent current time message to {CURRENT_TIME_TOPIC}: {message}")
                print(f"Partition: {result.partition}, Offset: {result.offset}")

            except Exception as e:
                print(f"Error sending current time message: {e}")

            # 等待1秒
            time.sleep(CURRENT_TIME_INTERVAL)

    def send_qc_loc(self):
        """
        发送位置信息到qc_loc topic
        每2秒发送一次，只更新current_time字段
        """
        while self.running:
            try:
                # 更新消息中的当前时间
                self.qc_loc_template["current_time"] = get_current_time()

                # 发送消息
                future = self.producer.send(QC_LOC_TOPIC, value=self.qc_loc_template)

                # 等待消息发送确认
                result = future.get(timeout=10)

                print(f"Sent qc_loc message to {QC_LOC_TOPIC}")
                print(f"Partition: {result.partition}, Offset: {result.offset}")

            except Exception as e:
                print(f"Error sending qc_loc message: {e}")

            # 等待2秒
            time.sleep(QC_LOC_INTERVAL)

    def start(self):
        """
        启动生产者，创建并启动发送消息的线程
        """
        self.running = True

        # 创建并启动发送当前时间的线程
        current_time_thread = threading.Thread(target=self.send_current_time)
        current_time_thread.daemon = True
        current_time_thread.start()

        # 创建并启动发送位置信息的线程
        qc_loc_thread = threading.Thread(target=self.send_qc_loc)
        qc_loc_thread.daemon = True
        qc_loc_thread.start()

        print("Kafka producer started. Press Ctrl+C to stop.")

        try:
            # 保持主线程运行
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping Kafka producer...")
            self.stop()

    def stop(self):
        """
        停止生产者，关闭所有线程和连接
        """
        self.running = False

        # 关闭生产者连接
        if hasattr(self, 'producer'):
            self.producer.close()

        print("Kafka producer stopped.")


if __name__ == "__main__":
    # 创建并启动Kafka生产者应用
    app = KafkaProducerApp()
    app.start()