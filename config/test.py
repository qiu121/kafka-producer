"""
单元/集成测试环境配置
"""

from .base import *

# 假设 CI 环境里有一个专用的 broker
KAFKA_BOOTSTRAP_SERVERS = ["47.100.225.208:9092"]

# 主题使用 test 前缀，避免写入真实主题
CURRENT_TIME_TOPIC = "current_time"
QC_LOC_TOPIC = "qc_loc"

# 为了加快 CI 运行，间隔设得更短
CURRENT_TIME_INTERVAL = 0.5
QC_LOC_INTERVAL = 1