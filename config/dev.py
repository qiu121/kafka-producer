"""
开发环境配置
"""

# 先把基准配置全部导入进来
from .base import *

# 开发时使用本机的 Kafka
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

# 让主题名带上前缀，便于在同一集群中区分
CURRENT_TIME_TOPIC = "dev_current_time"
QC_LOC_TOPIC = "dev_qc_loc"

# 频率可以保持基准值
CURRENT_TIME_INTERVAL = 1
QC_LOC_INTERVAL = 2