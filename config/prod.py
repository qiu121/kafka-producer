"""
生产环境配置
"""

# 生产集群的 broker 列表
KAFKA_BOOTSTRAP_SERVERS = [
    "kafka-prod-01:9092",
    "kafka-prod-02:9092",
    "kafka-prod-03:9092",
]

# 生产主题（不要加前缀）
CURRENT_TIME_TOPIC = "current_time"
QC_LOC_TOPIC = "qc_loc"

# 发送频率保持基准值即可
CURRENT_TIME_INTERVAL = 1
QC_LOC_INTERVAL = 2