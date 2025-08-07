"""
公共（基准）配置
所有环境都会先加载这里的值，然后在各自的文件中覆盖需要改动的字段。
"""

# ---- Kafka ----
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

# ---- Topics ----
CURRENT_TIME_TOPIC = "current_time"
QC_LOC_TOPIC = "qc_loc"

# ---- 发送间隔（秒）----
CURRENT_TIME_INTERVAL = 1      # 每 1s 发送一次 current_time
QC_LOC_INTERVAL = 2           # 每 2s 发送一次 qc_loc