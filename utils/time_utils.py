from datetime import datetime
import pytz

def get_current_time():
    """
    获取当前时间，格式化为ISO 8601格式，包含时区信息
    返回格式: "2025-07-31T11:02:22.000+08:00"
    """
    # 设置时区为东八区
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    # 格式化为ISO 8601格式
    return now.isoformat(timespec='milliseconds')