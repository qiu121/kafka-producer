"""
根据环境变量 APP_ENV 动态加载对应的配置模块，并把其中的
常量重新导出到根命名空间。

使用方式（保持不变）：

    from config import KAFKA_BOOTSTRAP_SERVERS, CURRENT_TIME_TOPIC, ...

"""

import os
import importlib
from types import ModuleType
from typing import List

# ------------------------------------------------------------
# 1️⃣ 读取环境变量，默认 development
# ------------------------------------------------------------
_ENV = os.getenv("APP_ENV", "dev").strip().lower()

# ------------------------------------------------------------
# 2️⃣ 合法的环境集合（与上面文件名保持一致）
# ------------------------------------------------------------
_VALID_ENVS = {"dev", "test", "prod"}

if _ENV not in _VALID_ENVS:
    raise ImportError(
        f"Unsupported APP_ENV='{_ENV}'. "
        f"Supported values are: {sorted(_VALID_ENVS)}"
    )

# ------------------------------------------------------------
# 3️⃣ 动态导入对应的子模块（如 config.development）
# ------------------------------------------------------------
_module_name = f".{_ENV}"
try:
    _env_module: ModuleType = importlib.import_module(_module_name, __package__)
except Exception as exc:  # pragma: no cover
    raise ImportError(
        f"Failed to import config module for APP_ENV='{_ENV}'"
    ) from exc

# ------------------------------------------------------------
# 4️⃣ 把子模块里所有 **大写** 常量（不以下划线开头）复制到当前包的全局命名空间
#    这样 `from config import X` 实际是从这里取值。
# ------------------------------------------------------------
for _name in dir(_env_module):
    # 只导出真正的常量（全部大写、且不以下划线开头）
    if _name.isupper() and not _name.startswith("_"):
        globals()[_name] = getattr(_env_module, _name)

# ------------------------------------------------------------
# 5️⃣ 为 IDE / 静态检查提供 __all__（可选，却能让 PyCharm、VS Code 等不报 “Cannot find reference”）
# ------------------------------------------------------------
__all__: List[str] = [name for name in globals() if name.isupper()]

# ------------------------------------------------------------
# 6️⃣ 为 type‑checkers（mypy、pyright）提供“伪声明”
#    下面的 # type: ignore 只在运行时不产生影响，静态检查会看到真正的类型。
# ------------------------------------------------------------
# 下面的行是帮助编辑器识别常量的“占位”声明，实际值已经在第 4 步注入。
KAFKA_BOOTSTRAP_SERVERS: List[str]  # type: ignore[assignment]
CURRENT_TIME_TOPIC: str              # type: ignore[assignment]
QC_LOC_TOPIC: str                    # type: ignore[assignment]
CURRENT_TIME_INTERVAL: float         # type: ignore[assignment]
QC_LOC_INTERVAL: float               # type: ignore[assignment]