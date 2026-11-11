"""Point every writable path at a throwaway directory before any src module loads.

Importing ``src.data.database`` (directly, or through ``src.web_app.app``) opens
``settings.DB_PATH`` at import time and runs migrations. Without this, collecting
the suite opened the real ``data/processed`` database, and test runs overlapping a
running job or web app corrupted it on 2026-11-10. Individual tests still patch
``settings.DB_PATH`` to their own ``tmp_path``; this only changes the default.
"""

import os
import tempfile
from pathlib import Path

_TEST_ROOT = Path(tempfile.mkdtemp(prefix="tradinggui-tests-"))
_ISOLATED_PATHS = {
    "DB_PATH": _TEST_ROOT / "stock_metrics_test.db",
    "PRICE_CACHE_DIR": _TEST_ROOT / "cache" / "ohlc",
    "REPORTS_DIR": _TEST_ROOT / "reports",
    "JOB_LOCK_PATH": _TEST_ROOT / ".job.lock",
    "LOG_FILE": _TEST_ROOT / "logs" / "trading.log",
}
for name, path in _ISOLATED_PATHS.items():
    os.environ[name] = str(path)

from src.config.settings import settings  # noqa: E402  (must follow the env setup)

_REPO_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
if settings.DB_PATH.resolve().is_relative_to(_REPO_DATA_DIR.resolve()):
    raise RuntimeError(
        f"Refusing to run tests against the real database at {settings.DB_PATH}"
    )
