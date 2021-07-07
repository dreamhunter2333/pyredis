import json
import logging

from typing import Any

_logger = logging.getLogger(__name__)


def snapshot(snapshot_path: str, data: Any):
    with open(snapshot_path, "w") as f:
        f.write(json.dumps(data))
    _logger.info("snapshot to %s", snapshot_path)
