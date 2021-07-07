import os
import json
import time
import logging
import threading

from typing import Any
from collections import defaultdict

from .snapshot import snapshot

# 支持的数据类型
DATA_TYPES = {"str": str, "list": list, "set": set, "dict": dict}
_logger = logging.getLogger(__name__)


class PyRedis(object):

    def __init__(self, snapshot_path: str, ttl_time: int, snapshot_time: int):
        """
        初始化数据
        params:
            snapshot_path: 快照路径
            ttl_time: 定时清零过期 key 的间隔 s
            snapshot_time: 定时创建快照的间隔 s
        """
        # 数据
        self.data = defaultdict(lambda: {
            "vtype": None,
            "data": None,
            "expire": None
        })
        # 锁
        self.lock = threading.Lock()
        self.snapshot_path = snapshot_path
        # 从快照中恢复数据
        self.restore()
        self.ttl_time = ttl_time
        self.snapshot_time = snapshot_time
        # 两个定时任务
        self.snapshot()
        self.check_expire()

    def restore(self):
        """
        从快照中恢复数据
        """
        if not self.snapshot_path or not os.path.isfile(self.snapshot_path):
            return
        self.lock.acquire()
        try:
            with open(self.snapshot_path, "r") as f:
                self.data.update(json.loads(f.read()))
            _logger.info("restore snapshot from %s", self.snapshot_path)
        except Exception as e:
            _logger.exception(e)
        finally:
            self.lock.release()

    def snapshot(self):
        """
        使用 os.fork() 在子进程进行快照
        """
        pid = os.fork()
        if pid == 0:
            _logger.info("snapshot by os fork start")
            snapshot(self.snapshot_path, self.data)
            _logger.info("snapshot by os fork end")
            os._exit(0)
        threading.Timer(self.snapshot_time, self.snapshot).start()

    def check_expire(self):
        """
        定时清理过期 key
        """
        self.lock.acquire()
        for key in list(self.data.keys()):
            self.check_key_expire(key)
        self.lock.release()
        threading.Timer(self.ttl_time, self.check_expire).start()

    def check_key_expire(self, key: str):
        """
        检查并清理过期 key
        """
        if key not in self.data or not self.data[key]["expire"]:
            return
        if self.data[key]["expire"] < time.time():
            _logger.info("del key %s", key)
            del self.data[key]

    def exec(self, command):
        """
        执行命令
        """
        res = {
            "code": 200,
            "data": None,
            "error": None
        }
        self.lock.acquire()
        try:
            func = command['func']
            args = command['args']
            res["data"] = getattr(self, func)(*args)
        except Exception as e:
            res["code"] = 400
            res["error"] = str(e)
        finally:
            self.lock.release()
        return json.dumps(res).encode('utf-8')

    def set(self, key: str, value: Any, vtype: Any) -> bool:
        """
        设置 key 的值
        """
        self.check_value(value, vtype)
        self.data[key]["vtype"] = vtype
        self.data[key]["data"] = value
        return True

    def get(self, key: str) -> Any:
        """
        获取 key 的值
        """
        self.check_key_expire(key)
        if key not in self.data:
            return
        return self.data[key]["data"]

    def delete(self, key: str) -> bool:
        """
        删除 key
        """
        del self.data[key]
        return True

    def expire_at(self, key: str, expire_time: float) -> bool:
        """
        设置过期时间
        """
        if key not in self.data:
            raise ValueError("No key %s", key)
        self.data[key]["expire"] = expire_time
        return True

    def check_value(self, value: Any, vtype: str) -> None:
        """
        检查值的类型，包括下一层数据的类型
        """
        if vtype not in DATA_TYPES or not isinstance(value, DATA_TYPES[vtype]):
            raise ValueError("None support for type %s", vtype)
        if type(value) not in DATA_TYPES.values():
            raise ValueError("Wrong value type %s for %s" % (type(value), vtype))
        if not isinstance(value, str) and any(type(v) not in DATA_TYPES.values() for v in value):
            raise ValueError("Wrong value item type %s for %s" % (type(value), vtype))

    def check_value_type(self, value_item: Any, func_name: str) -> None:
        """
        检查数据的类型
        """
        if type(value_item) not in DATA_TYPES.values():
            raise ValueError("%s doesn't support add item of type %s" % (func_name, type(value_item)))

    def check_key_value(self, key: str, vtype: str, func_name: str) -> None:
        """
        检查 key 和对应的类型
        """
        self.check_key_expire(key)
        if not isinstance(key, str) or key not in self.data:
            raise ValueError("None value for key %s" % key)
        if self.data[key]["vtype"] != vtype:
            raise ValueError("%s doesn't support for type %s" % (func_name, self.data[key]["vtype"]))

    def list_append(self, key: str, value_item: Any) -> bool:
        self.check_value_type(value_item, "List append")
        self.check_key_value(key, "list", "List append")
        self.data[key]["data"].append(value_item)
        return True

    def list_remove(self, key: str, value_item: Any) -> bool:
        self.check_key_value(key, "list", "List remove")
        self.data[key]["data"].remove(value_item)
        return True

    def list_pop(self, key: str, index: int) -> Any:
        self.check_key_value(key, "list", "List pop")
        return self.data[key]["data"].pop(index)

    def set_add(self, key: str, value_item: Any) -> bool:
        self.check_value_type(value_item, "Set add")
        self.check_key_value(key, "set", "Set add")
        self.data[key]["data"].add(value_item)
        return True

    def set_remove(self, key: str, value_item: Any) -> bool:
        self.check_key_value(key, "set", "Set remove")
        self.data[key]["data"].remove(value_item)
        return True

    def dict_set(self, key: str, dict_key: str, dict_item: Any) -> bool:
        self.check_value_type(dict_item, "Dict add")
        self.check_key_value(key, "dict", "Dict add")
        self.data[key]["data"][dict_key] = dict_item
        return True

    def dict_get(self, key: str, dict_key: str) -> Any:
        self.check_key_value(key, "dict", "Dict get")
        return self.data[key]["data"].get(dict_key)

    def dict_del(self, key: str, dict_key: str) -> bool:
        self.check_key_value(key, "dict", "Dict get")
        del self.data[key]["data"][dict_key]
        return True
