# pyredis

使用 python 实现的 key-value 数据库

已经实现的功能

- str, list, set, dict 的存储和更改
- key 过期时间 (定时删除 `threading.Timer` 定时任务，惰性删除 `get` 时检查)
- 定时生成快照(`threading.Timer` 定时备份，使用 `os.fork` 复制一个子进程进行数据备份)

未实现的功能

- 客户端
- 事务
- 多线程
