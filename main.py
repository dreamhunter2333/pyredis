import json
import logging

from wsgiref.simple_server import make_server

from pyredis.config import settings
from pyredis.pyredis import PyRedis

logging.basicConfig(
    format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    level=logging.INFO
)
_logger = logging.getLogger("pyredis main")


py_redis = PyRedis(
    settings.snapshot_path,
    settings.ttl_time,
    settings.snapshot_time
)


def application(environ, start_response):
    headers = [('Content-Type', 'application/json')]
    start_response('200 OK', headers)
    request_body = environ["wsgi.input"].read(
        int(environ.get("CONTENT_LENGTH", 0)))
    return [py_redis.exec(json.loads(request_body.decode()))]


_logger.info("Pyredis server start")
make_server('', settings.port, application).serve_forever()
