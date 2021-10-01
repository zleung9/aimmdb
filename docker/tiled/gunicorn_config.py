bind = "0.0.0.0:8000"
workers = 1
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
timeout = 60
keepalive = 2
wsgi_app = "tiled.server.app:app_factory()"
worker_tmp_dir = "/dev/shm"
errorlog = "-"
accesslog = "-"
loglevel = "warning"
# add client's ip address
access_log_format =  %({X_Forwarded_For}i)s %(l)s %(u)s %(t)s %(r)s %(s)s %(b)s %(f)s %(a)s