from celery import Celery

celery_app = Celery(
    "g2p_bridge_celery_tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)
