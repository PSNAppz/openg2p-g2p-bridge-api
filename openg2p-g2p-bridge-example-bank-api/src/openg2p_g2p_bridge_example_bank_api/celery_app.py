from celery import Celery

celery_app = Celery(
    "example_bank_celery_tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)

celery_app.conf.beat_schedule = {
    "initiate_fund_check_beat_producer": {
        "task": "initiate_fund_check_beat_producer",
        "schedule": 10,
    }
}

celery_app.conf.timezone = "UTC"


@celery_app.task
def initiate_fund_check_beat_producer():
    print("Initiating fund check beat producer")
    return True
