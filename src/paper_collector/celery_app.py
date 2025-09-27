from celery import Celery
from celery.schedules import crontab

# Create a Celery instance
app = Celery(
    'paper_collector',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
    # Ensure both tasks and pipelines are included
    include=['paper_collector.tasks', 'paper_collector.pipelines']
)

# Optional configuration
app.conf.update(
    result_expires=3600,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Seoul',
    enable_utc=True,
)

# Celery Beat Schedule
app.conf.beat_schedule = {
    'start-hourly-harvest': {
        'task': 'pipelines.start_harvest',
        # As per design: "OpenAlex 신규/업뎃 스캔: 매시 15분"
        'schedule': crontab(minute=15, hour='*'),
        'options': {'queue': 'harvest'},
    },
    'run-nightly-backfill-2023': {
        'task': 'pipelines.backfill_batch',
        # As per design: "백필(Backfill) 오래된 연도대: 새벽 2~5시 저부하 시간대"
        # Example: Run for year 2023 at 3 AM daily.
        'schedule': crontab(minute=0, hour=3),
        'args': (2023,), # Argument for the backfill_batch task
        'options': {'queue': 'backfill'},
    }
}

if __name__ == '__main__':
    app.start()