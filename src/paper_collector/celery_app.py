import os
import logging
from logging.handlers import RotatingFileHandler
from celery import Celery
from celery.schedules import crontab
from celery.signals import after_setup_logger

# --- Create logs directory if it doesn't exist ---
LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# --- Logging Configuration ---
@after_setup_logger.connect
def setup_loggers(logger, **kwargs):
    """
    Redirect Celery loggers to files.
    This function is connected to the `after_setup_logger` signal.
    """
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Configure file handler for the main Celery logger (workers)
    fh_celery = RotatingFileHandler(
        os.path.join(LOG_DIR, 'celery.log'),
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5
    )
    fh_celery.setFormatter(formatter)
    logger.addHandler(fh_celery)

    # Configure file handler for the Celery Beat logger
    beat_logger = logging.getLogger('celery.beat')
    fh_beat = RotatingFileHandler(
        os.path.join(LOG_DIR, 'celery_beat.log'),
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5
    )
    fh_beat.setFormatter(formatter)
    beat_logger.addHandler(fh_beat)


# --- Celery App Initialization ---
app = Celery(
    'paper_collector',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
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
        'schedule': crontab(minute=15, hour='*'),
        'options': {'queue': 'harvest'},
    },
    'run-nightly-backfill-2023': {
        'task': 'pipelines.backfill_batch',
        'schedule': crontab(minute=0, hour=3),
        'args': (2023,),
        'options': {'queue': 'backfill'},
    }
}

if __name__ == '__main__':
    app.start()