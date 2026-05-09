import logging
import time

from app.config import settings
from app.queue import RedisDelayQueue
from app.service import JobSchedulerService
from app.storage import DynamoJobStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_once() -> int:
    store = DynamoJobStore.from_settings()
    queue = RedisDelayQueue.from_settings()
    store.create_tables()
    service = JobSchedulerService(store=store, queue=queue)
    count = service.enqueue_pending_window()
    logger.info("enqueued pending executions", extra={"count": count})
    return count


def run_forever() -> None:
    while True:
        run_once()
        time.sleep(settings.scheduler_poll_seconds)


if __name__ == "__main__":
    run_forever()
