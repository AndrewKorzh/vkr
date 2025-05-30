import time
from enum import Enum
from typing import Dict, List, Optional
import time

from app.tasks.task_base import (
    TaskResponse,
    TaskStatus,
)

from app.tasks.task_base import TaskBase, TaskStatus, TaskError
from app.tasks.task_cards_list import taskCardsList
from app.tasks.task_nm_report_detail import taskNmReportDetail
from app.tasks.task_fact_stock import taskFactStock
from app.tasks.task_fact_sales import taskFactSales
from app.tasks.task_advert_info import taskAdvertInfo
from app.tasks.task_advert import taskAdvert

from app.worker_db_handler import WorkerDBHandler
from app.worker_logger import WorkerLogger

MAX_STORE_PROCESS_ERROR_AMOUNT = 100
MAX_STORE_PROCESS_LIVE_SECONDS = 5600


class StoreProcessStatus(Enum):
    SUCCESS = 200
    ERROR = 500
    IN_PROGRESS = 102

    def __repr__(self):
        return f"Status.{self.name}"


class StoreProcessResponse:

    def __init__(self,
                 status: StoreProcessStatus,
                 additional_info: str = None):
        self.status = status
        self.additional_info = additional_info

    def __repr__(self):
        return f"StoreProcessResponse(status={self.status}, additional_info='{self.additional_info}')"


class StoreProcess:

    def __init__(
        self,
        store_id,
        store_process_id,
        store_name,
        api_token,
        secret_key,
        db_handler,
        logger,
    ):

        self.store_id = store_id
        self.store_process_id = store_process_id
        self.store_name = store_name
        self.api_token = api_token
        self.secret_key = secret_key
        self.db_handler: WorkerDBHandler = db_handler
        self.logger: WorkerLogger = logger
        self.error_count = 0
        self.start_time = time.time()

        self.tasks: List[TaskBase] = [
            taskCardsList(
                db_handler=db_handler,
                logger=logger,
                store_id=store_id,
                api_token=api_token,
                last_run_time=0,
            ),
            taskNmReportDetail(
                db_handler=db_handler,
                logger=logger,
                store_id=store_id,
                api_token=api_token,
                last_run_time=5,
            ),
            taskFactStock(
                db_handler=db_handler,
                logger=logger,
                store_id=store_id,
                api_token=api_token,
                last_run_time=10,
            ),
            taskFactSales(
                db_handler=db_handler,
                logger=logger,
                store_id=store_id,
                api_token=api_token,
                last_run_time=15,
            ),
            taskAdvertInfo(
                db_handler=db_handler,
                logger=logger,
                store_id=store_id,
                api_token=api_token,
                last_run_time=30,
            ),
            taskAdvert(
                db_handler=db_handler,
                logger=logger,
                store_id=store_id,
                api_token=api_token,
                last_run_time=40,
            ),
        ]

    def get_earliest_task(self) -> Optional[TaskBase]:
        if not self.tasks:
            return None

        earliest_task = None
        earliest_time = float('inf')

        for task in self.tasks:
            last_run_time = task.last_run_time
            if last_run_time < earliest_time and task.status == TaskStatus.IN_PROGRESS:
                earliest_task = task
                earliest_time = last_run_time

        if earliest_task:
            earliest_task.last_run_time = time.time()

        return earliest_task

    def check_tasks_ready(self):
        for task in self.tasks:
            if task.status == TaskStatus.IN_PROGRESS:
                return False

        return True

    def store_process_iter(self) -> StoreProcessResponse:
        is_ready = self.check_tasks_ready()

        if (is_ready):
            return StoreProcessResponse(StoreProcessStatus.SUCCESS)

        task = self.get_earliest_task()
        print(
            f"---- start task: {task.__class__.task_class_identifier} store_id: {self.store_id} "
        )
        task_resp = None
        try:
            task_resp = task.process()
        except Exception as e:
            if isinstance(e, TaskError):
                self.logger.error(
                    source=e.task_class_identifier,
                    store_id=self.store_id,
                    message=str(e),
                )
            else:
                self.logger.error(
                    source="store_process",
                    store_id=self.store_id,
                    message=f"error: {e}",
                )
            self.error_count += 1

        if self.error_count > MAX_STORE_PROCESS_ERROR_AMOUNT:
            self.logger.error(
                source="store_process",
                store_id=self.store_id,
                message=f"error: {e}",
            )
            StoreProcessResponse(StoreProcessStatus.ERROR)

        current_time = time.time()
        if current_time - self.start_time > MAX_STORE_PROCESS_LIVE_SECONDS:
            self.logger.warning(
                source="store_process",
                store_id=self.store_id,
                message=f"To long store process live",
            )
            StoreProcessResponse(StoreProcessStatus.ERROR)

        print(
            f"---- end task: {task.__class__.task_class_identifier} store_id: {self.store_id}, status: {task.status}"
        )

        return StoreProcessResponse(StoreProcessStatus.IN_PROGRESS)

    def to_string(self):
        obj_str = f"""
        StoreProcess
            store_id: {self.store_id}
            store_process_id: {self.store_process_id}
            store_name: {self.store_name}
            api_token: {self.api_token}
            secret_key: {self.secret_key}
        ---------------------------------
        """
        return obj_str
