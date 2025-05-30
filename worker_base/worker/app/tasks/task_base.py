from abc import ABC, abstractmethod
from enum import Enum
import time
from collections import deque

from app.worker_db_handler import WorkerDBHandler
from app.worker_logger import WorkerLogger


class TaskStatus(Enum):
    SUCCESS = 200
    ERROR = 500
    IN_PROGRESS = 102

    def __repr__(self):
        return f"Status.{self.name}"


class TaskError(Exception):

    def __init__(
        self,
        message,
        task_class_identifier,
    ):
        super().__init__(message)
        self.task_class_identifier = task_class_identifier

    def get_error_details(self):
        return {
            "error": str(self),
            "task_id": self.task_id,
            "task_class_identifier": self.task_class_identifier,
        }


class RequestLimiter:

    def __init__(self, max_requests: int, per_seconds: int):
        self.max_requests = max_requests
        self.per_seconds = per_seconds
        self.request_timestamps = deque()
        self.block_until = 0

    def is_request_allowed(self) -> bool:
        current_time = time.time()
        if current_time < self.block_until:
            print("--block_until--")
            return False

        while self.request_timestamps and self.request_timestamps[
                0] <= current_time - self.per_seconds:
            self.request_timestamps.popleft()

        if len(self.request_timestamps) < self.max_requests:
            self.request_timestamps.append(current_time)
            return True
        else:
            return False

    def block_for_60_seconds(self):
        self.block_until = time.time() + 60


class TaskResponse:

    def __init__(
        self,
        status: TaskStatus,
        task_class_identifier: str,
        store_id: int,
        additional_info: str = None,
    ):
        self.status = status
        self.additional_info = additional_info
        self.store_id = store_id
        self.task_class_identifier = task_class_identifier

    def __repr__(self):
        return f"TaskResponse(status={self.status}, task_class_identifier = {self.task_class_identifier}, store_id = {self.store_id} additional_info='{self.additional_info}')"


class TaskBase(ABC):
    task_class_identifier: str = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.task_class_identifier is None:
            raise TypeError(
                f"Class {cls.__name__} must define 'task_class_identifier'")

    def __init__(
        self,
        db_handler: WorkerDBHandler,
        logger: WorkerLogger,
        store_id: int,
        api_token: str,
        last_run_time: int,
    ):
        self.status: TaskStatus = TaskStatus.IN_PROGRESS
        self.db_handler = db_handler
        self.logger = logger
        self.store_id = store_id
        self.api_token = api_token
        self.last_run_time = last_run_time

    def _make_response(
        self,
        additional_info: str = None,
    ) -> TaskResponse:
        return TaskResponse(
            status=self.status,
            task_class_identifier=self.__class__.task_class_identifier,
            store_id=self.store_id,
            additional_info=additional_info,
        )

    def raise_error(
        self,
        message,
    ):
        raise TaskError(
            message,
            self.__class__.task_class_identifier,
        )

    @abstractmethod
    def process(self) -> TaskResponse:
        pass
