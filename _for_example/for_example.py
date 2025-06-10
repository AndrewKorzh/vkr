# import time
# from enum import Enum
# from typing import Dict, List, Optional
# import time

# from app.tasks.task_cards_list import taskCardsList
# from app.tasks.task_nm_report_detail import taskNmReportDetail
# from app.tasks.task_fact_stock import taskFactStock
# from app.tasks.task_fact_sales import taskFactSales
# from app.tasks.task_advert_info import taskAdvertInfo
# from app.tasks.task_advert import taskAdvert

# MAX_STORE_PROCESS_ERROR_AMOUNT = 100
# MAX_STORE_PROCESS_LIVE_SECONDS = 5600

# class TaskStatus(Enum):
#     SUCCESS = 200
#     ERROR = 500
#     IN_PROGRESS = 102

#     def __repr__(self):
#         return f"Status.{self.name}"

# class TaskResponse:

#     def __init__(
#         self,
#         status: TaskStatus,
#         task_class_identifier: str,
#         store_id: int,
#         additional_info: str = None,
#     ):
#         self.status = status
#         self.additional_info = additional_info
#         self.store_id = store_id
#         self.task_class_identifier = task_class_identifier

# class TaskError(Exception):

#     def __init__(
#         self,
#         message,
#         task_class_identifier,
#     ):
#         super().__init__(message)
#         self.task_class_identifier = task_class_identifier

#     def get_error_details(self):
#         return {
#             "error": str(self),
#             "task_id": self.task_id,
#             "task_class_identifier": self.task_class_identifier,
#         }

# from abc import ABC, abstractmethod

# class TaskBase(ABC):
#     task_class_identifier: str = None

#     def __init_subclass__(cls, **kwargs):
#         super().__init_subclass__(**kwargs)
#         if cls.task_class_identifier is None:
#             raise TypeError(
#                 f"Class {cls.__name__} must define 'task_class_identifier'")

#     def __init__(self, db_handler: WorkerDBHandler, logger: WorkerLogger,
#                  store_id: int, api_token: str, last_run_time: int):
#         self.status: TaskStatus = TaskStatus.IN_PROGRESS
#         self.db_handler = db_handler
#         self.logger = logger
#         self.store_id = store_id
#         self.api_token = api_token
#         self.last_run_time = last_run_time

#     def _make_response(self, additional_info: str = None) -> TaskResponse:
#         return TaskResponse(
#             status=self.status,
#             task_class_identifier=self.__class__.task_class_identifier,
#             store_id=self.store_id,
#             additional_info=additional_info)

#     @abstractmethod
#     def process(self) -> TaskResponse:
#         pass

#     def raise_error(
#         self,
#         message,
#     ):
#         raise TaskError(message, self.__class__.task_class_identifier)

# class StoreProcessStatus(Enum):
#     SUCCESS = 200
#     ERROR = 500
#     IN_PROGRESS = 102

#     def __repr__(self):
#         return f"Status.{self.name}"

# class WorkerDBHandler:

#     def __init__(self):
#         pass

# class WorkerLogger:

#     def __init__(self):
#         pass

#     def error(self):
#         return

# class StoreProcessResponse:

#     def __init__(self,
#                  status: StoreProcessStatus,
#                  additional_info: str = None):
#         self.status = status
#         self.additional_info = additional_info

#     def __repr__(self):
#         return f"StoreProcessResponse(status={self.status}, additional_info='{self.additional_info}')"

# class StoreProcess:

#     def __init__(self, store_id, store_process_id, store_name, api_token,
#                  secret_key, db_handler, logger):

#         self.store_id = store_id
#         self.store_process_id = store_process_id
#         self.store_name = store_name
#         self.api_token = api_token
#         self.secret_key = secret_key
#         self.db_handler: WorkerDBHandler = db_handler
#         self.logger: WorkerLogger = logger
#         self.error_count = 0
#         self.start_time = time.time()

#         self.tasks: List[TaskBase] = [
#             # ...
#         ]

#     def get_earliest_task(self) -> Optional[TaskBase]:
#         if not self.tasks:
#             return None
#         earliest_task = None
#         earliest_time = float('inf')
#         for task in self.tasks:
#             last_run_time = task.last_run_time
#             if last_run_time < earliest_time and task.status == TaskStatus.IN_PROGRESS:
#                 earliest_task = task
#                 earliest_time = last_run_time
#         if earliest_task:
#             earliest_task.last_run_time = time.time()
#         return earliest_task

#     def check_tasks_ready(self):
#         for task in self.tasks:
#             if task.status == TaskStatus.IN_PROGRESS:
#                 return False
#         return True

#     def store_process_iter(self) -> StoreProcessResponse:
#         is_ready = self.check_tasks_ready()
#         if (is_ready):
#             return StoreProcessResponse(StoreProcessStatus.SUCCESS)
#         task = self.get_earliest_task()
#         try:
#             task.process()
#         except Exception as e:
#             self.logger.error(
#                 source="StoreProcess",
#                 store_id=self.store_id,
#                 message=e,
#             )

#         return StoreProcessResponse(StoreProcessStatus.IN_PROGRESS)
