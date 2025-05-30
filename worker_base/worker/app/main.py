import time
import threading
from fastapi import FastAPI, Request
import uvicorn
from app.worker_class import Worker
import os
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from app.worker_private_config import DB_CONFIG

stop_event = threading.Event()
info_lock = threading.Lock()
worker_status = {
    "running": True,
    "last_response": "worker not started",
}
stop_event.clear()


def worker_loop():
    worker_status["last_response"] = "try to make worker"
    worker_obj = Worker()
    worker_status["running"] = True
    worker_status["last_response"] = "main loop started"

    while not stop_event.is_set():
        time.sleep(0.01)
        result = worker_obj.run_iteration()
        with info_lock:
            worker_status["last_response"] = result
        print(result)

    worker_status["running"] = False
    print("Worker stopped.")


MICROSERVICE_SECRET_KEY = os.getenv("MICROSERVICE_SECRET_KEY")
VERSION = os.getenv("VERSION")


class MicroserviceAuthMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next):
        key = request.headers.get("authorization-microservice")
        if key and key.startswith("Bearer "):
            key_value = key.split(" ")[1]
            if key_value == MICROSERVICE_SECRET_KEY:
                request.state.service = {"authorized": True}
                return await call_next(request)
            else:
                return self._generate_error_response(
                    f"Недействительный ключ микросервиса")
        else:
            return self._generate_error_response(
                "Отсутствует 'authorization-microservice' или начало не с 'Bearer '"
            )

    def _generate_error_response(self, error_detail: str):
        return JSONResponse(status_code=401, content={"detail": error_detail})


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(MicroserviceAuthMiddleware)


@app.get("/status")
def get_status():
    with info_lock:
        return {
            "running": worker_status["running"],
            "last_response": worker_status["last_response"],
        }


@app.post("/stop")
def stop_worker():
    stop_event.set()
    return {"message": "Worker stopping..."}


@app.post("/start")
def start_worker():
    stop_event.clear()
    if not worker_status["running"]:
        threading.Thread(target=worker_loop, daemon=True).start()
        return {"message": "Worker started"}
    else:
        return {"message": "Worker already running"}


@app.get("/health")
def health_check():
    return {"status": "ok", "version": VERSION, "DB_CONFIG": str(DB_CONFIG)}


def start_api():
    uvicorn.run(app, host="0.0.0.0", port=5553)


api_thread = threading.Thread(target=start_api, daemon=True)
api_thread.start()
threading.Thread(target=worker_loop, daemon=True).start()
while True:
    time.sleep(1)
