import time
import threading
from fastapi import FastAPI, Request
import uvicorn
import os
from app.app_manager_class import AppManager
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from app.app_manager_private_config import DB_CONFIG

stop_event = threading.Event()
info_lock = threading.Lock()
app_manager_status = {
    "running": True,
    "last_response": None,
}
stop_event.clear()


def app_manager_loop():
    app_manager_obj = AppManager()
    app_manager_status["running"] = True

    while not stop_event.is_set():
        time.sleep(0.3)
        result = app_manager_obj.run_iteration()
        with info_lock:
            app_manager_status["last_response"] = result
        print(result)

    app_manager_status["running"] = False
    print("AppManager stopped.")


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
                    f"Недействительный ключ микросервиса {key_value} {MICROSERVICE_SECRET_KEY}"
                )
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
            "running": app_manager_status["running"],
            "last_response": app_manager_status["last_response"],
        }


@app.post("/stop")
def stop_app_manager():
    stop_event.set()
    return {"message": "AppManager stopping..."}


@app.post("/start")
def start_app_manager():
    stop_event.clear()
    if not app_manager_status["running"]:
        threading.Thread(target=app_manager_loop, daemon=True).start()
        return {"message": "AppManager started"}
    else:
        return {"message": "AppManager already running"}


@app.get("/health")
def health_check():
    return {"status": "ok", "version": VERSION, "DB_CONFIG": str(DB_CONFIG)}


def start_api():
    uvicorn.run(app, host="0.0.0.0", port=5551)


api_thread = threading.Thread(target=start_api, daemon=True)
api_thread.start()
threading.Thread(target=app_manager_loop, daemon=True).start()
while True:
    time.sleep(1)
