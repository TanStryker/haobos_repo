import os
import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from hrms.core.auth import SessionStore, ensure_default_admin
from hrms.core.oplog import append_oplog
from hrms.storage.sqlite_db import SQLiteDB
from hrms.modules.auth_routes import router as auth_router
from hrms.modules.employees_routes import router as employees_router
from hrms.modules.overtime_routes import router as overtime_router
from hrms.modules.attendance_routes import router as attendance_router
from hrms.modules.salary_routes import router as salary_router
from hrms.modules.system_routes import router as system_router
from hrms.modules.dashboard_routes import router as dashboard_router


def create_app() -> FastAPI:
    app = FastAPI(title="HRMS", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(base_dir), "data")
    log_dir = os.path.join(os.path.dirname(base_dir), "logs")
    ui_dir = os.path.join(os.path.dirname(base_dir), "ui")

    db_path = os.environ.get("HRMS_DB_PATH") or os.path.join(data_dir, "hrms.sqlite3")
    db = SQLiteDB(db_path)
    db.init_schema()
    db.migrate_from_json_dir(data_dir)
    ensure_default_admin(db)

    app.state.db = db
    app.state.sessions = SessionStore()
    app.state.log_dir = log_dir

    @app.middleware("http")
    async def oplog_middleware(request: Request, call_next):
        start = time.time()
        response = None
        exc = None
        try:
            response = await call_next(request)
            return response
        except Exception as e:
            exc = e
            raise
        finally:
            duration_ms = int((time.time() - start) * 1000)
            status_code = getattr(response, "status_code", 500)
            append_oplog(
                log_dir,
                {
                    "method": request.method,
                    "path": request.url.path,
                    "query": str(request.url.query),
                    "status_code": status_code,
                    "duration_ms": duration_ms,
                    "error": str(exc) if exc else "",
                },
            )

    app.mount("/ui", StaticFiles(directory=ui_dir, html=True), name="ui")

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse(url="/ui/")

    app.include_router(auth_router)
    app.include_router(dashboard_router)
    app.include_router(system_router)
    app.include_router(employees_router)
    app.include_router(overtime_router)
    app.include_router(attendance_router)
    app.include_router(salary_router)
    return app


app = create_app()
