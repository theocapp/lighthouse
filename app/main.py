import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.routes.pages import router as pages_router
from app.routes.api import router as api_router

app = FastAPI(title="Lighthouse", docs_url="/api/docs")

app.include_router(pages_router)
app.include_router(api_router)

static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
