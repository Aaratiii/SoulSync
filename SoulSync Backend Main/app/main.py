import gc
import os
import time
from contextlib import asynccontextmanager

import pymongo
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from app.models.book import Book
from app.models.media_item import FeatureWeights, MediaItem
from app.models.movie import Movie
from app.models.preference import Preference
from app.models.user import User
from app.router.book import router as book_router
from app.router.media_item import router as media_items_router
from app.router.movie import router as movie_router
from app.router.preference import router as preference_router
from app.router.user import router as user_router
from app.utils.logging import log as logger
from app.utils.password import verify_access_token

load_dotenv()

port = int(os.getenv("PORT", "8000"))
env = os.getenv("ENV", "development")
connection_string = os.getenv("MONGODB_CONNECTION")
if connection_string is None:
    raise ValueError("MONGODB_CONNECTION is required")
database = os.getenv("MONGODB_DATABASE", "")
if database == "":
    raise ValueError("MONGODB_DATABASE is required")

version = "0.0.1"
root_path = "/api/v1"
unauthorized_paths = [
    f"{root_path}/openapi.json",
    f"{root_path}/users/login",
    f"{root_path}/users/signup",
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    client: pymongo.MongoClient = pymongo.MongoClient(connection_string)
    db = client[database]
    User.getInstance().init(db)
    MediaItem.getInstance().init(db, "vectors/item_vectors.joblib", FeatureWeights())
    Book.getInstance().init(db)
    Movie.getInstance().init(db)
    Preference.getInstance().init(db)
    gc.collect()
    logger.info("---Initialised application---")
    yield
    client.close()
    logger.info("---Shutting down---")


app = FastAPI(
    debug=env == "development",
    lifespan=lifespan,
    version=version,
    title="Recommendation API",
    description="API for recommendation system",
    root_path=root_path,
)


@app.middleware("http")
async def authenticate(request: Request, call_next):
    try:
        if (
            request.url.path.startswith(root_path)
            and request.url.path not in unauthorized_paths
            and request.method != "OPTIONS"
        ):
            # Call the verify_access_token function to validate the token
            verify_access_token(request)
        # If token validation succeeds, continue to the next middleware or route handler
        response = await call_next(request)
        return response
    except HTTPException as exc:
        # If token validation fails due to HTTPException, return the error response
        return JSONResponse(content={"detail": exc.detail}, status_code=exc.status_code)
    except Exception as exc:
        # If token validation fails due to other exceptions, return a generic error response
        return JSONResponse(content={"detail": f"Error: {str(exc)}"}, status_code=500)


app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


@app.get("/")
def handleRoot():
    return {"status": "ok", "version": version}


app.include_router(user_router, prefix="/users", tags=["users"])
app.include_router(media_items_router, prefix="/media_items", tags=["media_items"])
app.include_router(preference_router, prefix="/preferences", tags=["preferences"])
app.include_router(book_router, prefix="/books", tags=["books"])
app.include_router(movie_router, prefix="/movies", tags=["movies"])
