import gc
from typing import Annotated, Any

from fastapi import (APIRouter, BackgroundTasks, Header, HTTPException, Query,
                     status)

from app.models.book import Book
from app.models.media_item import MediaItem, MediaItemModel, MediaItemType
from app.models.movie import Movie
from app.models.preference import Preference, UserBookModel, UserMovieModel
from app.utils.logging import log as logger
from app.utils.password import decode_token

router = APIRouter()


def get_item(user_id: str, item: dict[str, Any]):
    if item["type"] == MediaItemType.book:
        book = Book.getInstance().get_by_id(item["_id"])
        if book is None:
            logger.error(f"Book not found: {item['_id']}")
            return None
        likes, dislikes = Preference.getInstance().get_media_preference(item["_id"])
        preference = Preference.getInstance().get_user_preference_for_media_item(
            user_id, item["_id"]
        )
        return UserBookModel(
            **book.model_dump(by_alias=True),
            likes=likes,
            dislikes=dislikes,
            preference=preference,
        )
    elif item["type"] == MediaItemType.movie:
        movie = Movie.getInstance().get_by_id(item["_id"])
        if movie is None:
            logger.error(f"Movie not found: {item['_id']}")
            return None
        likes, dislikes = Preference.getInstance().get_media_preference(item["_id"])
        preference = Preference.getInstance().get_user_preference_for_media_item(
            user_id, item["_id"]
        )
        return UserMovieModel(
            **movie.model_dump(by_alias=True),
            likes=likes,
            dislikes=dislikes,
            preference=preference,
        )
    else:
        logger.error(f"Unknown media type: {item['type']}")
        raise ValueError(f"Unknown media type: {item['type']}")


@router.get("/recommend", status_code=status.HTTP_200_OK)
def handleRecommend(
    background_tasks: BackgroundTasks,
    filter: MediaItemType = Query(
        MediaItemType.all, description="Filter by media type"
    ),
    authorization: Annotated[str | None, Header()] = None,
    limit: int = Query(10, description="How many items to return?"),
):
    try:
        user = decode_token(authorization)
        preferences = Preference.getInstance().get_user_preference(user["id"])
        results = MediaItem.getInstance().get_recommendations(
            filter, preferences, limit
        )
        items = [get_item(user["id"], item) for item in results]
        background_tasks.add_task(gc.collect)
        return [i for i in items if i is not None]
    except Exception as e:
        logger.error(f"Error in recommend: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during generating recommendation"
        )


@router.get(
    "/search",
    status_code=status.HTTP_200_OK,
    response_model=list[UserBookModel | UserMovieModel],
)
def handleSearch(
    authorization: Annotated[str | None, Header()] = None,
    filter: MediaItemType = Query(
        MediaItemType.all, description="Filter by media type"
    ),
    search: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, description="How many items to return?"),
):
    try:
        user = decode_token(authorization)
        results = MediaItem.getInstance().search(filter, search, limit)
        items = [get_item(user["id"], item) for item in results]
        return [i for i in items if i is not None]
    except Exception as e:
        logger.error(f"Error in search media item: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during searching media items"
        )


@router.post("/create", status_code=status.HTTP_201_CREATED)
def handleCreate(
    item: MediaItemModel,
):
    try:
        MediaItem.getInstance().create(item)
    except Exception as e:
        logger.error(f"Error in create media item: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during creating media item"
        )


@router.patch("/update", status_code=status.HTTP_200_OK)
def handleUpdate(
    item: MediaItemModel,
):
    try:
        MediaItem.getInstance().update(item)
    except Exception as e:
        logger.error(f"Error in update media item: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during updating media item"
        )


@router.delete("/delete", status_code=status.HTTP_200_OK)
def handleDelete(id: str = Query(..., min_length=1, description="media item id")):
    try:
        MediaItem.getInstance().delete(id)
    except Exception as e:
        logger.error(f"Error in delete media item: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during deleting media item"
        )
