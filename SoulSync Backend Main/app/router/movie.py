from fastapi import APIRouter, HTTPException, Query, status

from app.models.movie import Movie, MovieModel
from app.utils.logging import log as logger

router = APIRouter()


@router.post("/create", status_code=status.HTTP_201_CREATED)
def handleCreate(
    item: MovieModel,
):
    try:
        Movie.getInstance().create(item)
    except Exception as e:
        logger.error(f"Error in create movie: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during creating movie"
        )


@router.patch("/update", status_code=status.HTTP_200_OK)
def handleUpdate(
    item: MovieModel,
):
    try:
        Movie.getInstance().update(item)
    except Exception as e:
        logger.error(f"Error in update movie: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during updating movie"
        )


@router.delete("/delete", status_code=status.HTTP_200_OK)
def handleDelete(id: str = Query(..., min_length=1, description="movie id")):
    try:
        Movie.getInstance().delete(id)
    except Exception as e:
        logger.error(f"Error in delete movie: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during deleting movie"
        )
