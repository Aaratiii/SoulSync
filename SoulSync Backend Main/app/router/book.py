from fastapi import APIRouter, HTTPException, Query, status

from app.models.book import Book, BookModel
from app.utils.logging import log as logger

router = APIRouter()


@router.post("/create", status_code=status.HTTP_201_CREATED)
def handleCreate(
    item: BookModel,
):
    try:
        Book.getInstance().create(item)
    except Exception as e:
        logger.error(f"Error in create book: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during creating book"
        )


@router.patch("/update", status_code=status.HTTP_200_OK)
def handleUpdate(
    item: BookModel,
):
    try:
        Book.getInstance().update(item)
    except Exception as e:
        logger.error(f"Error in update book: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during updating book"
        )


@router.delete("/delete", status_code=status.HTTP_200_OK)
def handleDelete(id: str = Query(..., min_length=1, description="book id")):
    try:
        Book.getInstance().delete(id)
    except Exception as e:
        logger.error(f"Error in delete book: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during deleting book"
        )
