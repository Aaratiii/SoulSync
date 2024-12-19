from datetime import datetime
from typing import Any, Self

from pydantic import BaseModel, Field
from pymongo.collection import Collection
from pymongo.database import Database


class BookModel(BaseModel):
    id: str = Field(..., alias="_id")
    type: str = "book"
    creator: str
    img_url: str
    description: str
    genres: list[str]
    title: str
    liked_percent: int
    pages: int | None
    price: float | None
    release_date: datetime
    publisher: str | None
    rating: float


class Book:
    _instance: Self | None = None

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            cls._instance = Book()
        return cls._instance

    def init(self, db: Database):
        self.db = db
        self.collection: Collection[dict[str, Any]] = db.get_collection("books")

    def get_by_id(self, id: str):
        book = self.collection.find_one({"_id": id})
        return BookModel(**book) if book is not None else None

    def create(self, book: BookModel):
        self.collection.insert_one(book.model_dump(by_alias=True))

    def update(self, book: BookModel):
        self.collection.replace_one({"_id": book.id}, book.model_dump(by_alias=True))

    def delete(self, id: str):
        self.collection.delete_one({"_id": id})
