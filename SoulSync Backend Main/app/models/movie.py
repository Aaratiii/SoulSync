from datetime import datetime
from typing import Any, Self

from pydantic import BaseModel, Field
from pymongo.collection import Collection
from pymongo.database import Database


class MovieModel(BaseModel):
    id: str = Field(..., alias="_id")
    type: str = "movie"
    budget: int
    creator: str
    description: str
    genres: list[str]
    imdb_id: str | None
    img_url: str
    origin_country: list[str]
    original_language: str
    original_title: str
    popularity: float
    production_companies: list[str]
    rating: float
    release_date: datetime | None
    revenue: int
    runtime: int
    spoken_languages: list[str]
    tagline: str | None
    title: str


class Movie:
    _instance: Self | None = None

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            cls._instance = Movie()
        return cls._instance

    def init(self, db: Database):
        self.db = db
        self.collection: Collection[dict[str, Any]] = db.get_collection("movies")

    def get_by_id(self, id: str):
        movie = self.collection.find_one({"_id": id})
        return MovieModel(**movie) if movie is not None else None

    def create(self, movie: MovieModel):
        self.collection.insert_one(movie.model_dump(by_alias=True))

    def update(self, movie: MovieModel):
        self.collection.replace_one({"_id": movie.id}, movie.model_dump(by_alias=True))

    def delete(self, id: str):
        self.collection.delete_one({"_id": id})
