from enum import Enum
from typing import Any, Self, Tuple

from pydantic import BaseModel
from pymongo import IndexModel
from pymongo.database import Collection, Database
from pymongo.errors import DuplicateKeyError

from app.models.book import BookModel
from app.models.movie import MovieModel
from app.utils.logging import log as logger


class PreferenceType(str, Enum):
    like = "like"
    dislike = "dislike"
    nil = ""


class UserBookModel(BookModel):
    preference: PreferenceType = PreferenceType.nil
    likes: int = 0
    dislikes: int = 0


class UserMovieModel(MovieModel):
    preference: PreferenceType = PreferenceType.nil
    likes: int = 0
    dislikes: int = 0


class PreferenceModel(BaseModel):
    user_id: str
    media_item_id: str
    preference: PreferenceType


class Preference:
    _instance: Self | None = None

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            cls._instance = Preference()
        return cls._instance

    def init(self, db: Database):
        self.db = db
        self.collection: Collection[dict[str, Any]] = db.get_collection("preferences")
        self.collection.create_indexes(
            [
                IndexModel(
                    {
                        "user_id": 1,
                        "media_item_id": 1,
                    },
                    unique=True,
                    name="user_item_unique",
                ),
                IndexModel(
                    {
                        "media_item_id": 1,
                        "preference": 1,
                    },
                    name="item_preference",
                ),
            ]
        )

    def update_preference(self, preference: PreferenceModel):
        filter_query = {
            "user_id": preference.user_id,
            "media_item_id": preference.media_item_id,
        }

        if preference.preference == PreferenceType.nil:
            # Delete the preference if it exists
            delete_result = self.collection.delete_one(filter_query)
            if delete_result.deleted_count > 0:
                logger.info(
                    f"Preference deleted for user {preference.user_id} on item {preference.media_item_id}"
                )
            else:
                logger.info(
                    f"No preference found to delete for user {preference.user_id} on item {preference.media_item_id}"
                )
        else:
            # Update or insert the preference
            update_data = {"$set": {"preference": preference.preference}}
            try:
                update_result = self.collection.update_one(
                    filter_query, update_data, upsert=True
                )
                if update_result.matched_count > 0:
                    logger.info(
                        f"Preference updated for user {preference.user_id} on item {preference.media_item_id}"
                    )
                elif update_result.upserted_id:
                    logger.info(
                        f"New preference created for user {preference.user_id} on item {preference.media_item_id}"
                    )
            except DuplicateKeyError:
                # This shouldn't happen due to our filter, but just in case
                logger.error(
                    f"Duplicate key error. Preference already exists for user {preference.user_id} on item {preference.media_item_id}"
                )

    def get_user_preference(self, user_id: str):
        results = self.collection.find({"user_id": user_id})
        return [PreferenceModel(**result) for result in results]

    def get_user_preference_for_media_item(self, user_id: str, media_item_id: str):
        result = self.collection.find_one(
            {"user_id": user_id, "media_item_id": media_item_id}
        )
        return (
            PreferenceModel(**result).preference
            if result is not None
            else PreferenceType.nil
        )

    def get_media_preference(self, media_item_id: str) -> Tuple[int, int]:
        pipeline = [
            {"$match": {"media_item_id": media_item_id}},
            {"$group": {"_id": "$preference", "count": {"$sum": 1}}},
            {"$project": {"_id": 0, "preference": "$_id", "count": 1}},
        ]

        results = list(self.collection.aggregate(pipeline))

        likes = 0
        dislikes = 0

        for result in results:
            if result["preference"] == "like":
                likes = result["count"]
            elif result["preference"] == "dislike":
                dislikes = result["count"]
            else:
                logger.error(f"Unknown preference type: {result['preference']}")
                raise ValueError(f"Unknown preference type: {result['preference']}")

        return likes, dislikes
