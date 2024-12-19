import heapq
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Self

import joblib
import numpy as np
from pydantic import BaseModel, Field
from pymongo.database import Collection, Database
from scipy import sparse
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MinMaxScaler
from sklearn.random_projection import GaussianRandomProjection

from app.models.preference import PreferenceModel, PreferenceType
from app.utils.logging import log as logger

# Initialize this once, outside the function
n_components = 200  # Adjust based on desired accuracy vs. speed trade-off
random_projection = GaussianRandomProjection(n_components=n_components)


class MediaItemType(str, Enum):
    book = "book"
    movie = "movie"
    all = "all"


class MediaItemModel(BaseModel):
    id: str = Field(..., alias="_id")
    type: MediaItemType
    creator: str
    description: str
    genres: list[str]
    title: str
    pages_runtime: int | None
    release_date: datetime | None
    rating: float


@dataclass
class FeatureWeights:
    title: float = 0.15
    description: float = 0.3
    creator: float = 0.1
    genres: float = 0.25
    release_date: float = 0.1
    pages_runtime: float = 0.1

    def __post_init__(self):
        if not np.isclose(sum(self.__dict__.values()), 1.0, atol=1e-5):
            raise ValueError("Weights must sum to 1")
        if any(not 0 <= weight <= 1 for weight in self.__dict__.values()):
            raise ValueError("All weights must be between 0 and 1")

    def to_dict(self) -> dict[str, float]:
        return self.__dict__


class MediaItem:
    _instance: Self | None = None

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            cls._instance = MediaItem()
        return cls._instance

    def init(
        self,
        db: Database,
        vector_filename: str,
        weights: FeatureWeights,
        force_compute_weights=False,
    ):
        self.db = db
        self.collection: Collection[dict[str, Any]] = db.get_collection("mediaItems")
        # Ensure text index is created for relevant fields
        self.collection.create_index(
            [("title", "text"), ("creator", "text"), ("description", "text")]
        )
        self.collection.create_index([("type", 1)])
        self.vector_filename = vector_filename
        self.item_vectors: dict = {}
        self.item_ids: list[str] = []
        self.date_scaler = MinMaxScaler()
        self.runtime_scaler = MinMaxScaler()
        self.weights = weights
        self.feature_sizes: dict = {}
        self.reduced_vectors: np.ndarray = None
        self.item_id_to_index: dict[str, int] = {}
        if not os.path.exists(vector_filename) or force_compute_weights:
            self._precompute_vectors()
            self._load_vectors()
        else:
            logger.debug("Skipping vectors calculations")
            self._load_vectors()

    def create(self, item: MediaItemModel):
        self.collection.insert_one(item.model_dump(by_alias=True))

    def update(self, item: MediaItemModel):
        self.collection.update_one(
            {"_id": item.id}, {"$set": item.model_dump(by_alias=True)}
        )

    def delete(self, id: str):
        self.collection.delete_one({"_id": id})

    def _precompute_vectors(self, batch_size=10000):
        total_items = self.collection.count_documents({})
        feature_texts = {
            feature: [] for feature in ["title", "description", "creator", "genres"]
        }

        dates = []
        self.item_ids = []
        runtimes = []

        for i in range(0, total_items, batch_size):
            batch = (
                self.collection.find(
                    {},
                )
                .skip(i)
                .limit(batch_size)
            )
            try:
                batch_media_items = [MediaItemModel(**item) for item in batch]
                self.item_ids.extend([item.id for item in batch_media_items])
                feature_texts["title"].extend(
                    [item.title for item in batch_media_items]
                )
                feature_texts["description"].extend(
                    [item.description for item in batch_media_items]
                )
                feature_texts["creator"].extend(
                    [item.creator for item in batch_media_items]
                )
                feature_texts["genres"].extend(
                    " ".join(item.genres) for item in batch_media_items
                )
                dates.extend(
                    [
                        (
                            np.nan
                            if item.release_date is None
                            else item.release_date.timestamp()
                        )
                        for item in batch_media_items
                    ]
                )
                runtimes.extend(
                    [
                        np.nan if item.pages_runtime is None else item.pages_runtime
                        for item in batch_media_items
                    ]
                )
            except Exception as e:
                logger.error(f"Error processing batch {i}: {e}")
                raise e
        logger.debug("Aggregated data from MongoDB")
        self.item_vectors = {}
        self.feature_sizes = {}
        for feature, texts in feature_texts.items():
            vectorizer = TfidfVectorizer(stop_words="english")
            self.item_vectors[feature] = vectorizer.fit_transform(texts)
            self.feature_sizes[feature] = self.item_vectors[feature].shape[1]

        dates_array = np.array(dates).reshape(-1, 1)
        date_mask = ~np.isnan(dates_array.flatten())
        self.date_scaler.fit(dates_array[date_mask])

        normalized_dates = np.zeros(dates_array.shape)
        normalized_dates[date_mask] = self.date_scaler.transform(dates_array[date_mask])
        self.item_vectors["release_date"] = normalized_dates
        self.feature_sizes["release_date"] = 1

        runtimes_array = np.array(runtimes).reshape(-1, 1)
        runtime_mask = ~np.isnan(runtimes_array.flatten())
        self.runtime_scaler.fit(runtimes_array[runtime_mask])
        normalized_runtimes = np.zeros(runtimes_array.shape)
        normalized_runtimes[runtime_mask] = self.runtime_scaler.transform(
            runtimes_array[runtime_mask]
        )
        self.item_vectors["pages_runtime"] = normalized_runtimes
        self.feature_sizes["pages_runtime"] = 1

        joblib.dump(
            (
                self.item_vectors,
                self.item_ids,
                self.date_scaler,
                self.runtime_scaler,
                self.feature_sizes,
            ),
            self.vector_filename,
        )
        logger.info(f"Vectors computed and saved to {self.vector_filename}")

    def _load_vectors(self) -> None:
        (
            self.item_vectors,
            self.item_ids,
            self.date_scaler,
            self.runtime_scaler,
            self.feature_sizes,
        ) = joblib.load(self.vector_filename)
        weighted_vectors = self._get_weighted_vectors()
        self.reduced_vectors = random_projection.fit_transform(weighted_vectors)
        self.item_id_to_index = {
            item_id: index for index, item_id in enumerate(self.item_ids)
        }

    def _get_weighted_vectors(self) -> csr_matrix:
        weighted_vectors = []
        for feature, weight in self.weights.to_dict().items():
            if feature in self.item_vectors:
                feature_vector = self.item_vectors[feature]
                if not sparse.issparse(feature_vector):
                    feature_vector = csr_matrix(feature_vector)
                weighted_vectors.append(feature_vector * weight)
        return sparse.hstack(weighted_vectors)

    def get_popular_items(self, n: int, filter: MediaItemType) -> list[dict[str, Any]]:
        query = {} if filter == MediaItemType.all else {"type": filter}
        results = (
            self.collection.find(query, {"_id": 1, "type": 1})
            .sort("rating", -1)
            .limit(n)
        )
        return list(results)

    def search(self, filter_type: MediaItemType, search: str, limit: int):
        # Base query
        query: dict[str, Any] = {"$text": {"$search": search}}

        # Add type filter if not "all"
        if filter_type != MediaItemType.all:
            query["type"] = filter_type

        # Perform the search
        results = (
            self.collection.find(
                query,
                {
                    "_id": 1,
                    "type": 1,
                    "score": {"$meta": "textScore"},  # Include the text match score
                },
            )
            .sort([("score", {"$meta": "textScore"})])  # Sort by relevance
            .limit(limit)
        )
        return list(results)

    def validate_media_id(self, media_id: str) -> bool:
        return self.collection.find_one({"_id": media_id}) is not None

    def get_recommendations(
        self,
        filter: MediaItemType,
        user_preferences: list[PreferenceModel],
        n_recommendations: int = 10,
        diversity_factor: float = 0.2,
    ):
        # Filter items by type in the database
        if filter != MediaItemType.all:
            filtered_items = list(self.collection.find({"type": filter}, {"_id": 1}))
            filtered_item_ids = [item["_id"] for item in filtered_items]
            filtered_indices = [
                self.item_id_to_index[item_id]
                for item_id in filtered_item_ids
                if item_id in self.item_id_to_index
            ]
        else:
            filtered_indices = list(range(len(self.item_ids)))
            filtered_item_ids = self.item_ids

        filtered_vectors = self.reduced_vectors[filtered_indices]

        liked_indices = [
            filtered_item_ids.index(pref.media_item_id)
            for pref in user_preferences
            if pref.preference == PreferenceType.like
            and pref.media_item_id in filtered_item_ids
        ]
        disliked_indices = [
            filtered_item_ids.index(pref.media_item_id)
            for pref in user_preferences
            if pref.preference == PreferenceType.dislike
            and pref.media_item_id in filtered_item_ids
        ]

        if not liked_indices:
            return self.get_popular_items(n_recommendations, filter)

        user_profile = np.mean(filtered_vectors[liked_indices], axis=0)
        if disliked_indices:
            user_profile -= 0.5 * np.mean(filtered_vectors[disliked_indices], axis=0)

        def process_batch(start, end):
            batch = filtered_vectors[start:end]
            dot_products = np.dot(user_profile, batch.T)
            batch_similarities = dot_products / (
                np.linalg.norm(user_profile) * np.linalg.norm(batch, axis=1)
            )
            top_indices = np.argpartition(batch_similarities, -2 * n_recommendations)[
                -2 * n_recommendations :
            ]
            return [(start + i, batch_similarities[i]) for i in top_indices]

        batch_size = 10_000
        top_similarities = []
        for i in range(0, filtered_vectors.shape[0], batch_size):
            batch_top = process_batch(i, min(i + batch_size, filtered_vectors.shape[0]))
            top_similarities.extend(batch_top)
            top_similarities = heapq.nlargest(
                2 * n_recommendations, top_similarities, key=lambda x: x[1]
            )

        top_indices, similarities = zip(*top_similarities)
        top_indices = np.array(top_indices)
        similarities = np.array(similarities)

        mask = np.isin(top_indices, liked_indices + disliked_indices, invert=True)
        top_indices = top_indices[mask]
        similarities = similarities[mask]

        if np.random.random() < diversity_factor:
            n_top = n_recommendations // 2
            n_diverse = n_recommendations - n_top
            top_picks = top_indices[:n_top]
            diverse_picks = np.random.choice(
                top_indices[n_top:], size=n_diverse, replace=False
            )
            selected_indices = np.concatenate([top_picks, diverse_picks])
            np.random.shuffle(selected_indices)
        else:
            selected_indices = top_indices[:n_recommendations]

        recommended_items: list[dict[str, Any]] = []
        for i in selected_indices:
            item_id = filtered_item_ids[i]
            result = {
                "_id": item_id,
                "type": (
                    filter
                    if filter != MediaItemType.all
                    else self.collection.find_one({"_id": item_id})["type"]
                ),
            }
            recommended_items.append(result)
            if len(recommended_items) == n_recommendations:
                break

        return recommended_items
