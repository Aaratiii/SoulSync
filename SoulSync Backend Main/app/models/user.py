import os
from typing import Any, Self

from pydantic import BaseModel, EmailStr, Field
from pymongo.collection import Collection
from pymongo.database import Database

from app.utils.password import get_password_hash, verify_password
from app.utils.uuid import gen_uuid


class UserModel(BaseModel):
    id: str = Field(alias="_id", default_factory=gen_uuid)
    email: EmailStr = Field(...)
    password: str = Field(...)
    full_name: str = Field(..., min_length=1, max_length=128)

    @staticmethod
    def from_user_create(user: "UserCreateModel"):
        return UserModel(
            email=user.email,
            full_name=user.full_name,
            password=user.password,
        )


class UserCreateModel(BaseModel):
    email: EmailStr = Field(...)
    password: str = Field(..., min_length=8)
    full_name: str = Field(..., min_length=1, max_length=128)


class UserVerifyModel(BaseModel):
    email: EmailStr = Field(...)
    password: str = Field(..., min_length=8)


class User:
    _instance: Self | None = None

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            cls._instance = User()
        return cls._instance

    def init(self, db: Database):
        self.db = db
        self.collection: Collection[dict[str, Any]] = db.get_collection("users")
        admin_email = os.getenv("ADMIN_EMAIL", "test@example.com")
        if self.collection.find_one({"email": admin_email}) is None:
            admin_password = os.getenv("ADMIN_PASSWORD", "password")
            admin_name = os.getenv("ADMIN_NAME", "ADMIN")
            admin_user = UserModel(
                email=admin_email,
                password=get_password_hash(admin_password),
                full_name=admin_name,
            )
            self.collection.insert_one(admin_user.model_dump(by_alias=True))

    def create(self, user: UserCreateModel):
        user.password = get_password_hash(user.password)
        new_user = UserModel(**user.model_dump())
        self.collection.insert_one(new_user.model_dump(by_alias=True))

    def verify(self, user: UserVerifyModel) -> bool:
        result = self.collection.find_one({"email": user.email})
        if result is None:
            return False
        db_user = UserModel(**result)
        if not verify_password(user.password, db_user.password):
            return False
        return True

    def get_by_email(self, email: str):
        result = self.collection.find_one({"email": email})
        return UserModel(**result) if result is not None else None

    def validate_user_id(self, user_id: str) -> bool:
        return self.collection.find_one({"_id": user_id}) is not None
