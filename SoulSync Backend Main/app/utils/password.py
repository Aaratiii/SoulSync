import os
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from fastapi import HTTPException, Request
from passlib.context import CryptContext

SECRET_KEY = os.getenv("SECRET_KEY", "secret")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = 60

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# Function to verify the password
def verify_password(plain_password: str, hashed_password: str):
    return pwd_context.verify(plain_password, hashed_password)


# Function to hash the password
def get_password_hash(password: str):
    return pwd_context.hash(password)


# Function to create an access token
def create_access_token(to_encode: dict[str, Any]):
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# Function to decode the token and return the payload
def decode_token(header: str | None):
    if header is None or header == "" or len(header) < 7 or header[:7] != "Bearer ":
        raise HTTPException(status_code=401, detail="Token is missing")
    token = header[7:]
    payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    return payload


# Function to verify the access token extracted from the request
def verify_access_token(request: Request):
    # Extract the token from the request
    token = request.headers["Authorization"]
    try:
        return decode_token(token)
    except jwt.ExpiredSignatureError:
        # Raise an HTTPException with status code 401 if the token has expired
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        # Raise an HTTPException with status code 401 if the token is invalid
        raise HTTPException(status_code=401, detail="Invalid token")
