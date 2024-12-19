from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.models.user import User, UserCreateModel, UserVerifyModel
from app.utils.logging import log as logger
from app.utils.password import create_access_token

router = APIRouter()


class Token(BaseModel):
    access_token: str
    token_type: str


@router.post("/signup", status_code=status.HTTP_201_CREATED)
def handleSignup(user: UserCreateModel):
    try:
        logger.debug(f"signing up user {user.email}")
        User.getInstance().create(user)
        logger.debug(f"Created user {user.email}")
    except Exception as e:
        logger.error(f"Error in signup: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during signup",
        )


@router.post("/login", status_code=status.HTTP_200_OK, response_model=Token)
def handleLogin(user: UserVerifyModel):
    try:
        logger.debug(f"logging in user {user.email}")
        user_db = User.getInstance()
        if not user_db.verify(user):
            logger.debug(f"failed logging in user {user.email}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
            )
        logged_user = user_db.get_by_email(user.email)
        if logged_user is None:
            logger.debug(f"(shouldn't happen) failed logging in user {user.email}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
            )
        access_token = create_access_token(logged_user.model_dump(exclude={"password"}))
        logger.debug(f"logged in user {user.email}")
        return Token(access_token=access_token, token_type="Bearer")
    except HTTPException:
        # Re-raise HTTPExceptions without modifying them
        raise
    except Exception as e:
        logger.error(f"Error in login: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during login",
        )
