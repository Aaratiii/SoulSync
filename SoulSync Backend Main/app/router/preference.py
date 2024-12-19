from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, status

from app.models.media_item import MediaItem
from app.models.preference import Preference, PreferenceModel
from app.models.user import User
from app.utils.logging import log as logger
from app.utils.password import decode_token

router = APIRouter()


@router.post("/upsert", status_code=status.HTTP_200_OK, response_model=None)
def handleUpsert(
    preference: PreferenceModel, authorization: Annotated[str | None, Header()] = None
) -> None:
    try:
        user = decode_token(authorization)
        if user["id"] != preference.user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User id does not match token",
            )
        if not User.getInstance().validate_user_id(
            preference.user_id
        ) or not MediaItem.getInstance().validate_media_id(preference.media_item_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid user or media_item id",
            )
        Preference.getInstance().update_preference(preference)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in upsert preference: {str(e)}")
        raise HTTPException(
            status_code=500, detail="An error occurred during the update"
        )
