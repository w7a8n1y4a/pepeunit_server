import uuid as uuid_pkg

from fastapi import APIRouter, BackgroundTasks, Depends, Request, status

from app.configs.rest import get_user_service
from app.repositories.user_repository import UserFilter
from app.schemas.bot.telegram_menu import TelegramMenu
from app.schemas.pydantic.user import (
    AccessToken,
    AuthChallengeCreate,
    AuthChallengeRead,
    AuthTokenCreate,
    UserCreate,
    UserRead,
    UsersResult,
    UserUpdate,
)
from app.services.user_service import UserService

router = APIRouter()


@router.post(
    "",
    response_model=UserRead,
    status_code=status.HTTP_201_CREATED,
)
def create(
    data: UserCreate, user_service: UserService = Depends(get_user_service)
):
    return UserRead(**user_service.create(data).dict())


@router.get("/{uuid}", response_model=UserRead)
def get(
    uuid: uuid_pkg.UUID, user_service: UserService = Depends(get_user_service)
):
    return UserRead(**user_service.get(uuid).dict())


@router.post("/auth/challenge", response_model=AuthChallengeRead)
async def create_challenge(
    data: AuthChallengeCreate,
    user_service: UserService = Depends(get_user_service),
):
    return AuthChallengeRead(
        challenge=await user_service.create_challenge(data)
    )


@router.post("/auth", response_model=AccessToken)
async def get_token(
    data: AuthTokenCreate,
    user_service: UserService = Depends(get_user_service),
):
    return AccessToken(token=await user_service.get_token(data))


@router.patch("/{uuid}", response_model=UserRead)
def update(
    data: UserUpdate, user_service: UserService = Depends(get_user_service)
):
    return UserRead(**user_service.update(data).dict())


@router.get("/generate_verification_link/", response_model=str)
async def get_verification(
    user_service: UserService = Depends(get_user_service),
):
    return await user_service.generate_verification_link()


@router.patch("/grant_admin/{uuid}", response_model=UserRead)
def grant_admin(
    uuid: uuid_pkg.UUID,
    request: Request,
    background_tasks: BackgroundTasks,
    user_service: UserService = Depends(get_user_service),
) -> UserRead:
    user = user_service.grant_admin(uuid)
    TelegramMenu.schedule_refresh(
        request, background_tasks, user.telegram_private_chat_id
    )
    return UserRead.model_validate(user, from_attributes=True)


@router.patch("/revoke_admin/{uuid}", response_model=UserRead)
def revoke_admin(
    uuid: uuid_pkg.UUID,
    request: Request,
    background_tasks: BackgroundTasks,
    user_service: UserService = Depends(get_user_service),
) -> UserRead:
    user = user_service.revoke_admin(uuid)
    TelegramMenu.schedule_refresh(
        request, background_tasks, user.telegram_private_chat_id
    )
    return UserRead.model_validate(user, from_attributes=True)


@router.patch("/block/{uuid}", status_code=status.HTTP_204_NO_CONTENT)
def block(
    uuid: uuid_pkg.UUID,
    request: Request,
    background_tasks: BackgroundTasks,
    user_service: UserService = Depends(get_user_service),
) -> None:
    user = user_service.block(uuid)
    TelegramMenu.schedule_refresh(
        request, background_tasks, user.telegram_private_chat_id
    )


@router.patch("/unblock/{uuid}", status_code=status.HTTP_204_NO_CONTENT)
def unblock(
    uuid: uuid_pkg.UUID,
    request: Request,
    background_tasks: BackgroundTasks,
    user_service: UserService = Depends(get_user_service),
) -> None:
    user = user_service.unblock(uuid)
    TelegramMenu.schedule_refresh(
        request, background_tasks, user.telegram_private_chat_id
    )


@router.get("", response_model=UsersResult)
def get_users(
    filters: UserFilter = Depends(UserFilter),
    user_service: UserService = Depends(get_user_service),
):
    count, users = user_service.list(filters)
    return UsersResult(
        count=count, users=[UserRead(**user.dict()) for user in users]
    )
