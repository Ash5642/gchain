from fastapi import FastAPI, Depends
import api.models as api_models
import crypt.models as crypt_models
from sqlmodel import Field, SQLModel, select
from database import get_session
from fastapi import APIRouter
import api.message_models as message_models

import anchors.models as anchor_models

app = FastAPI()
router = APIRouter()
