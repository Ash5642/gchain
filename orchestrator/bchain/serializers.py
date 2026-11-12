from typing import Optional
from fastapi import FastAPI, Depends
from sqlmodel import Field, SQLModel, Session, Relationship, create_engine, select
import api.models as api_models
import crypt.models as crypt_models
from cryptography.hazmat.primitives.asymmetric import rsa
from Crypto.Cipher import AES
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import utils
from Crypto.Cipher import AES
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
import base64, json, secrets, hashlib
from pydantic import BaseModel, ConfigDict, field_validator
import api.serializers as api_serializers
from typing import List, Optional


class OrgMessage(SQLModel, table=False):
    name: str
    data: dict


ROLE_ACTIONS = {
    "add", "remove"
    # Add hundreds more options here
}
class OrgRoleMessage(SQLModel, table=False):
    org: str
    role: str
    action: str
    @field_validator("action")
    @classmethod
    def check_allowed_options(cls, value: str) -> str:
        # Lowercase the value if you want case-insensitive matching
        if value.lower() not in ROLE_ACTIONS:
            raise ValueError(f"'{value}' is not a valid option.")
        return value

class RoleEmployeeMessage(SQLModel, table=False):
    org: str
    role: str
    action: str
    employees: List[str]
    @field_validator("action")
    @classmethod
    def check_allowed_options(cls, value: str) -> str:
        # Lowercase the value if you want case-insensitive matching
        if value.lower() not in ROLE_ACTIONS:
            raise ValueError(f"'{value}' is not a valid option.")
        return value