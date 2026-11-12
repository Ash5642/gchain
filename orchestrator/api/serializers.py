from typing import Optional, List, Any
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
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

   
class GlobalEntity(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    gid: str

class GlobalResource(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    gid: str

class Value(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    name: str
    type: str
    value: int|str|bool
    @field_validator('value', mode='before')
    @classmethod
    def verify_password_match(cls, v: str, info: FieldValidationInfo):
        # info.data is a dictionary containing previously validated fields
        if info.data['type'] == 'str':
            return v.decode()
        if info.data['type'] == 'bool':
            return True if int.from_bytes(v) == 1 else False
        if info.data['type'] == 'bytes':
            return v.hex()
        return v


class DocumentFamilyGrant(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    subject_id: str


class Signature(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    signature: Optional[bytes] = None
    grid_id: Optional[str] = None
    key_id: Optional[str] = None
    resource_id:Optional[str] = None
    status: Optional[str] = None
    @field_validator('signature', mode='before')
    @classmethod
    def decode_safe_string(cls, v: any) -> str:
        if isinstance(v, bytes):
            # If the DB returned raw bytes, decode safely using latin-1 or ignore errors
            return v.hex()
        return None

class GlobalID(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    gid: str
    type: str
    values: Optional[List[Value]] = None
    children : Optional[List[GlobalID]] = None
    @model_validator(mode="before")
    @classmethod
    def inspect_raw_input(cls, data: Any) -> Any:
        # 'data' is the raw input object (usually a dict)
        return data


class Entity(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    geid: Optional[GlobalID] = None

class Resource(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    grid_id: str

class Key(Resource):
    model_config = ConfigDict(from_attributes=True)
    
class Record(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    gid: Optional[str]



class Anchor(Entity):
    model_config = ConfigDict(from_attributes=True)
    geid: Optional[GlobalID] = None


class FullGlobalId(BaseModel):
    gid: Optional[str] = None
    keys: Optional[List[GlobalID]] = None
    children: Optional[List[GlobalID]] = None
    data: Optional[Any]


class GIdMessage(SQLModel, table=False):
    gid: str|dict
    action: dict|str = None
    include: list = None
    
class Pollute(SQLModel, table=False):
    count: int

    
class Revoke(SQLModel, table=False):
    grid: List[str] = None

class Cash(SQLModel, table=False):
    amt: int = 0
    rx: str = ""

class Executable(SQLModel, table=False):
    bytecode: bytes = b""

class Call(SQLModel, table=False):
    executable: str = ""

class DocumentIssue(SQLModel, table=False):
    entity: str
    data: dict
    grant: str = None

class DocumentFamily(SQLModel, table=False):
    entity: str
    name: str
    template: str

class DocumentGrantPermissions(SQLModel, table=False):

    issue: bool
    grant: bool
    

class DocumentGrant(SQLModel, table=False):
    grid: str
    entity: str
    permissions: DocumentGrantPermissions
    

class ChainMessage(SQLModel, table=False):
    action: str = None
    message: Optional[str|dict] = None
    
   

class EntityCreate(SQLModel, table=False):
    name: str
    parent: Optional[str] = None
    type: Optional[str] = None

class EntityCreateMessage(SQLModel, table=False):
    entity: EntityCreate



class SignatureMessage(SQLModel, table=False):
    key: str
    signature: Optional[str] = None

class SignatureManagementMessage(SQLModel, table=False):
    grid: str
    signatures: List[SignatureMessage]
