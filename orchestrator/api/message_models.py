from typing import Optional
from fastapi import FastAPI, Depends
from sqlmodel import Field, SQLModel, Session, Relationship, create_engine, select
from sqlmodel import Session


class KeyReference(SQLModel):
    grid:str

class Signature(SQLModel):
    signature:bytes
    key:KeyReference

class APIMessage(SQLModel):
    type: str
    value: dict
    delay: Optional[int] = 0
    signatures: list[Signature]

class APIRequest(SQLModel):
    loop: Optional[int] = 1
    messages : list[APIMessage]
    auth: Optional[dict]
