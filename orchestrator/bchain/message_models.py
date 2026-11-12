from typing import Optional
from fastapi import FastAPI, Depends
from sqlmodel import Field, SQLModel, Session, Relationship, create_engine, select
from sqlmodel import Session


class Push(SQLModel):
    merkle:Optional[list] = None
    grid: Optional[str] = None

class Revoke(SQLModel):
    grid: Optional[str] = None

