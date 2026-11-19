from typing import Optional
from fastapi import FastAPI, Depends
from sqlmodel import Field, SQLModel, Session, Relationship, create_engine, select
import api.models as api_models
from cryptography.hazmat.primitives.asymmetric import rsa
from Crypto.Cipher import AES
from cryptography.hazmat.primitives import serialization
from Crypto.Cipher import AES
import base64, json, secrets, hashlib
import string, secrets
from typing import Optional
from sqlalchemy import String, Text, ForeignKey, LargeBinary, select
from sqlalchemy.orm import Mapped, mapped_column, declared_attr, relationship
import data_pb2
from google.protobuf.any_pb2 import Any
from api.utils import resource_utils

   
class Organization(api_models.GlobalEntity):
    __tablename__ = "organization"
    __abstract__ = True
    
