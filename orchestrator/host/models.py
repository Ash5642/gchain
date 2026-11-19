from typing import Optional
from fastapi import FastAPI, Depends
from sqlmodel import Field, SQLModel, Session, Relationship, create_engine, select
import api.models as api_models
from cryptography.hazmat.primitives.asymmetric import rsa
from Crypto.Cipher import AES
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import utils
from Crypto.Cipher import AES
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
import base64, json, secrets, hashlib



    

   
class Host(api_models.GlobalEntity):
    __tablename__ = "organization"
    __abstract__ = True

    
   