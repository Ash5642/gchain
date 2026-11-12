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
from pydantic import BaseModel, ConfigDict
import api.serializers as api_serializers
from typing import List, Optional
import orgs.serializers as org_serializers

class DocumentFamilyMessage(org_serializers.OrgMessage, table=False):
    name: str

class DocumentGrantMessage(org_serializers.OrgMessage, table=False):
    name: str
    geid: str
