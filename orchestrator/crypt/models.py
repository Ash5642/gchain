from typing import Optional
from fastapi import FastAPI, Depends
from sqlmodel import Field, SQLModel, Session, Relationship, create_engine, select
from api.models import GlobalResource
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
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.mldsa import MLDSA65PrivateKey
from cryptography.hazmat.primitives.asymmetric import mlkem
from cryptography.hazmat.primitives.asymmetric.mlkem import MLKEM768PrivateKey
import api.utils.entity_utils as entity_utils
class Key(GlobalResource):
    __tablename__ = "key"
    __abstract__ = True
    
    # Public/Private keys mapped safely to LargeBinary blobs
    public_key: Mapped[bytes] = mapped_column(Text, nullable=True)
    private_key: Mapped[bytes] = mapped_column(Text, nullable=True)
    type: Mapped[str] = mapped_column(String(16))


    @staticmethod
    def generate_key(*args, **kwargs):
        key_type = ""
        if kwargs.get('type') == 'SIGN':
            private_key = MLDSA65PrivateKey.generate()
            public_key = private_key.public_key()

            private_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            public_bytes = public_key.public_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )

            context = b"secure-transaction-v1"
            message = b"Data payload to be signed"
            signature = private_key.sign(message, context)

            public_key.verify(signature, message, context)
            #print"Signature successfully verified using quantum-safe ML-DSA!")
            key_type = "ML-DSA"

        else:
            private_key = MLKEM768PrivateKey.generate()
            public_key = private_key.public_key()

            private_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            public_bytes = public_key.public_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
            key_type = "ML-KEM"





        nonce = (''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))).encode()
        signature = b""
        generated_slug = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
        import api.models as api_models
        new_gid = api_models.GlobalID.create_resource(
            f"{kwargs.get('geid')}[KEY][{kwargs.get('type')}]={generated_slug}",
            "KEY",
            session = kwargs['session'],
            parent = kwargs['geid'],
            push_type = kwargs.get('push_type', None),
            issuer = kwargs.get('issuer', None),
            values = {
                "public_key":public_bytes,
                "private_key":private_bytes,
                "nonce":nonce,
                "signature":signature,
                "type":key_type
            }
        )
        if '::' in kwargs['geid']:
            anchor_grid = kwargs['geid'].split('::')[0]
            #printanchor_grid)
            anchor_key = entity_utils.get_entity_key(anchor_grid, "SIGN", session = kwargs.get('session'))
            resource_utils.sign_resource(new_gid, anchor_key, session = kwargs.get('session'))
        kwargs['session'].add(new_gid)
        return new_gid

    @staticmethod
    def generate_key_2(*args, **kwargs):
        rsa_keys = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        public_key = rsa_keys.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        private_key = rsa_keys.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        private_key_loaded = serialization.load_der_private_key(
            private_key,
            password=None
        )
        nonce = (''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))).encode()
        signature = private_key_loaded.sign(
            nonce,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        
        generated_slug = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
        import api.models as api_models
        new_gid = api_models.GlobalID.create_resource(
            f"{kwargs.get('geid')}[KEY][{kwargs.get('type')}]={generated_slug}",
            "KEY",
            session = kwargs['session'],
            parent = kwargs['geid'],
            values = {
                "public_key":public_key,
                "private_key":private_key,
                "nonce":nonce,
                "signature":signature
            }
        )

        #print"COCKS")
        #printnew_gid)
        #printnew_gid.gid)


        new_gid.get_hash(session = kwargs.get('session'))
        #printsignature)
        kwargs['session'].add(new_gid)
        

        return new_gid

class Signature(GlobalResource):
    __tablename__ = "signature"
    __abstract__ = True
    
    # Decoupled from GlobalResource to accommodate your clean One-to-Many rule cleanly
    grid_id: Mapped[str] = mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=True)
    signature: Mapped[bytes] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=True)
    grid: Mapped[GlobalID] = relationship(
        "GlobalID", 
        back_populates="signature",
        foreign_keys=[grid_id]
    )
    

    key_id: Mapped[str] = mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=False)
    key: Mapped[GlobalID] = relationship(
        "GlobalID", 
        back_populates="signs",
        foreign_keys=[key_id]
    )

    resource_id: Mapped[str] = mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=True)
    resource: Mapped[GlobalID] = relationship(
        "GlobalID", 
        back_populates="signatures",
        foreign_keys=[resource_id]
    )
