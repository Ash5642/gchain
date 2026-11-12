from typing import List
from sqlalchemy import String, Text, ForeignKey, LargeBinary, select, Boolean, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, declared_attr, joinedload
from pydantic import TypeAdapter
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
import api.exceptions as api_exceptions
import secrets, string
from cryptography.exceptions import InvalidSignature
import data_pb2
import hashlib
import requests
from functools import reduce
from scapy.all import IP, TCP, send
import scapy.all as scapy
import socket
from sqlalchemy.exc import IntegrityError
# Scapy automatically calculates lengths and checksums


class BaseModel(DeclarativeBase): 
    pass

class GlobalIdBase(BaseModel):
    __abstract__ = True
    # Defined cleanly without duplicate mapping declarations
    gid: Mapped[str] = mapped_column(String(255), primary_key=True)
    type: Mapped[str] = mapped_column(String(16))

class GlobalID(BaseModel):
    __tablename__ = "global_id"
    __allow_unmapped__ = True
    gid: Mapped[str] = mapped_column(String(255), primary_key=True)
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    
    # Cryptographic elements should use LargeBinary instead of Text to block UnicodeDecodeErrors
    hash: Mapped[bytes] = mapped_column(Text, nullable=True)
    nonce: Mapped[bytes] = mapped_column(Text, nullable=True)
    binary: Mapped[bytes] = mapped_column(Text, nullable=True)
    pushed: Mapped[bool] = mapped_column(Boolean, nullable=True, default=False)
    confirmed: Mapped[bool] = mapped_column(Boolean, nullable=True, default=False)
    push_type: Mapped[str] = mapped_column(Text, nullable=True, default=None)
    push_id: Mapped[str] = mapped_column(Text, nullable=True, default=None)
    revoked: Mapped[bool] = mapped_column(Boolean, nullable=True, default=False)
    revoke_id: Mapped[str] = mapped_column(Text, nullable=True, default=None)
    issuer: Mapped[str] = mapped_column(Text, nullable=True, default=None)
    loaded: Mapped[bool] = mapped_column(Boolean, nullable=True, default=False)
    values: Mapped[List["Value"]] = relationship(
        "Value", 
        back_populates="resource",
        foreign_keys="Value.resource_id"
    )
    value: Mapped[Optional[EntityRecord]] = relationship(
        "Value", 
        back_populates="grid", 
        uselist=False,
        foreign_keys="Value.grid_id"
    )
    


    proofs: Mapped[List["MerkleNode"]] = relationship(
        "MerkleNode", 
        back_populates="resource",
        foreign_keys="MerkleNode.resource_id"
    )
    
    children: Mapped[List["Relation"]] = relationship(
        "Relation", 
        back_populates="parent_resource",
        foreign_keys="Relation.parent_resource_id"
    )

    parents: Mapped[List["Relation"]] = relationship(
        "Relation", 
        back_populates="child_resource",
        foreign_keys="Relation.child_resource_id"
    )
    relation: Mapped[Optional[Relation]] = relationship(
        "Relation", 
        back_populates="grid", 
        uselist=False,
        foreign_keys="Relation.grid_id"
    )
    merkle_proofs: list = None
    proof_resources: list = []
    

    loaded_values: dict = {}

    @property
    def merkle_proofs(self):
        # If the private attribute doesn't exist yet, create a fresh list
        if not hasattr(self, "_merkle_proofs"):
            self._merkle_proofs = []
        return self._merkle_proofs
        

    def get_or_create(session, *args, **kwargs):
        with session.begin_nested():
            try:
                instance = GlobalID(**kwargs)
                session.add(instance)
                session.flush()
                print("Added!")
                return instance, True
                
            except IntegrityError:
                print("Exists!")
                pass
        return kwargs.get('gid')

    def load_values(self):
        for value in self.values:
            self.loaded_values[value.name] = value.value

        return

    def push(self, *args, **kwargs):

        target_host = "127.0.0.1"
        target_port = 8001
        raw_payload = self.get_protobuf(False, session = kwargs['session']).SerializeToString()
        #printraw_payload.hex())
        enveloped_payload = data_pb2.ChainMessage(
            type = "RESOURCE",
            message = raw_payload
        )
        self.pushed = True
        kwargs['session'].add(self)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(("127.0.0.1", 8001))
        client_socket.sendall(enveloped_payload.SerializeToString().hex().encode())



    def sign_resource(self, key, *args, **kwargs):

        private_key = serialization.load_der_private_key(
            key.loaded_values['private_key'],
            password=None
        )

        signature = private_key.sign(
            self.binary,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        status = "complete"


        

        from crypt.models import Signature
        new_signature_grid = GlobalID.create_resource(
            f"{key.gid}[SIGNATURE]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(4))}",
            "SIGNATURE",
            session = kwargs.get('session'),
            parent = self.gid,
            values = {
                "signature":signature,
                "resource":self.gid,
                "status":"complete",
                "key":key.gid
            }
        )
        #print"SISISISISISISISS")
        #printnew_signature_grid)
        
        return new_signature_grid

    def check_signature(self, key, signature, *args, **kwargs):
        #printkey.loaded_values)
        if isinstance(signature, str):
            signature = bytes.fromhex(signature)
        public_key = serialization.load_der_public_key(
            key.loaded_values['public_key']
        )
        try:
            public_key.verify(
                signature,
                self.binary,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except InvalidSignature:
            return False

    @staticmethod
    def create_resource(gid, resource_type, *args, **kwargs):
        print(f"hjghughufhf {type(kwargs.get('issuer'))}")
        new_gid = GlobalID(
            gid = gid,
            type = resource_type,
            nonce = kwargs.get('nonce', (''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))).encode()),
            push_type = kwargs.get('push_type'),
            issuer = kwargs.get('issuer', None),
            loaded = kwargs.get('loaded', True)
        )
        if kwargs.get('issuer') != None:
            print(f"Issued by {kwargs.get('issuer')}")
        kwargs['session'].add(new_gid)
        if 'parent' in kwargs:
            new_relation = Relation(
                grid_id = f"{gid}:{kwargs['parent']}{resource_type}",
                parent_resource_id = kwargs['parent'],
                child_resource_id = gid,
                type = resource_type
            )
            kwargs['session'].add(new_relation)
        import api.rules as rules
        rule_set = rules.resource.get(resource_type)
        values = []
        if rule_set != None:
            if 'data_template' in rule_set and kwargs.get('values') != False:
                for key in rule_set['data_template'].keys():
                    value_type = rule_set['data_template'][key][0]
                    value = rule_set['data_template'][key][1]
                    value_encoded = b""
                    if kwargs.get('values') != None:
                        if key in kwargs.get('values'):
                            value = kwargs.get('values')[key]
                    
                    match value_type:
                        case "str"|"gid"|"gid:key":
                            value_encoded = value.encode()
                        case "ARR:gid":
                            value_str = ";".join(value)
                            value_encoded = value_str.encode()
                        case 'bool':
                            value_encoded = int(value).to_bytes(1, byteorder='big')
                        case 'bytes':
                            value_encoded = value
                        case 'int':
                            value_encoded = value.to_bytes(4, byteorder='little')
                    new_key_grid = GlobalID.create_resource(
                        f"{gid}[VALUE][{key}]",
                        "VALUE",
                        session = kwargs.get('session'),
                        push_type = None
                    )
                    new_key_value = Value(
                        name = key,
                        grid_id = f"{gid}[VALUE][{key}]",
                        type = value_type,
                        value = value_encoded,
                        resource_id = gid
                    )            
                    new_key_value.grid = new_key_grid
                    values.append(new_key_value)
                    kwargs['session'].add(new_key_value)
        if kwargs.get('push_type') != None:
            print(f"Created {kwargs.get('push_type')} {gid}")
        new_gid.values = values
        new_gid.load_values()
        new_gid.get_hash(session = kwargs.get('session'))
        return new_gid

        

    def get_protobuf(self, envelope, *args, **kwargs):

        
        payload_protobuf = data_pb2.Payload(
            grid = self.gid,
            nonce = self.nonce,
            type = self.type,
        )
        if kwargs.get('skip_dhash') != True:
            values_hash = b'\x00' * 32
            values_hash_list = []
            for value in self.values:

                value_protobuf = data_pb2.Field(
                    key = value.name,
                    type = value.type,
                    value = value.value,
                    nonce = value.grid.nonce
                )
                m = hashlib.sha256()
                m.update(value_protobuf.SerializeToString())
                payload_protobuf.fields.append(value_protobuf)
                values_hash_list.append(m.digest())
                
            if values_hash_list != []:
                values_hash = reduce(lambda x, y: bytes(b1 ^ b2 for b1, b2 in zip(x, y)), values_hash_list)
            
            hash_payload = data_pb2.HashPayload(
                grid = self.gid,
                nonce = self.nonce,
                type = self.type,
                fields = values_hash,
                action = ""
            )
            m = hashlib.sha256()
            m.update(hash_payload.SerializeToString())
            resource_hash = m.digest()
            self.hash = resource_hash
            print(f"Hash: {self.hash.hex()}")

        resource_payload = data_pb2.Resource(
            payload = payload_protobuf
        )
        for proof in self.proof_resources:
            resource_payload.signatures.append(
                proof.get_protobuf(False, session = kwargs.get('session'))
            )
        if envelope:
            return payload_protobuf

        for proof in self.merkle_proofs:
            new_merkle_proof_protobuf = data_pb2.MerkleProof(
                type = proof['type'],
                hashes = proof['path'],
                block = proof['block'],
                grid = proof['grid']
            )
            resource_payload.proof.append(new_merkle_proof_protobuf)

        return resource_payload


    def get_hash(self, *args, **kwargs):

        payload_protobuf = self.get_protobuf(True,  session = kwargs['session'])
        kwargs.get('session').add(self)
        return self.hash




# 3. Abstract GlobalResource Model
class GlobalResource(BaseModel):
    __abstract__ = True
    
    @declared_attr
    def grid_id(cls) -> Mapped[str]:
        return mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=True)
    
    @declared_attr
    def grid(cls) -> Mapped[GlobalID]:
        # Keeps safe back-population reference paths intact dynamically
        return relationship(
            GlobalID, 
            back_populates=cls.__tablename__, 
            uselist=False
        )


    def get_protobuf(self):
        return


class GlobalEntity(BaseModel):
    __abstract__ = True
    
    
    
    @declared_attr
    def geid_id(cls) -> Mapped[str]:
        return mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=True)
    
    @declared_attr
    def geid(cls) -> Mapped[GlobalID]:
        return relationship(
            GlobalID, 
            back_populates=cls.__tablename__, 
            uselist=False
        )

    def create_entity(self, *args, **kwargs):
        split_name = self.geid_id.split(':')
        if len(split_name)>1:
            parent_name = self.geid_id.rsplit(':', maxsplit = 1)
            #printparent_name)
            stmt = select(GlobalID).where(GlobalID.gid == parent_name[0])
            parent_result = kwargs.get('session').scalars(stmt).first()
            #print"PR")
            #printparent_result)
            if parent_result == None:
                raise api_exceptions.NotFound("ENTITY", parent_name[0])


class Relation(GlobalResource):
    __abstract__ = False
    __tablename__ = "relation"
    parent_resource_id: Mapped[str] = mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=False, nullable=True)
    parent_resource: Mapped[GlobalID] = relationship(
        "GlobalID", 
        back_populates="children",
        foreign_keys=[parent_resource_id],
    )

    child_resource_id: Mapped[str] = mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=False, nullable=True)
    child_resource: Mapped[GlobalID] = relationship(
        "GlobalID", 
        back_populates="parents",
        foreign_keys=[child_resource_id],
    )
    type: Mapped[str] = mapped_column(String(16))
    grid_id: Mapped[str] = mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=True)
    grid: Mapped[GlobalID] = relationship(
        "GlobalID", 
        back_populates="relation",
        foreign_keys=[grid_id]
    )

    

class Value(GlobalResource):
    __abstract__ = False
    __tablename__ = "data"

    name: Mapped[str] = mapped_column(String(16))
    type: Mapped[str] = mapped_column(String(16))
    value: Mapped[bytes] = mapped_column(Text, nullable=True)
    resource_id: Mapped[str] = mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=False)
    resource: Mapped[GlobalID] = relationship(
        "GlobalID", 
        back_populates="values",
        foreign_keys=[resource_id]
    )
    grid_id: Mapped[str] = mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=True)
    grid: Mapped[GlobalID] = relationship(
        "GlobalID", 
        back_populates="value",
        foreign_keys=[grid_id]
    )

    
    def get_protobuf_body(self):
        record_protobuf = data_pb2.Record(
            grid = self.grid_id
        )
        return record_protobuf


    def get_value(self):
        match self.type:
            case "str"|"gid"|"gid:key":
                return self.value.decode()
            case "bytes":
                return self.value.hex()
            case "bool":
                return True if int.from_bytes(self.value) == 1 else False
            case "ARR:gid":
                return self.value.decode().split(";")
            case "value"|"int":
                return int.from_bytes(self.value, byteorder='little')
            

        return self.type
