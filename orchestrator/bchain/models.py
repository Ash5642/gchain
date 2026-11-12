from typing import List
from sqlalchemy import String, Text, Integer, ForeignKey, LargeBinary, select, or_
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
from functools import reduce
import api.models as api_models
import random
from api.utils import resource_utils, entity_utils
from itertools import batched
class MerkleNode(api_models.BaseModel):
    __tablename__ = "merkle_node"
    
    value: Mapped[bytes] = mapped_column(Text, nullable=True, primary_key=True)
    type: Mapped[str] = mapped_column(String(16), nullable=True)
    proof_for: Mapped[str] = mapped_column(String(16), nullable=True)
    children: Mapped[List["MerkleNode"]] = relationship(
        "MerkleNode", 
        back_populates="parent",
        foreign_keys="MerkleNode.parent_id"
    )
    parent_id: Mapped[str] = mapped_column(String(255), ForeignKey("merkle_node.value"), primary_key=False, nullable=True)
    parent: Mapped[MerkleNode] = relationship(
        "MerkleNode", 
        back_populates="children",
        foreign_keys=[parent_id],
        remote_side=[value]
    )
    resource_id: Mapped[str] = mapped_column(String(255), ForeignKey("global_id.gid"), primary_key=False, nullable=True)
    resource: Mapped[api_models.GlobalID] = relationship(
        "GlobalID", 
        back_populates="proofs",
        foreign_keys=[resource_id],
    )
    grid: Mapped[str] = mapped_column(String(255), primary_key=False, nullable=True)
    block: Mapped[int] = mapped_column(Integer, primary_key=False, nullable=True)
    @staticmethod
    def add_proof(resource, proof):
        for hash_value in proof:
            pass

    @staticmethod
    def generate_merkle(entity, *args, **kwargs):
        stmt = select(
            api_models.GlobalID
        ).where(
            api_models.GlobalID.type.notin_(["LOCALROOT", "LROOT", "VALUE"]),
        ).where(
            api_models.GlobalID.issuer == entity
        ).where(
            api_models.GlobalID.confirmed == False
        ).where(
            api_models.GlobalID.push_type == "LOCAL"
        )
        
        prefix = b'\x01'
        resource_results = kwargs['session'].scalars(stmt).unique().all()

        lroot_name = f"{entity}[LOCALROOT]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(4))}"
        
        entity_signing_key = entity_utils.get_entity_key(entity, "SIGN", session = kwargs.get('session'))
        resource_grids = []
        tree = [[]]
        for resource in resource_results:

            kwargs.get('session').add(resource)
            if resource.hash != None:
                tree[0].append(
                    MerkleNode(
                        value = resource.hash,
                        type = "LEAF",
                        proof_for = lroot_name,
                        resource_id = resource.gid
                    )
                )
                resource_grids.append(resource.gid)
            

        max_depth = 1;
        real_count = len(tree[0])
        while 2**max_depth < len(tree[0]):
            max_depth = max_depth + 1
        for ins in range(len(tree[0]), 2**max_depth):
            tree[0].insert(
                    random.randint(
                        0,
                        len(tree[0])
                    ),
                    MerkleNode(
                        value = (''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))).encode(),
                        type = "LEAF"
                    )
                )
        curdep = 1
        while len(tree[-1]) > 1:

            curdep = curdep+1
           
            top_paired = list(batched(tree[-1], 2))

            tree.append([])
            for pair in top_paired:
                m = hashlib.sha256()
                m.update((pair[0].value + pair[1].value))
                new_hash = m.digest()
                new_node = MerkleNode(
                    value = new_hash,
                    type = "BRANCH"
                )
                tree[-1].append(new_node)
                pair[0].parent_id = new_hash
                pair[1].parent_id = new_hash

        tree[-1][0].type = "LROOT"
        root_signature = resource_utils.sign_resource(tree[-1][0].value, entity_signing_key, session = kwargs.get('session'), ret_only = True)
        print("COCOCOCOC")
        print(entity_signing_key.gid)
        new_root_record = api_models.GlobalID.create_resource(
            lroot_name,
            "LOCALROOT",
            session = kwargs['session'],
            push_type = "CHAIN",
            issuer = entity,
            values = {
                "resource_hash":tree[-1][0].value,
                "key":entity_signing_key.gid,
                "signature":root_signature
            }
        )
        new_root_record_relation = api_models.Relation(
            parent_resource_id = entity_signing_key.gid,
            child_resource_id = new_root_record.gid,
            type = "KEY",
            grid_id = f"{new_root_record.gid}[KEY]"
        )
        saved = kwargs.get('session').add(new_root_record_relation)
        #print"NEW RELLLLLLLLLLL")
        #printnew_root_record_relation)
        #printsaved)

        tree[-1][0].resource_id = new_root_record.gid
        for level in tree:
            for node in level:
                node.grid = new_root_record.gid
                kwargs['session'].add(node)
        for resource in resource_results:
            new_lroot_relation = api_models.Relation(
                parent_resource_id = new_root_record.gid,
                child_resource_id = resource.gid,
                type = "LROOT",
                grid_id = f"{resource.gid}[LROOT]={root_signature}"
            )
            resource.pushed = True
            resource.confirmed = True
            resource.push_type = "LOCAL"
            kwargs.get('session').add(resource)
            kwargs.get('session').add(new_lroot_relation)
        

        return {
            "gid":new_root_record.gid,
            "tree":[[node.value.hex() for node in nodes] for nodes in tree],
            "grids":resource_grids,
            "depth":2**max_depth,
            "count":len(tree[0]),
            "real":real_count
        }

        
class Block(api_models.BaseModel):
    __tablename__ = "block"
    
    value: Mapped[bytes] = mapped_column(Text, nullable=True, primary_key=True)
    type: Mapped[str] = mapped_column(String(16), nullable=True)
    proof_for: Mapped[str] = mapped_column(String(16), nullable=True)