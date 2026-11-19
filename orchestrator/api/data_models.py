import api.models as api_models
import bchain.models as bchain_models
from sqlmodel import Field, SQLModel, Session, Relationship, create_engine, select
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, declared_attr, joinedload, selectinload

from typing import Optional, List
from pydantic import TypeAdapter
import api.rules as rules
import hashlib
class Key(BaseModel):
    public_key: Optional[bytes]
    grid: Optional[str]

class Signature(BaseModel):
    signature: Optional[bytes]
    grid: Optional[str]

class Value(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    name: str
    type: str
    value: int|str|bool|list
    @field_validator('value', mode='before')
    @classmethod
    def verify_password_match(cls, v: str, info: FieldValidationInfo):
        # info.data is a dictionary containing previously validated fields
        print(info.data)
        print("DIX")
        if info.data['type'] == 'str':
            return v.decode()
        if info.data['type'] in ['INT', 'int']:
            cock()
            return v.decode()
        if info.data['type'] == 'bool':
            return True if int.from_bytes(v) == 1 else False
        if info.data['type'] == 'bytes':
            return v.hex()
        if info.data['type'] == "ARR:gid":
            return "COCK"
        
        return v

class Proof(BaseModel):
    path: Optional[list] = []
    type: Optional[str] = ""
    proof_for: Optional[str|int] = ""
    grid: Optional[str] = None
    block: Optional[int] = None


    
class Resource(BaseModel):
    gid: Optional[str] = None
    type: Optional[str] = None
    children: Optional[List[Resource]] =  Field(default=[], exclude_if=lambda v: v == [])
    values : Optional[dict] = {}
    binary: Optional[str] = Field(default=None, exclude_if=lambda v: v == None)
    hash: Optional[str] = Field(default=None, exclude_if=lambda v: v == None)
    proof: Optional[List[Proof]] =  Field(default=[], exclude_if=lambda v: v == [])
    proof_resource: Optional[List[Resource]] =  Field(default=[], exclude_if=lambda v: v == [])
    roots: Optional[List[Resource]] =  Field(default=[], exclude_if=lambda v: v == [])
    depth: Optional[int] = 5
    children : Optional[list] = []
    push_type : Optional[str] = []
    pushed: Optional[bool] = []
    confirmed: Optional[bool] = []
    revoked: Optional[bool] = []
    revoke_id: Optional[bool] = []
    def __init__(self, data, *args, **kwargs):
        #printdata.gid)
        super().__init__()
        self.gid = data.gid
        self.type = data.type
        binary = data.get_protobuf(True, session = kwargs['session']).SerializeToString()
        self.binary = binary.hex()
        m = hashlib.sha256()
        m.update(binary)
        new_hash = m.digest()
        self.hash = data.hash.hex()
        self.proof = []
        self.pushed = data.pushed
        self.confirmed = data.confirmed
        self.push_type = data.push_type
        self.revoked = data.revoked
        self.revoke_id = data.revoke_id
        self.depth = kwargs.get('depth', 5)
        if kwargs.get('depth') == 0:
            return 
        value_list_adapter = TypeAdapter(List[Value])
        for value in data.values:
            decoded_value = None
            self.values[value.name] = value.get_value()
        for proof_resource in data.proof_resources:
            self.proof_resource.append(
                Resource(
                    proof_resource,
                    session = kwargs.get('session')
                )
            )
        self.children = [child.child_resource_id for  child in data.children]
        for proof in data.merkle_proofs:
            path = []
            for node_hash in proof['path']:
                path.append(node_hash.hex())
            self.proof.append(
                Proof(
                    path = path ,
                    type = proof['type'],
                    block = proof['block'],
                    grid = proof['grid'],
                    proof_for = proof['block']
                )
            )
        

    

   
    def get_children(self, *args, **kwargs):
        children_list = []
        if rules.resource.get(self.type):
            if rules.resource.get(self.type)['recurse'] == False:
                return children_list
        stmt = select(
            api_models.GlobalID, api_models.Relation, api_models.Value
        ).options(
            joinedload(api_models.GlobalID.values)
        ).join(
            api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.child_resource_id
        ).where(
            api_models.Relation.parent_resource_id == self.gid
        )
        child_results = kwargs['session'].scalars(stmt).unique().all()
        
        parents = kwargs.get('parents', [])
        parents.append(self.gid)
        for child_result in child_results:

            if child_result.gid in parents:
                #print"SAME")
                continue
            parents.append(child_result.gid)
            child = Resource(child_result, session = kwargs['session'])
            children_list.append(child)
            children_list = children_list + child.get_children(session = kwargs['session'], parents = parents) if kwargs.get('depth')!=0 else []
            #print"CCCCCCCCCCC")
            
        return children_list