import api.models as api_models
import crypt.models as crypt_models
from sqlmodel import Field, SQLModel, select
from database import get_session
import api.message_models as message_models
import anchors.models as anchor_models
import api.utils.resource_utils as resource_utils
from api.generic_handlers import *
import anchors.serializers as anchor_serializers
import api.serializers as api_serializers
import crypt.serializers as crypt_serializers
from api.utils import entity_utils
import api.rules as rules
import api.exceptions as api_exceptions
from sqlalchemy import select
from pydantic import TypeAdapter
from typing import List
import api.data_models as data_models
import bchain.models as bchain_models
import secrets
import string
import random
from bchain.utils import core_messages

class GIdFetch(MessageHandler):
    value_serializer = api_serializers.GIdMessage
    def handle(self):

        gid_serialized = []
        
        if self.value.action:
            match self.value.action['type']:
                case 'sign':
                    key_match = resource_utils.get_by_gid(self.value.action['signing_key'], session = self.session, model_only = True)
                    key_match.load_values()
                    resource_utils.sign_resource(gid_match, key_match, signature = self.value.action['signature'], session = self.session)

                case 'proof':
                    key_match = resource_utils.get_by_gid(self.value.action['signing_key'], session = self.session, model_only = True)
                    key_match.load_values()
                    resource_utils.sign_resource(gid_match, key_match, signature = self.value.action['signature'], session = self.session)


                case 'tree':
                    tree_result = bchain_models.MerkleNode.generate_merkle(self.value.gid, session = self.session)
                    return tree_result

                

                case 'push':
                    tree_result = gid_match.push(session = self.session)
                    #print"ENKKKODED")
                    #printgid_match.get_protobuf(False, session = self.session).SerializeToString().hex())
                    #print"ENKKKODED")
                    return tree_result


        return [serialized.model_dump() for serialized in gid_serialized]
        
class GIDAssemble(MessageHandler):
    value_serializer = api_serializers.GIdMessage
    def handle(self):
        if self.value.action:
            match self.value.action:
                case "RESET":
                    stmt = select(
                        api_models.GlobalID, api_models.Value
                    ).where(
                        api_models.GlobalID.gid == self.value.gid
                    )
                    root_resource = self.session.scalars(stmt).first()
                    root_resource.pushed = False
                    root_resource.push_type = None
                    self.session.add(root_resource)
                    return {
                        "status":"SUCCESS"
                    }
        if self.value.include != None:
            resource = resource_utils.get_packaged(self.value.gid, self.value.include, session = self.session)
        else:
            resource = resource_utils.get_packaged(self.value.gid, ["RELATED", "PROOF"], session = self.session)
        gid_serialized = []
        for resource in resource:
            gid_serialized.append(data_models.Resource(resource, session = self.session))
            #print"HUBBBBBBBB")
            #printresource.proof_resources)

        return [serialized.model_dump() for serialized in gid_serialized]

class EntityCreate(MessageHandler):
    value_serializer = api_serializers.EntityCreateMessage
    rule_set = None
    def handle(self):
        if self.value.entity.type in rules.entity:
            self.rule_set = rules.entity[self.value.entity.type]
            if self.value.entity.parent == None and None in self.rule_set['parents']:
                pass
            else:
                parent_match = resource_utils.get_by_gid(self.value.entity.parent, session = self.session, model_only = True)
                if parent_match.type in self.rule_set['parents']:
                    pass
                else:
                    raise api_exceptions.InvalidParent(None, self.rule_set['parents'])
        new_entity = entity_utils.create_entity(self.value.entity.name, type=self.value.entity.type, parent=self.value.entity.parent, session = self.session)
    
        return {
            "geid":new_entity.gid       
        }

class SignatureManagement(MessageHandler):
    value_serializer = api_serializers.SignatureManagementMessage
    rule_set = None
    def handle(self):
        stmt = select(api_models.GlobalID).where(api_models.GlobalID.gid == self.value.grid)
        grid_result = self.session.scalars(stmt).first()
        if grid_result == None:
            raise api_exceptions.NotFound("grid", self.value.grid)

        

        self.auth['key']
    
        stmt = select(crypt_models.Signature).where(crypt_models.Signature.resource_id == self.value.grid)
        signature_result = self.session.scalars(stmt).all()
        signature_list_adapter = TypeAdapter(List[api_serializers.Signature])
        serialized_signature_list = signature_list_adapter.validate_python(signature_result)
        

        return {
            "grid":new_signature.grid_id,
            "signatures":serialized_signature_list
        }

class Pollute(MessageHandler):
    value_serializer = api_serializers.Pollute
    rule_set = None
    def handle(self):
        created_anchors = []
        stmt = select(api_models.GlobalID).where(api_models.GlobalID.confirmed == True).where(api_models.GlobalID.type == "RECORD")
        confirmed = self.session.scalars(stmt).all()


        #printf"Starting with {len(confirmed)}")
        geid_list = []
        for i in range(0, self.value.count):

            entity_name  = (''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(8)))
            #printentity_name)
            entity_type = random.choice(["ANCHOR"])
            has_anchor = random.choice([True, False])
            if has_anchor and confirmed != []:
                anchor = random.choice(confirmed)
                new_entity = entity_utils.create_entity(f"{entity_name}", parent = anchor.gid.split('[')[0], type=entity_type, session = self.session)
            else:
                new_entity = entity_utils.create_entity(f"{entity_name}", type=entity_type, session = self.session)
            geid_list.append(new_entity.gid)

        return {
            "grid":geid_list
        }


class Revoke(MessageHandler):
    value_serializer = api_serializers.Revoke
    rule_set = None
    def handle(self):
        combined_hash = ";".join(self.value.grid)
        grid_signatures = resource_utils.sign_resource(combined_hash.encode(), self.auth['keys']['sign'], session = self.session, ret_only = True)
        new_signature_grid = api_models.GlobalID.create_resource(
            f"{self.auth['entity'].gid}[REVOKE]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))}",
            "REVOKE",
            session = self.session,
            push_type = "CHAIN",
            values = {
                "grid":self.value.grid,
                "key":self.auth['keys']['sign'].gid,
                "signature":grid_signatures
            }
        )
        print(new_signature_grid.gid)
            

 

class Cash(MessageHandler):
    value_serializer = api_serializers.Cash
    rule_set = None
    def handle(self):
        new_signature_grid = api_models.GlobalID.create_resource(
            f"{self.auth['entity'].gid}[CASH]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))}",
            "CASH",
            session = self.session,
            push_type = "CHAIN",
            values = {
                "rx":self.value.rx,
                "amt":self.value.amt
            }
        )
        print(new_signature_grid.gid)
        return {
            "grid":new_signature_grid.gid
        }
            

class Executable(MessageHandler):
    value_serializer = api_serializers.Executable
    rule_set = None
    def handle(self):
        new_signature_grid = api_models.GlobalID.create_resource(
            f"{self.auth['entity'].gid}[EXECUTABLE]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))}",
            "EXECUTABLE",
            session = self.session,
            push_type = "CHAIN",
            values = {
                "bytecode":bytes.fromhex(self.value.bytecode)
            }
        )
        print(new_signature_grid.gid)
        return {
            "grid":new_signature_grid.gid
        }
            
class Call(MessageHandler):
    value_serializer = api_serializers.Call
    rule_set = None
    def handle(self):
        new_signature_grid = api_models.GlobalID.create_resource(
            f"{self.auth['entity'].gid}[CALL]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))}",
            "CALL",
            session = self.session,
            push_type = "CHAIN",
            values = {
                "executable":self.value.executable,
                "caller":self.auth['entity'].gid
            }
        )
        print(new_signature_grid.gid)
        return {
            "grid":new_signature_grid.gid
        }

class Delete(MessageHandler):
    value_serializer = api_serializers.GIdMessage
    rule_set = None
    def handle(self):
        
        stmt = select(
            api_models.GlobalID, api_models.Value
        ).where(
            api_models.GlobalID.gid == self.value.gid
        )
        root_resource = self.session.scalars(stmt).first()
        root_resource.pushed = True
        root_resource.confirmed = True
        self.session.add(root_resource)
        return {
            "grid":self.value.gid
        }
            