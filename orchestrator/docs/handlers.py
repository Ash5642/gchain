import api.models as api_models
import crypt.models as crypt_models
from sqlmodel import Field, SQLModel, select
from database import get_session
import api.message_models as message_models
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, declared_attr, joinedload, selectinload

import anchors.models as anchor_models
import orgs.models as org_models
import user.models as user_models
import docs.models as doc_models

from sqlalchemy import select
from api.generic_handlers import *
import orgs.serializers as org_serializers
import api.serializers as api_serializers
import user.serializers as user_serializers
import docs.serializers as doc_serializers
from pydantic import TypeAdapter
from typing import List
import api.exceptions as exceptions
import orgs.handlers as org_handlers

import crypt.serializers as crypt_serializers
import api.utils.entity_utils as entity_utils
import api.utils.resource_utils as resource_utils

import secrets, string

class DocumentFamilyCreate(org_handlers.OrganizationHandler):
    value_serializer = doc_serializers.DocumentFamilyMessage
    def handle(self):
        new_gid = api_models.GlobalID(
            gid = f"{self.org.gid}[DOCUMENT FAMILY][{self.value.name}][RECORD]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(4))}",
            type = "DOCUMENT_FAMILY"
        )
        self.session.add(new_gid)
        return {
            "gid":new_gid.gid
        }

class DocumentFamilyView(org_handlers.OrganizationHandler):
    value_serializer = doc_serializers.DocumentFamilyMessage
    def handle(self):
        document_family_name = f"{self.value.org}[DOCUMENT FAMILY][{self.value.name}][RECORD]"
        stmt = select(api_models.GlobalID).where(api_models.GlobalID.gid.startswith(document_family_name))
        document_family = self.session.scalars(stmt).first()

        document_family_grant_prefix = f"{self.value.org}[DOCUMENT FAMILY][{self.value.name}][GRANT]"
        stmt = select(api_models.GlobalID).where(api_models.GlobalID.gid.startswith(document_family_grant_prefix))
        document_grants = self.session.scalars(stmt).all()

        grants_list_adapter = TypeAdapter(List[api_serializers.GlobalID])
        serialized_grants_list = grants_list_adapter.validate_python(document_grants)
        return {
            "gid":serialized_grants_list,
            "name":document_family_grant_prefix
        }

class DocumentGrantCreate(org_handlers.OrganizationHandler):
    value_serializer = doc_serializers.DocumentGrantMessage
    def handle(self):
        document_family_name = f"{self.value.org}[DOCUMENT FAMILY][{self.value.name}][RECORD]"
        stmt = select(api_models.GlobalID).where(api_models.GlobalID.gid.startswith(document_family_name))
        document_family = self.session.scalars(stmt).first()
        if document_family == None:
            return {
                "status":False,
                "name":document_family_name
            }
        
        entity = resource_utils.get_by_gid(self.value.geid, session = self.session, model_only = True)


        new_grant_gid = api_models.GlobalID.create_resource(
            f"{self.org.gid}[DOCUMENT FAMILY][{self.value.name}][GRANT]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(4))}",
            "DOCUMENT-FAMILY:GRANT",
            session = self.session,
            push_type = "LOCAL"
        )
        new_record_gid = doc_models.DocumentGrant(
            grid_id = new_grant_gid.gid,
            subject_id = entity.gid
        )
       
        self.session.add(new_grant_gid)
        self.session.add(new_record_gid)
        return {
            "status":True,
            "gid":new_grant_gid.gid
        }

class DocumentFamily(MessageHandler):
    value_serializer = api_serializers.DocumentFamily
    rule_set = None
    def handle(self):
        created_anchors = []
        new_grant_gid = api_models.GlobalID.create_resource(
            f"{self.auth['entity'].gid}[FAMILY]={self.value.name}",
            "FAMILY",
            session = self.session,
            issuer = self.auth['entity'].gid,
            push_type = "LOCAL",
            values = {
                "data":self.value.name,
                "family":""
            }
        )
        print(new_grant_gid)
        print("GYDGYUDGDYG")
        new_grant_relation = api_models.Relation(
            grid_id = f"{new_grant_gid.gid}[ENTITY]", 
            parent_resource_id = self.auth['entity'].gid,
            child_resource_id = new_grant_gid.gid,
            type = "ENTITY:FAMILY"
        )
        resource_utils.sign_resource(
            new_grant_gid,
            self.auth['keys']['sign'],
            session = self.session
        )
        self.session.add(new_grant_relation)
        return {
            "document":{
                "grid":new_grant_gid.gid
            }
        }

class DocumentFamilyGrant(MessageHandler):
    value_serializer = api_serializers.DocumentGrant
    rule_set = None
    def handle(self):
        created_anchors = []
        resource, proof_resources, children = resource_utils.get_with_proof(self.value.grid, ['proof', 'children', 'signature'], session = self.session)
        family = resource[0]
        if not self.value.grid.startswith(f"{self.auth['entity'].gid}[FAMILY]"):
            raise exceptions.AuthError(self.value.grid)
        new_grant_gid = api_models.GlobalID.create_resource(
            f"{self.value.grid}[GRANT]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(4))}",
            "GRANT",
            session = self.session,
            issuer = self.auth['entity'].gid,
            push_type = "LOCAL",
            values = {
                "family":self.value.grid,
                "entity": self.value.entity,
                "issue":self.value.permissions.issue,
                "grant":self.value.permissions.grant
            }
        )
        new_signature = resource_utils.sign_resource(
            new_grant_gid,
            self.auth['keys']['sign'],
            session = self.session
        )
        print(f"Signature: {new_signature.gid}")
        new_signature_relation = api_models.Relation(
            grid_id = f"{new_grant_gid.gid}[ENTITY]",
            parent_resource_id = self.value.grid,
            child_resource_id = self.value.entity,
            type = "ENTITY:GRANT"
        )
        self.session.add(new_signature_relation)
        return {
            "document":{
                "grid":new_grant_gid.gid
            }
        }


class IssueDocument(MessageHandler):
    value_serializer = api_serializers.DocumentIssue
    rule_set = None
    def handle(self):
        created_anchors = []
        family = None
        grant = None
        if self.value.grant:
            """stmt = select(
                    api_models.GlobalID, api_models.Relation
                ).join(
                    api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.parent_resource_id
                ).where(
                    api_models.Relation.parent_resource_id == self.value.family
                ).where(
                    api_models.Relation.child_resource_id == self.value.entity
                ).where(
                    api_models.Relation.type == "ENTITY:GRANT"
                )
            document_family_grant = self.session.scalars(stmt).first()
            if document_family_grant == None:
                cock()"""
            grant = self.value.grant
        new_document = api_models.GlobalID.create_resource(
            f"{self.auth['entity'].gid}[DOCUMENT]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(4))}",
            "DOCUMENT",
            session = self.session,
            push_type = "LOCAL",
            values = {
                "data":"ASSS",
                "grant":grant
            }
        )
        if grant != None:
            new_signature_relation = api_models.Relation(
                grid_id = f"{new_document.gid}[DOCUMENT_GRANT]",
                parent_resource_id = grant,
                child_resource_id = new_document.gid,
                type = "GRANT"
            )
            #print"GRANT")
            #printnew_signature_relation.grid_id)
            self.session.add(new_signature_relation)
        resource_utils.sign_resource(
            new_document,
            self.auth['keys']['sign'],
            session = self.session
        )
        return {
            "document":{
                "grid":new_document.gid
            }
        }