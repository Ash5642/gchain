import api.models as api_models
import crypt.models as crypt_models
from sqlmodel import Field, SQLModel, select
from database import get_session
import api.message_models as message_models

import anchors.models as anchor_models
import orgs.models as org_models


from api.generic_handlers import *
import orgs.serializers as org_serializers
import api.serializers as api_serializers
import host.serializers as host_serializers

import crypt.serializers as crypt_serializers
import api.utils.entity_utils as entity_utils
import api.utils.resource_utils as resource_utils

class HostCreate(MessageHandler):
    value_serializer = org_serializers.OrganizationCreateMessage
    def handle(self):
        #print"Running handler")
        new_gid = api_models.GlobalID(
            gid = self.value.organization.geid,
            type = "ORG"
        )
        new_org = org_models.Organization(
            geid_id = new_gid.gid
        )

        sign_key = crypt_models.Key.generate_key(geid = new_gid.gid, session = self.session, type = "SIGN")
        cipher_key = crypt_models.Key.generate_key(geid = new_gid.gid, session = self.session, type = "CIPHER")
        self.session.add(new_gid)
        self.session.add(new_org)
        entity_utils.create_entity(new_org, session = self.session)
        anchor_serialized = org_serializers.Organization.model_validate(new_org).model_dump()


    
        return {
            "penis":True,
            "cock":anchor_serialized
            
        }

