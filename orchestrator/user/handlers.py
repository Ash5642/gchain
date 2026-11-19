import api.models as api_models
import crypt.models as crypt_models
from sqlmodel import Field, SQLModel, select
from database import get_session
import api.message_models as message_models

import anchors.models as anchor_models
import orgs.models as org_models
import user.models as user_models


from api.generic_handlers import *
import orgs.serializers as org_serializers
import api.serializers as api_serializers
import user.serializers as user_serializers

import crypt.serializers as crypt_serializers
import api.utils.entity_utils as entity_utils
import api.utils.resource_utils as resource_utils

class UserCreate(MessageHandler):
    value_serializer = user_serializers.UserCreateMessage
    def handle(self):

        new_user = entity_utils.create_entity(self.value.user.geid, type="USER", parent=self.value.user.anchor, session = self.session)


    
        return {
            "penis":True,
            "user":new_user.gid
            
        }

