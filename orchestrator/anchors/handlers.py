import api.models as api_models
import crypt.models as crypt_models
from sqlmodel import Field, SQLModel, select
from database import get_session
import api.message_models as message_models

import anchors.models as anchor_models


from api.generic_handlers import *
import anchors.serializers as anchor_serializers
import api.serializers as api_serializers

import crypt.serializers as crypt_serializers
import api.utils.entity_utils as entity_utils
import api.utils.resource_utils as resource_utils

class AnchorCreate(MessageHandler):
    value_serializer = anchor_serializers.AnchorCreateMessage
    def handle(self):
        #print"Running handler")
        #printself.value.anchor.name)


        entity_utils.create_entity(self.value.anchor.name, type="ANCHOR", parent = self.value.anchor.parent, session = self.session)


    
        return {
            "penis":True
            
        }

