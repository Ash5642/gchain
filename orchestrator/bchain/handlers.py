import api.models as api_models
import crypt.models as crypt_models
from sqlmodel import Field, SQLModel, select
from database import get_session
import api.message_models as message_models

import anchors.models as anchor_models
import orgs.models as org_models

import struct
from api.generic_handlers import *
import orgs.serializers as org_serializers
import api.serializers as api_serializers
import orgs.serializers as org_serializers

import crypt.serializers as crypt_serializers
import api.utils.entity_utils as entity_utils
import api.utils.resource_utils as resource_utils
import secrets, string
from bchain.utils import core_messages
import requests
from functools import reduce
from scapy.all import IP, TCP, send
import scapy.all as scapy
from bchain import message_models
import socket
import data_pb2
import bchain.utils.core_mappers as core_mapper
import bchain.utils.resource_management as resource_management
class ChainOptions(MessageHandler):
    value_serializer = api_serializers.ChainMessage
    def handle(self):
        if self.value.action:
            if self.value.message:
                core_messages.send_command(self.value.action, message = self.value.message, session = self.session)
            else:
                core_messages.send_command(self.value.action, session = self.session)
    
        return

class ChainPush(MessageHandler):
    value_serializer = message_models.Push
    def handle(self):
        core_messages.push_resource(self.value.grid, session = self.session)


class ChainReset(MessageHandler):
    def handle(self):
        stmt = select(api_models.GlobalID).where(api_models.GlobalID.hash.is_not(None))
        
        grid_result = self.session.scalars(stmt).all()
        for result in grid_result:
            result.pushed = False
            self.session.add(result)
        return



class HostMessage(MessageHandler):
    value_serializer = api_serializers.ChainMessage
    def handle(self):
        handler = core_mapper.handler_map[self.value.message['type']][0](self.value.message, session = self.session)
        handler.send_message()
        handler.handle_response()
        #print"Response recieved!")

        return