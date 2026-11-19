from fastapi import FastAPI, Depends
import api.models as api_models
import api.utils.entity_utils as entity_utils
import crypt.models as crypt_models
from sqlmodel import Field, SQLModel, select
from database import get_session
from fastapi import APIRouter
import api.message_models as message_models
import api.exceptions as api_exceptions
import anchors.models as anchor_models
from abc import ABC



class MessageHandler(ABC):
    value: dict = None
    handler_result = None
    value_serializer = None
    result = None
    session = None
    commit = True
    user = None
    auth = None
    entity = None
    def load(self):
        return 

    def handle(self):
        return


    def __init__(self, value, *args, **kwargs):
        try:
            self.result = {
                "status":False,
                "value":None,
                "errors":[]
            }
            self.auth = kwargs.get('auth')
            #printself.auth)
            if self.value_serializer != None:
                self.value = self.value_serializer.model_validate(value)
            self.session = kwargs['session']
            self.load()
            self.handler_result = self.handle()
            self.result['value'] = self.handler_result
            self.result['status'] = True
        except Exception as e:
            raise e
            #print"ERRRRRRRRRR")
            self.commit = False
            if hasattr(e, 'result'):
                self.result['errors'].append(
                    e.result
                )
            else:
                self.result['errors'].append(
                    str(e)
                )
