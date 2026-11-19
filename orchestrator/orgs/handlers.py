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
import orgs.serializers as org_serializers

import crypt.serializers as crypt_serializers
import api.utils.entity_utils as entity_utils
import api.utils.resource_utils as resource_utils
import secrets, string
class OrganizationHandler(MessageHandler):
    value_serializer = None
    value_serializer = org_serializers.OrgMessage
    org = None
    employee = None
    def load(self, *args, **kwargs):
        self.org = resource_utils.get_by_gid(self.value.org, session = self.session, model_only = True)

    
    def handle(self, *args, **kwargs):

        return {
            "ass":"COCK"
        }
    
        
class EmploymentManagement(OrganizationHandler):
    value_serializer = None
    value_serializer = org_serializers.OrgMessage
    org = None
    def handle(self, *args, **kwargs):

        
        
        new_employee_gid = api_models.GlobalID(
            gid = f"{self.org.gid}[EMPLOYEE]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(4))}",
            type = "RECORD:EMPLOYEE"
        )
        new_employee = org_models.Employees(
            grid_id = new_employee_gid.gid,
            employee_id = self.auth['geid']
        )
        self.session.add(new_employee_gid)
        self.session.add(new_employee)
        return {
            "ass":"COCK",
            "cock":new_employee.grid_id
        }

class RoleManagement(OrganizationHandler):
    value_serializer = None
    value_serializer = org_serializers.OrgRoleMessage
    org = None
    def handle(self, *args, **kwargs):
        self.org = resource_utils.get_by_gid(self.value.org, session = self.session, model_only = True)
        stmt = select(org_models.Role).where(org_models.Role.grid_id == f"{self.org.gid}[ROLE][{self.value.role}]")
        role_match = self.session.scalars(stmt).first()
    
        match self.value.action.upper():
            case "ADD":
                if role_match != None:
                    raise api_exceptions.Exists("role", f"{self.org.gid}[ROLE][{self.value.role}]")
        
                new_employee_gid = api_models.GlobalID(
                    gid = f"{self.org.gid}[ROLE][{self.value.role}]",
                    type = "ROLE"
                )
                new_employee = org_models.Role(
                    grid_id = new_employee_gid.gid
                )
                self.session.add(new_employee_gid)
                self.session.add(new_employee)
            case "REMOVE":
                pass
        
        return {
        }


class RoleEmployees(OrganizationHandler):
    value_serializer = None
    value_serializer = org_serializers.OrgRoleMessage
    org = None
    def handle(self, *args, **kwargs):
        self.org = resource_utils.get_by_gid(self.value.org, session = self.session, model_only = True)
        stmt = select(org_models.Role).where(org_models.Role.grid_id == f"{self.org.gid}[ROLE][{self.value.role}]")
        role_match = self.session.scalars(stmt).first()
        if role_match == None:
            raise api_exceptions.NotFound("role", self.value.role)
        match self.value.action.upper():
            case "ADD":
                pass
            case "REMOVE":
                pass
        
        return {
        }
