



class APIException(Exception):
    result = {}


class KeyError(Exception):

    def __init__(self):
        self.result = {
            "type":"key",
            "detail":"no_valid_keys"
        }

class AuthError(Exception):

    def __init__(self, resource_name):
        self.result = {
            "type":"auth",
            "resource":resource_name
        }
class NotFound(Exception):
    type = None
    name = None
    def __init__(self, resource_type, resource_name):
        self.result = {
            "type":"not found",
            "resource":f"{resource_type}={resource_name}"
        }
    
class InvalidParent(Exception):
    type = None
    name = None
    def __init__(self, resource_type, resource_name):
        self.result = {
            "type":"invalid_type",
            "resource":f"parent must be of types {resource_name}"
        }
    
class Exists(Exception):
    type = None
    name = None
    def __init__(self, resource_type, resource_name):
        self.result = {
            "type":"already exists",
            "resource":f"{resource_type}={resource_name}"
        }