import api.serializers as api_serializers
import api.models as api_models
import docs.models as doc_models
from abc import ABC



entity = {
    "USER":{
        "parents":[None, "ANCHOR"]
    },
    "ANCHOR":{
        "parents":[None, "ANCHOR"]
    },
    "HOST":{
        "parents":[None, "ANCHOR"]
    },
    "ORG":{
        "parents":["HOST"]
    }
}

resource = {
    "ANCHOR":{
        "data_template":{}
    },
    "KEY":{
        "attribute":"key",
        "recurse":False,
        "serializer":api_serializers.GlobalID,
        "issued":[

        ],
         "data_template":{
            "public_key":["bytes", b""],
            "private_key":["bytes", b""],
            "REVOKED":["bool", True],
            "nonce":["bytes", b""],
            "signature":["bytes", b""],
            "type":["str", "RSA"]
            
        },
        "load":[
            ["KEY", "PARENT"],
            ["REVOKE", "PARENT"]
        ],
        "chain":"BOTH"
    },
    "SIGNATURE":{
        "attribute":"key",
        "recurse":False,
        "serializer":api_serializers.GlobalID,
        "issued":[

        ],
         "data_template":{
            "key":["gid:key", "KEY"],
            "resource":["gid", "GRID"],
            "resource_hash": ["bytes", b""],
            "signature":["bytes", b""],
            "status":["str", "pending"]
        },
        "load":[
            ["KEY", "PARENT"]
        ],
        "chain":"BOTH"
    },
    "VALUE":{},
    "RECORD":{
        "attribute":"record",
        "recurse":True,
        "serializer":api_serializers.GlobalID,
        "issued":[

        ],
         "data_template":{
            "type":["str", ""]
        },
        "chain":"BOTH"
    },
    "DOCUMENT":{
        "data_template":{
            "type":["str", "GENERIC"],
            "family":["gid", "", ["FAMILY"]],
            "grant":["gid", "", ["GRANT"]],
            "data":["str", ""]
        },
        "load":[
            ["GRANT", "PARENT"]
        ]
    },

    "FAMILY":{
        "data_template":{
            "name":["str", "GENERIC"],
            "parent":["gid", "", ["FAMILY"]],
            "template":["str", ""]
        }
    },
    "GRANT":{
        "data_template":{
            "family":["gid", "", ["FAMILY"]],
            "entity":["gid", "", ["FAMILY"]],
        }
    },
    "LOCALROOT":{
        "data_template":{
            "key":["gid:key", "KEY"],
            "resource_hash": ["bytes", b""],
            "signature":["bytes", b""],
        },
        "load":[
            ["KEY", "PARENT"]
        ]
    },
    "REVOKE":{
        "data_template":{
            "key":["gid:key", "KEY"],
            "grid":["ARR:gid", "KEY"],
            "signature":["bytes", b""],
        },
        "load":[]
    },
    "CASH":{
        "data_template":{
            "rx":["str", ""],
            "amt":["int", int(0).to_bytes()],
        },
        "load":[]
    },
    "EXECUTABLE":{
        "data_template":{
            "bytecode":["bytes", b""],
        }
    },
    "CALL":{
        "data_template":{
            "executable":["gid", b""],
            "caller":["gid", b""],
        }
    },
}

chain_types = [
    "LROOT"
]

local_types = [
    "DOCUMENT", 
    "KEY",
    "GRANT"
]


class Resource(ABC):
    actions = {
        "sign":api_models.GlobalID.sign_resource
    }
    pass


class Key(Resource):
    identifier = "KEY"
    ownership = "*"
    fields = {
        "public_key":{
            "type":"bytes",
        },
        "private_key":{
            "type":"bytes",
            "options":["SIGN", "CIPHER"],
            "hidden":True
        },
        "type":{
            "type":"str",
            "options":["SIGN", "CIPHER"]
        }
    }


class Record(Resource):
    identifier = "KEY"
    ownership = "*"
    fields = {

        "type":{
            "type":"str"
        }
    }

class Signature(Resource):
    identifier = "SIGNATURE"
    ownership = "*"
    actions = []
    fields = {
        "signature":{
            "type":"bytes",
        },
        "key":{
            "type":"gid",
        }
    }