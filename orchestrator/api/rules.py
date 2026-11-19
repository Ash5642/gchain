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
            "public_key":["bytes", b"", 1],
            "private_key":["bytes", b"", 2],
            "REVOKED":["bool", True, 0],
            "nonce":["bytes", b"", 0],
            "signature":["bytes", b"", 0],
            "type":["str", "RSA", 3]
            
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
            "key":["gid:key", "KEY", 0],
            "resource":["gid", "GRID", 0],
            "resource_hash": ["bytes", b"", 0],
            "signature":["bytes", b"", 0],
            "status":["str", "pending", 0]
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
            "type":["str", "", 0]
        },
        "chain":"BOTH"
    },
    "DOCUMENT":{
        "data_template":{
            "type":["str", "GENERIC", 0],
            "family":["gid", "", ["FAMILY"], 0],
            "grant":["gid", "", ["GRANT"], 0],
            "data":["str", "", 0]
        },
        "load":[
            ["GRANT", "PARENT"]
        ]
    },

    "FAMILY":{
        "data_template":{
            "name":["str", "GENERIC", 0],
            "parent":["gid", "", ["FAMILY"], 0],
            "template":["str", "", 0]
        }
    },
    "GRANT":{
        "data_template":{
            "family":["gid", "", ["FAMILY"], 0],
            "entity":["gid", "", ["FAMILY"], 0],
        }
    },
    "LOCALROOT":{
        "data_template":{
            "key":["gid:key", "KEY", 0],
            "resource_hash": ["bytes", b"", 0],
            "signature":["bytes", b"", 0],
        },
        "load":[
            ["KEY", "PARENT"]
        ]
    },
    "REVOKE":{
        "data_template":{
            "key":["gid:key", "KEY", 0],
            "grid":["ARR:gid", "KEY", 0],
            "signature":["bytes", b"", 0],
        },
        "load":[]
    },
    "CASH":{
        "data_template":{
            "rx":["str", "", 0],
            "amt":["int", int(0).to_bytes(), 0],
        },
        "load":[]
    },
    "EXECUTABLE":{
        "data_template":{
            "bytecode":["bytes", b"", 0],
        }
    },
    "CALL":{
        "data_template":{
            "executable":["gid", b"", 0],
            "caller":["gid", b"", 0],
            "arguments":["bytes", b"", 0]
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