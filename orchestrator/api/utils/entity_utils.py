import api.exceptions as api_exceptions
import api.models as api_models
import crypt.models as crypt_models
from api.utils import resource_utils
from sqlalchemy import String, Text, ForeignKey, LargeBinary, select
import secrets, string
import api.exceptions as api_exceptions
def create_entity(name, *args, **kwargs):
    geid = ""
    parent = ""
    parent_geid = None
    
    if kwargs.get('parent')!=None:
        stmt = select(api_models.GlobalID).where(api_models.GlobalID.gid == kwargs['parent'])
        parent = kwargs.get('session').scalars(stmt).first()
        parent_geid = kwargs['parent']
        if parent == None:
            raise api_exceptions.NotFound("entity", kwargs['parent'])
        if kwargs['type'] == 'ANCHOR' and parent.type == 'ANCHOR':
            geid = f"{kwargs['parent']}:{name}"
        elif parent.type == 'ANCHOR':
            geid = f"{kwargs['parent']}::{name}"
        else:
            geid = f"{kwargs['parent']}:{name}"
        #print"GEID")
        #printname)
        #printkwargs['parent'])
        #printgeid)

    else:
        parent = None
        parent_geid = None
        if kwargs['type'] == 'ANCHOR':
            geid = name
        else:
            geid = f"PRIVATE::{name}"

    new_gid = api_models.GlobalID(
        gid = geid,
        type = kwargs.get('type')
    )
    new_record_grid = api_models.GlobalID.create_resource(
        f"{geid}[RECORD]",
        "RECORD",
        parent = new_gid.gid if new_gid != None else None,
        session = kwargs.get('session'),
        push_type = "CHAIN" if parent == None else "LOCAL",
        issuer = parent_geid,
        values = {
            "type":kwargs['type']
        }
    )
    kwargs['session'].add(new_gid)
    kwargs['session'].add(new_record_grid)
    print(f"Created new entity {geid}")
    signing_key = crypt_models.Key.generate_key(
        geid = geid, 
        type = "SIGN", 
        session = kwargs.get('session'),
        push_type = "CHAIN" if parent == None else "LOCAL",
        issuer = parent_geid
    )
    cipher_key = crypt_models.Key.generate_key(
        geid = geid, 
        type = "CIPHER", 
        session = kwargs.get('session'),
        push_type = "CHAIN" if parent == None else "LOCAL",
        issuer = parent_geid
    )


    
    return new_gid

    
def get_entity_key(entity, key_type, *args, **kwargs):
    if 'grid' not in kwargs:
        results = resource_utils.get_packaged(
            {
                "prefix":f"{entity}[KEY][{key_type}]",
                "type":"KEY"
            },
            [],
            session = kwargs.get('session')
        )
    else:
        results = resource_utils.get_packaged(
            {
                "grid":kwargs.get('grid'),
                "type":"KEY"
            },
            [],
            session = kwargs.get('session')
        )

    if len(results) == 0:
        raise api_exceptions.KeyError()
    results[-1].load_values()
    return results[-1]
    

