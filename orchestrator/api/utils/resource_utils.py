import api.models as api_models
import api.serializers as api_serializers
import api.utils.entity_utils as entity_utils
import crypt.models as crypt_models
import bchain.models as bchain_models
from typing import List
from sqlalchemy import String, Text, ForeignKey, LargeBinary, select, except_
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, declared_attr, joinedload, selectinload
from pydantic import TypeAdapter
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
import api.exceptions as api_exceptions
import secrets, string
import api.rules as rules
import api.data_models as data_models
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.mldsa import MLDSA65PrivateKey
from cryptography.hazmat.primitives.asymmetric import mlkem
from cryptography.hazmat.primitives.asymmetric.mlkem import MLKEM768PrivateKey
import hashlib
def get_by_gid(gid, *args, **kwargs):
    session = kwargs.get('session')

    stmt = select(api_models.GlobalID).options(joinedload(api_models.GlobalID.children)).options(joinedload(api_models.GlobalID.values)).where(api_models.GlobalID.gid == gid)
    result = kwargs['session'].scalars(stmt).first()
    if result == None:
        raise api_exceptions.NotFound("gid", gid)
    if kwargs.get('model_only'):
        return result
    
    data_model = data_models.Resource(result, session = kwargs.get('session'), depth = 3, no_recurse = True)
    data_list = [data_model]+data_model.get_children(session = kwargs['session'])
    
    return result, data_list


def get_recursed(gid, *args, **kwargs):
    results = []
    session = kwargs.get('session')
    stmt = select(api_models.GlobalID).options(joinedload(api_models.GlobalID.children)).options(joinedload(api_models.GlobalID.values)).where(api_models.GlobalID.gid == gid)
    result = kwargs['session'].scalars(stmt).first()
    results.append(result)

    stmt = select(
        api_models.GlobalID, api_models.Relation
    ).join(
        api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.child_resource_id
    ).where(
        api_models.Relation.parent_resource_id == gid
    )
    child_results = kwargs['session'].scalars(stmt).unique().all()
    loaded = kwargs.get('loaded', [result.gid])
    children = []
    for child_result in child_results:
        children.append(child_result.gid)

    return results, children

def get_with_proof2(gid, include, *args, **kwargs):
    
    if isinstance(gid, api_models.GlobalID):
        result = gid
    elif isinstance(gid, str):
        if gid.endswith('*'):
            stmt = select(api_models.GlobalID).options(joinedload(api_models.GlobalID.children)).options(joinedload(api_models.GlobalID.values)).where(api_models.GlobalID.gid.startswith(gid[:-1]))
        else:
            stmt = select(api_models.GlobalID).options(joinedload(api_models.GlobalID.children)).options(joinedload(api_models.GlobalID.values)).where(api_models.GlobalID.gid == gid)
        result = kwargs['session'].scalars(stmt).first()
    loaded = kwargs.get('loaded', [])
    loaded.append(result.gid)
    children = []
    resources = [result]
    proofs = []

    ruleset = rules.resource.get(result.type)
    result.load_values()
    if ruleset != None and ruleset.get("load"):
        for load_type in ruleset.get('load'):
            #printload_type)
            if load_type[1] == "PARENT":
                stmt = select(
                    api_models.GlobalID, api_models.Relation
                ).join(
                    api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.parent_resource_id
                ).where(
                    api_models.Relation.child_resource_id == result.gid
                ).where(
                    api_models.Relation.type == load_type[0]
                )
            else:
                stmt = select(
                    api_models.GlobalID, api_models.Relation
                ).join(
                    api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.child_resource_id
                ).where(
                    api_models.Relation.parent_resource_id == result.gid
                ).where(
                    api_models.Relation.type == load_type[0]
                )
            proofs = proofs + kwargs['session'].scalars(stmt).all()
            
    if 'proof' in include:
        stmt = select(
            api_models.GlobalID, api_models.Relation
        ).join(
            api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.parent_resource_id
        ).where(
            api_models.Relation.child_resource_id == result.gid
        ).where(
            api_models.Relation.type == "LROOT"
        )
        
        proofs = proofs + kwargs['session'].scalars(stmt).all()
        stmt = select(
            api_models.GlobalID, api_models.Relation
        ).join(
            api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.parent_resource_id
        ).where(
            api_models.Relation.child_resource_id == result.gid
        ).where(
            api_models.GlobalID.type == "SIGNATURE"
        )
        proofs = proofs + kwargs['session'].scalars(stmt).all()
    result.merkle_proofs = []
    lroot_gids = []
    if 'proof' in include:
        stmt = select(
            bchain_models.MerkleNode
        ).options(
            joinedload(bchain_models.MerkleNode.parent)
        ).where(
            bchain_models.MerkleNode.resource_id == result.gid
        ).where(
            bchain_models.MerkleNode.type == "LEAF"
        )
        merkle_proofs = kwargs['session'].scalars(stmt).unique().all()
        #printmerkle_proofs)
        merkle_proof_list = []
        #print"OGPROOFS:",len(merkle_proofs))
        for proof in merkle_proofs:
    
            current_proof = []
            current_node = proof
            while current_node.parent_id != None:
                stmt = select(
                    bchain_models.MerkleNode
                ).options(
                    joinedload(bchain_models.MerkleNode.parent)
                ).where(
                    bchain_models.MerkleNode.value == current_node.parent_id
                )
                current_node = kwargs['session'].scalars(stmt).unique().first()
                
                current_proof.append(current_node.children[0].value)
                current_proof.append(current_node.children[1].value)
            #printcurrent_node.type)
            #printcurrent_node.parent_id)
            current_proof.append(current_node.value)
    
            result.merkle_proofs.append(
                {
                    "path":current_proof,
                    "root":"ASS",
                    "type":current_node.type,
                    "proof_for":proof.proof_for
                }
            )
            if current_node.type == "ROOT:CHAIN":
                result.proof_resources = []
                return resources, [], children
    
    #print"PROOFS:",len(result.merkle_proofs))
    #printresult.merkle_proofs)
    loaded_proofs = []
    for proof in proofs:
        proof_resource, new_proofs, new_children = get_with_proof(proof, ['proof', 'children', 'signature'], flat = False, session = kwargs.get('session'))
        for new_proof in proof_resource + new_proofs:
            #printf"NEWPROOF for {proof.gid}: {new_proof.gid}")
            if new_proof.gid not in loaded:
                loaded_proofs.append(new_proof)
                loaded.append(new_proof.gid)


    if 'children' in include:
        stmt = select(
            api_models.GlobalID.gid, api_models.Relation.child_resource_id, api_models.Relation.parent_resource_id
        ).join(
            api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.child_resource_id
        ).where(
            api_models.Relation.parent_resource_id == result.gid
        )

        child_results = kwargs['session'].scalars(stmt).all()
        for child in child_results:
            children.append(child)
    if kwargs.get('flat') != True:
        result.proof_resources = loaded_proofs
    return resources, loaded_proofs, children

def get_with_proof(gid, include, *args, **kwargs):
    
    if isinstance(gid, api_models.GlobalID):
        result = gid
    elif isinstance(gid, str):
        if gid.endswith('*'):
            stmt = select(api_models.GlobalID).options(joinedload(api_models.GlobalID.children)).options(joinedload(api_models.GlobalID.values)).where(api_models.GlobalID.gid.startswith(gid[:-1]))
        else:
            stmt = select(api_models.GlobalID).options(joinedload(api_models.GlobalID.children)).options(joinedload(api_models.GlobalID.values)).where(api_models.GlobalID.gid == gid)
        result = kwargs['session'].scalars(stmt).first()
    
    loaded = kwargs.get('loaded', [])
    loaded.append(result.gid)
    children = []
    resources = [result]

    proofs = []
    signatures = []

    combined_proof_gids = []

    ruleset = rules.resource.get(result.type)
    result.load_values()

    if 'proof' in include:

        stmt = select(
            api_models.GlobalID, api_models.Relation
        ).join(
            api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.parent_resource_id
        ).where(
            api_models.Relation.child_resource_id == result.gid
        ).where(
            api_models.GlobalID.type == "SIGNATURE"
        )
        signatures = kwargs['session'].scalars(stmt).all()
        proofs = proofs + signatures
    lroot_gids = []


    if 'proofs' in kwargs:
        for proof in kwargs.get('proofs'):
            if proof['resource'] == result.gid:
                result.merkle_proofs.append(proof)
    elif 'proof' in include:
        stmt = select(
            bchain_models.MerkleNode
        ).options(
            joinedload(bchain_models.MerkleNode.parent)
        ).where(
            bchain_models.MerkleNode.resource_id.in_([proof_resource.gid for proof_resource in resources+signatures])
        ).where(
            bchain_models.MerkleNode.type == "LEAF"
        )
        merkle_proofs = kwargs['session'].scalars(stmt).unique().all()
        #printmerkle_proofs)
        merkle_proof_list = []
        #print"OGPROOFS:",len(merkle_proofs))
        for proof in merkle_proofs:
            current_proof = []
            current_node = proof
            while current_node.parent_id != None:
                stmt = select(
                    bchain_models.MerkleNode
                ).options(
                    joinedload(bchain_models.MerkleNode.parent)
                ).where(
                    bchain_models.MerkleNode.value == current_node.parent_id
                )
                current_node = kwargs['session'].scalars(stmt).unique().first()
                
                current_proof.append(current_node.children[0].value)
                current_proof.append(current_node.children[1].value)
            #printcurrent_node.type)
            #printcurrent_node.parent_id)
            current_proof.append(current_node.value)
    
            result.merkle_proofs.append(
                {
                    "path":current_proof,
                    "root":"ASS",
                    "type":current_node.type,
                    "proof_for":proof.proof_for,
                    "block":proof.block,
                    "resource":proof.resource_id
                }
            )
            if current_node.type == "ROOT:CHAIN":
                result.proof_resources = []
                return resources, [], children
    
    #print"PROOFS:",len(result.merkle_proofs))
    #printresult.merkle_proofs)
    loaded_proofs = []
    for proof in proofs:
        proof_resource, new_proofs, new_children = get_with_proof(proof, ['proof', 'children', 'signature'], flat = False, session = kwargs.get('session'), proofs = result.merkle_proofs)
        for new_proof in proof_resource + new_proofs:
            #printf"NEWPROOF for {proof.gid}: {new_proof.gid}")
            if new_proof.gid not in loaded:
                loaded_proofs.append(new_proof)
                loaded.append(new_proof.gid)


    if 'children' in include:
        stmt = select(
            api_models.GlobalID.gid, api_models.Relation.child_resource_id, api_models.Relation.parent_resource_id
        ).join(
            api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.child_resource_id
        ).where(
            api_models.Relation.parent_resource_id == result.gid
        )

        child_results = kwargs['session'].scalars(stmt).all()
        for child in child_results:
            children.append(child)
    if kwargs.get('flat') != True:
        result.proof_resources = loaded_proofs
    return resources, loaded_proofs, children


def get_packaged(gid, include, *args, **kwargs):
    stmt = select(
        api_models.GlobalID, api_models.Value
    )


    if isinstance(gid, str):
        stmt = stmt.where(
            api_models.GlobalID.gid == gid
        )
        root_resource = kwargs['session'].scalars(stmt).unique().all()
    if isinstance(gid, dict):
        if 'prefix' in gid:
            stmt = stmt.where(
                api_models.GlobalID.gid.startswith(gid['prefix'])
            )
        if 'gid' in gid:
            stmt = stmt.where(
                api_models.GlobalID.gid == gid['gid']
            )
        if 'type' in gid:
            stmt = stmt.where(
                api_models.GlobalID.type == gid['type']
            )
        if 'entity' in gid:
            stmt = stmt.where(
                api_models.GlobalID.issuer == gid['entity']
            )
        if 'pushed' in gid:
            stmt = stmt.where(
                api_models.GlobalID.pushed == gid['pushed']
            )
        if 'confirmed' in gid:
            stmt = stmt.where(
                api_models.GlobalID.pushed == gid['pushed']
            )

        root_resource = kwargs['session'].scalars(stmt).unique().all()
        
    if isinstance(gid, api_models.GlobalID):
        root_resource = [gid]
        

    final = []
    queue = root_resource
    """if include == []:
        #printroot_resource[0].gid)
        #print"KEEE")
        root_resource[0].load_values()
        return root_resource"""
    #print"CALLED")
    while queue:
        resource = queue.pop(0)
        resource.load_values()
        resource.merkle_node = []
        if resource.type in ["RECORD", "KEY"]:
            final.append(resource)
            continue
        if resource.push_type == "CHAIN" and resource.confirmed == True and "FULL" not in include:
            final.append(resource)
            continue

        load_types = []
        ruleset = rules.resource.get(resource.type)

        if ruleset != None and ruleset.get("load"):
            load_types = ruleset.get("load")
        full_load_types = ["SIGNATURE", "LROOT", "LOCALROOT"] + ([load_type[0] for load_type in load_types] if 'RELATED' in include else [])
        if resource.push_type == "LOCAL":
            full_load_types = ["LOCAL", "LOCALROOT"]

        stmt = select(
            api_models.GlobalID, api_models.Relation.parent_resource_id, api_models.Value
        ).join(
            api_models.GlobalID, api_models.GlobalID.gid == api_models.Relation.parent_resource_id
        ).options(
            joinedload(api_models.GlobalID.values)
        ).where(
            api_models.Relation.child_resource_id == resource.gid
        ).where(
            api_models.GlobalID.type.in_(
                full_load_types
            )
        ).where(
            api_models.GlobalID.gid.notin_([existing.gid for existing in final])
        )

        requires = kwargs['session'].scalars(stmt).unique().all()
        #print[require for require in requires if require.gid not in [queue_resource.gid for queue_resource in queue]])
        resource.proof_resources = requires
        final.append(resource)
        queue = queue+[require for require in requires if require.gid not in [queue_resource.gid for queue_resource in queue]]
    
    

    if 'PROOF' in include:
        merkle_proofs = get_proofs([element.gid for element in final], session = kwargs.get('session'))
        for proof in merkle_proofs:
            for resource in final:
                if resource.gid == proof['resource']:
                    resource.merkle_proofs.append(proof)

    if 'CHILDREN' in include:
        children = get_children([element.gid for element in final], session = kwargs.get('session'))
        for child in children:
            for resource in final:
                if resource.gid == child.parent_resource_id:
                    resource.children.append(child)

    return final


def get_children(gids, *args, **kwargs):
    stmt = select(
        api_models.Relation, api_models.Relation.parent_resource_id
    ).where(
        api_models.Relation.parent_resource_id.in_(gids)
    )

    return kwargs['session'].scalars(stmt).all()
    

def get_proofs(gids, *args, **kwargs):
    stmt = select(
        bchain_models.MerkleNode
    ).options(
        joinedload(bchain_models.MerkleNode.parent)
    ).where(
        bchain_models.MerkleNode.resource_id.in_(gids)
    ).where(
        bchain_models.MerkleNode.parent_id != None
    )
    merkle_proofs = kwargs['session'].scalars(stmt).unique().all()
    merkle_proof_list = []
    for proof in merkle_proofs:
        current_proof = []
        current_node = proof
        while current_node.parent_id != None:
            stmt = select(
                bchain_models.MerkleNode
            ).options(
                joinedload(bchain_models.MerkleNode.parent)
            ).where(
                bchain_models.MerkleNode.value == current_node.parent_id
            )
            current_node = kwargs['session'].scalars(stmt).first()

            for child in current_node.children:
                current_proof.append(child.value)

        #printcurrent_node.type)
        #printcurrent_node.parent_id)
        current_proof.append(current_node.value)
        print(proof.block)
        print("CUICHUKHCUIYGHCBC")
        merkle_proof_list.append(
            {
                "path":current_proof,
                "type":"ROOT:CHAIN" if proof.proof_for.isdigit() else "LOCAL",
                "proof_for":proof.proof_for if proof.proof_for != None else "ROOT:CHAIN",
                "block":proof.block,
                "grid":proof.grid,
                "resource":proof.resource_id
            }
        )
    return merkle_proof_list



def check_signature(resource, key, signature, *args, **kwargs):
        #printkey.loaded_values)
        if isinstance(signature, str):
            signature = bytes.fromhex(signature)
        public_key = serialization.load_der_public_key(
            key.loaded_values['public_key']
        )
        try:
            public_key.verify(
                signature,
                resource.binary,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except InvalidSignature:
            return False

def sign_resource(resource, key, *args, **kwargs):
    signature = None
    status = None


    private_key = serialization.load_der_private_key(
        data=key.loaded_values['private_key'],
        password=None  # Change to your byte password if it was encrypted
    )
    public_key = serialization.load_der_public_key(
        data=key.loaded_values['public_key'],
    )
    if kwargs.get("ret_only"):
        signature = private_key.sign(resource)
        return signature
        
    resource_encoded_hash = resource.get_hash(session = kwargs.get('session'))
    print(resource_encoded_hash)
    print("HAH")
    signature = private_key.sign(resource_encoded_hash)
    signature_validity = public_key.verify(signature, resource_encoded_hash)



    new_signature_grid = api_models.GlobalID.create_resource(
        f"{key.gid.split('[')[0]}[SIGNATURE][{key.gid.split('=')[1]}]={''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))}",
        "SIGNATURE",
        session = kwargs.get('session'),
        values = {
            "key":key.gid,
            "signature":signature,
            "resource":resource.gid,
            "status":"complete",
            "resource_hash":resource_encoded_hash
        }
    )
    new_signature_relation = api_models.Relation(
        grid_id = f"{new_signature_grid.gid}[SIGNATURE]",
        parent_resource_id = new_signature_grid.gid,
        child_resource_id = resource.gid,
        type = "SIGNATURE"
    )
    new_relation = api_models.Relation(
        grid_id = f"{new_signature_grid.gid}[KEY]",
        parent_resource_id = key.gid,
        child_resource_id = new_signature_grid.gid,
        type = "KEY"
    )


    kwargs.get('session').add(new_relation)
    kwargs.get('session').add(new_signature_relation)
    print("Signed successfully!")
    return new_signature_grid


    