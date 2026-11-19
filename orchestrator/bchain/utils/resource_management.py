import socket, data_pb2, struct, hashlib, secrets, string
import bchain.models as bchain_models
import abc
import hashlib
import string
import secrets
import api.models as api_models
import bchain.models as chain_models
from itertools import batched
from sqlmodel import Field, SQLModel, select

def bytes_to_hex(data):
    if isinstance(data, dict):
        return {k: bytes_to_hex(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [bytes_to_hex(i) for i in data]
    elif isinstance(data, bytes):
        return data.hex()  # Or data.hex(' ') for separated format
    return data



class Resource:

    def save_protobuf(protobuf, *args, **kwargs):
        #printprotobuf.payload.grid)
            
        stmt = select(api_models.GlobalID).where(api_models.GlobalID.gid == protobuf.payload.grid)
        grid_result = kwargs.get('session').scalars(stmt).first()




        Resource.save_proof(protobuf, session = kwargs.get('session'))

    def save_proof(protobuf, *args, **kwargs):
        stmt = select(chain_models.MerkleNode).where(chain_models.MerkleNode.resource_id == protobuf.payload.grid)
        old_proofs = kwargs.get('session').scalars(stmt).all()

        stmt = select(api_models.GlobalID).where(api_models.GlobalID.gid == protobuf.payload.grid)
        resource = kwargs.get('session').scalars(stmt).first()
        if resource == None:
            print("Creating resource")
            resource = api_models.GlobalID.create_resource(
                protobuf.payload.grid,
                protobuf.payload.type,
                session = kwargs.get('session'),
                values = False,
                loaded = False,
                nonce = protobuf.payload.nonce
            )
            for value in protobuf.payload.fields:
                print(f"Adding field: {value.key}")
                new_key_grid = api_models.GlobalID.create_resource(
                    f"{protobuf.payload.grid}[VALUE][{value.key}]",
                    "VALUE",
                    session = kwargs.get('session'),
                    push_type = None,
                    nonce = value.nonce
                )
                new_key_value = api_models.Value(
                    name = value.key,
                    grid_id = f"{protobuf.payload.grid}[VALUE][{value.key}]",
                    type = value.type,
                    value = value.value,
                    resource_id = protobuf.payload.grid
                )   

                kwargs.get('session').add(new_key_value)
            
            print(f"Generated: {resource.get_protobuf(False).SerializeToString().hex()}")
            print(f"Recieved: {protobuf.payload.SerializeToString().hex()}")
            print(f"Hash: {resource.hash.hex()}")
        
        resource.pulled = True
        resource.loaded = True
        resource.confirmed = True
        kwargs.get('session').add(resource)

        for proof in old_proofs:
            current = proof
            while current.parent_id != None:
                stmt = select(chain_models.MerkleNode).where(chain_models.MerkleNode.value == current.parent_id)
                new = kwargs.get('session').scalars(stmt).first()
                if new == None:
                    break
                current = new


        for proof in protobuf.proof:
            print(proof.type)
            print(proof)
            if proof.type == "REVOKE":
                resource.revoked = True
                resource.revoke_id = proof.grid
                kwargs.get('session').add(resource)
                revoke = api_models.GlobalID.get_or_create(kwargs.get('session'), gid = proof.grid)
                new_revoke_relation = api_models.Relation(
                    parent_resource_id = proof.grid,
                    child_resource_id = protobuf.payload.grid,
                    type = "REVOKE",
                    grid_id = f"{protobuf.payload.grid}[REVOKE]"
                )
                kwargs.get('session').add(new_revoke_relation)
                continue
            stmt = select(chain_models.MerkleNode).where(chain_models.MerkleNode.value == proof.hashes[0])
            root_result = kwargs.get('session').scalars(stmt).all()

            if len(root_result) != -1:
                #print"New proof!")
                prefix = b''
                resource_proof = proof.hashes[0]
                proof_pair = list(batched(proof.hashes[1:-1], 2))
                for pair in proof_pair:
                    print("-------------------------------")
                    for value in pair:
                        print(value.hex())
                new_hash = None
                for pair in proof_pair:
                    m = hashlib.sha256()
                    m.update(pair[0]+pair[1])
                    new_hash = m.digest()
                    #printf"BU-{new_hash.hex()}")

                    stmt = select(chain_models.MerkleNode).where(chain_models.MerkleNode.value == prefix+pair[0])
                    new_node_left = kwargs.get('session').scalars(stmt).first()
                    if new_node_left == None:
                        new_node_left = chain_models.MerkleNode(
                            value = prefix+pair[0],
                            type = "LEAF" if pair[0] == resource_proof else "NODE",
                            parent_id = prefix+new_hash
                        )

                    if pair[0] == resource_proof:
                        new_node_left.resource_id = protobuf.payload.grid
                        new_node_left.proof_for = proof.type
                        new_node_left.type = "LEAF"
                        new_node_left.block = proof.block
                    kwargs.get('session').add(new_node_left)    
                    #printnew_node_left.value.hex())


                    stmt = select(chain_models.MerkleNode).where(chain_models.MerkleNode.value == prefix+pair[1])
                    new_node_right = kwargs.get('session').scalars(stmt).first()
                    if new_node_right == None:
                        new_node_right = chain_models.MerkleNode(
                            value = prefix+pair[1],
                            type = "LEAF" if pair[1] == resource_proof else "NODE",
                            parent_id = prefix+new_hash
                        )
 
                        
                    if pair[1] == resource_proof:
                        new_node_right.resource_id = protobuf.payload.grid
                        new_node_right.type == "LEAF"
                        new_node_right.proof_for = proof.type
                        new_node_right.block = proof.block
                    kwargs.get('session').add(new_node_right)
                    #printnew_node_right.value.hex())



                stmt = select(chain_models.MerkleNode).where(chain_models.MerkleNode.value == prefix+new_hash)
                new_root = kwargs.get('session').scalars(stmt).first()
                #printnew_hash.hex())
                print(proof)
                print(proof.block)
                print("HONHONHON")

                if new_root == None:
                    new_root = chain_models.MerkleNode(
                        value = prefix+new_hash,
                        type = f"{proof.type}",
                        parent_id = None,
                        resource_id = None,
                        block = proof.block
                    )
                kwargs.get('session').add(new_root)



 