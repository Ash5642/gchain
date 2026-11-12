import socket, data_pb2, struct, hashlib, secrets, string
import bchain.models as bchain_models
import abc
from sqlmodel import Field, SQLModel, select, update
import api.models as api_models
import bchain.utils.resource_management as resource_management
import api.utils.resource_utils as resource_utils
def send_command(message_type, *args, **kwargs):
    target_host = "127.0.0.1"
    target_port = 8001
    message_protobuf = data_pb2.HostMessage(
        type = message_type
    )
    if 'message' in kwargs:
        message_protobuf.message = kwargs['message'].encode()
    raw_payload = message_protobuf.SerializeToString()
    msg_len = len(raw_payload)
    packed_len = struct.pack('!I', msg_len)


    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(("127.0.0.1", 8001))
    client_socket.sendall(packed_len+raw_payload)
    if message_type in ['REQUEST', 'STATUS', "REQUESTS"]:
        #print"READING");
        raw_length = client_socket.recv(4)
        if not raw_length:
            return None
        
        message_length = struct.unpack('!I', raw_length)[0]
        if message_length == 0:
            #print"Not found")
            return None
        data = b""
        while len(data) < message_length:
            packet = client_socket.recv(message_length - len(data))
            if not packet:
                raise ConnectionError("Socket closed before full message received")
            data += packet
        
        #printdata.decode('utf-8'))
        message = data_pb2.HostMessage()
        message.ParseFromString(bytes.fromhex(data.decode('utf-8')))

        if message_type == "REQUEST":
            resource = data_pb2.Resource()
            resource.ParseFromString(message.message)
            #printresource)
            for proof in resource.proof:
                resource_hash = proof.hashes[0]
                proof_prefix = (''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(4))).encode()
                for pos in range (1, len(proof.hashes[1:])-1, 2):
                    #printf"{proof.hashes[pos].hex()} {proof.hashes[pos+1].hex()}")
                    combined = bytes.fromhex(proof.hashes[pos].hex()+proof.hashes[pos+1].hex())
                    m = hashlib.sha256()
                    m.update(combined)
                    new_hash = m.digest()
                    #printnew_hash.hex())


                    try: 
                        kwargs.get('session').add(
                            bchain_models.MerkleNode(
                                value = proof_prefix + proof.hashes[pos],
                                type = "LEAF",
                                resource_id = resource.payload.grid if proof.hashes[pos] == resource_hash else None,
                                parent_id = proof_prefix + new_hash
                            )
                        )
                    except Exception as e:
                        pass

                    try:
                        kwargs.get('session').add(
                            bchain_models.MerkleNode(
                                value = proof_prefix + proof.hashes[pos+1],
                                type = "LEAF",
                                resource_id = resource.payload.grid if proof.hashes[pos+1] == resource_hash else None,
                                parent_id = proof_prefix + new_hash
                            )
                        )
                    except Exception as e:
                        pass

            

def request_resources(grid_list):
    request_message = data_pb2.ResourceRequest(
        grid = grid_list
    )

def push_resource(grid, *args, **kwargs):
    if grid:
        stmt = select(api_models.GlobalID).where(api_models.GlobalID.hash.is_not(None)).where(api_models.GlobalID.gid == grid)
        grid_results = kwargs.get('session').scalars(stmt).all()
    else:

        stmt_exp = select(
            api_models.GlobalID
        ).where(
            api_models.GlobalID.confirmed == False
        ).where(
            api_models.GlobalID.push_type == "CHAIN"
        )
        grid_results = kwargs.get('session').scalars(stmt_exp).all()[:512]
    #print"Pushing:")
    #printgrid_results)
    bulk_resources = data_pb2.BulkResources(
        resources = []
    )
    for grid_result in grid_results:
        print(f"Pushing: {grid_result.gid}")
        if grid_result.type in ["RECORD", "KEY"]:
            result = grid_result
        else:
            resource = resource_utils.get_packaged(grid_result, ["RELATED", 'PROOF'], session = kwargs.get('session'))
            result = resource[0]
        target_host = "127.0.0.1"
        target_port = 8001
        
        new_resource_protobuf = result.get_protobuf(False, session = kwargs.get('session'), skip_hash = True)
        bulk_resources.resources.append(new_resource_protobuf)
        result.pushed = True
        result.push_type = "CHAIN"
        kwargs.get('session').add(result)
    print(f"Pushing {len(grid_results)} resources")

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    enveloped_payload = data_pb2.HostMessage(
        type = "CMD:RESOURCES",
        success = True,
        message = bulk_resources.SerializeToString()
    )
    message = enveloped_payload.SerializeToString()
    msg_len = len(message)
    packed_len = struct.pack('!I', msg_len)
    #print"Message lenght: {}", len(enveloped_payload.SerializeToString()))
    client_socket.connect(("127.0.0.1", 8001))
    client_socket.sendall(packed_len+message)
    client_socket.close()
    print(f"{len(bulk_resources.resources)} resources sent!")
        
    return


class CoreMessage(abc.ABC):
    chain_message = None
    response_format = None
    request_message = None
    response = None
    session = None
    has_response = True
    client_socket = None
    type_name = "HOST_MESSAGE"
    def create_chain_message(self):
        self.chain_message = data_pb2.HostMessage(
            type = self.type_name,
            success = True,
            message = self.request_message.SerializeToString()
        )

    def send_message(self):
        raw_payload = self.chain_message.SerializeToString()
        #print"MSSGSS")
        #printself.chain_message)
        msg_len = len(raw_payload)
        packed_len = struct.pack('!I', msg_len)
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect(("127.0.0.1", 8001))
        #printlen(raw_payload))
        #printpacked_len)
        self.client_socket.sendall(packed_len+raw_payload)
        self.handle_response()
        self.process()

    def handle_response(self):
        if self.has_response:
            #print"READING");
            raw_length = self.client_socket.recv(4)
            if not raw_length:
                return None
            

            message_length = struct.unpack('!I', raw_length)[0]
            
            data = b""
            while len(data) < message_length:
                packet = self.client_socket.recv(message_length - len(data))
                if not packet:
                    raise ConnectionError("Socket closed before full message received")
                data += packet
            #printdata)
            self.response = self.response_format()
            self.response.ParseFromString(bytes.fromhex(data))
            
    
    def __init__(self, data, *args, **kwargs):
        self.session = kwargs.get('session')
        self.load(data)
        self.create_chain_message()
        return

    def load(self):
        return

    def process(self):
        return
        


class ResourceRequest(CoreMessage):
    has_response = True
    response_format = data_pb2.ResourceResponse
    type_name = "HOST:REQUEST"
    def load(self, data):
        if 'grid' in data:
            resource_grids = data['grid']
        else:
            stmt = select(
                api_models.GlobalID.gid
            ).where(
                api_models.GlobalID.pushed == True
            ).where(
                api_models.GlobalID.push_type == "CHAIN"
            ).where(
                api_models.GlobalID.confirmed == True
            )
            resource_grids = self.session.scalars(stmt).all()
        #printf"Pulling {len(resource_grids)} resources")
        self.request_message = data_pb2.ResourceRequest(
            grid = resource_grids
        )
        
    def process(self):
        for resource in self.response.resources:
            print(f"Processing {resource.payload.grid}")
            try:
                resource_management.Resource.save_protobuf(resource, session = self.session)
            except Exception as e:
                print(e)


class ConfirmCheck(CoreMessage):
    has_response = True
    response_format = data_pb2.GridResponse
    type_name = "HOST:CONFIRMED"
    def load(self, data):
        if 'grid' in data:
            self.request_message = data_pb2.ResourceRequest(
                grid = data['grid']
            )
        else:
            stmt = select(
                api_models.GlobalID.gid
            ).where(
                api_models.GlobalID.pushed == True
            ).where(
                api_models.GlobalID.push_type == "CHAIN"
            ).where(
                api_models.GlobalID.confirmed == False
            )
            resource_grids = self.session.scalars(stmt).all()
            self.request_message = data_pb2.ResourceRequest(
                grid = resource_grids
            )
            
    def process(self, *args, **kwargs):
        grid_table = []
        for resource in self.response.exists:
            print(
                f"\n{resource.grid}"
            )
            for status in resource.status:
                print(f"{status.status} in {status.block}")
                match status.status:
                    case "CHAIN":
                        grid_table.append(resource.grid)
                    case "REVOKE":
                        pass
                    case "MEMPOOL":
                        pass

        stmt = (
            update(api_models.GlobalID)
            .where(api_models.GlobalID.gid.in_(grid_table))
            .values(confirmed=True)
        )
        self.session.execute(stmt)
 
class Revoke(CoreMessage):
    has_response = True
    response_format = data_pb2.GridResponse
    type_name = "HOST:REVOKE"
    def load(self, data):
        self.request_message = data_pb2.ResourceRequest(
            grid = [data['grid']]
        )
  
            
    def process(self, *args, **kwargs):
        grid_table = []
        
 
