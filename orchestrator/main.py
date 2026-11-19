from fastapi import FastAPI, Depends
import api.models as api_models
import crypt.models as crypt_models
from sqlmodel import Field, SQLModel, select
from database import get_session
from sqlalchemy import text, select
import api.message_models as message_models
import anchors.views as anchor_views
import api.handler_mapping as handler_mapping
import api.exceptions as api_exceptions
from api.utils import resource_utils
from contextlib import asynccontextmanager
import asyncio
from fastapi import APIRouter
import api.utils.entity_utils as entity_utils
import time
# Include the routers into the main app object
tcp_writer = None
tcp_reader = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Open the persistent TCP connection
    global tcp_reader, tcp_writer
    tcp_reader, tcp_writer = await asyncio.open_connection('127.0.0.1', 8001)
    #print"Persistent TCP connection established.")
    await read_root()
    yield  # FastAPI runs your application here
    
    # Shutdown: Close the TCP connection
    tcp_writer.close()
    await tcp_writer.wait_closed()
    #print"Persistent TCP connection closed.")
app = FastAPI(lifespan=lifespan)
app.include_router(anchor_views.router, prefix="/api/anchors", tags=["Users"])


@app.get("/api")
@app.post("/api")
def read_root(data:message_models.APIRequest, session: Session = Depends(get_session)):
    result = []
    auth = None
    session.execute(text("PRAGMA case_sensitive_like = ON;"))
    if 'entity' in data.auth:
        if 'key' in data.auth:
            signing_key = entity_utils.get_entity_key(data.auth['entity'], 'SIGN', grid = data.auth['key'], session = session)
        else:
            signing_key = entity_utils.get_entity_key(data.auth['entity'], 'SIGN', session = session)
        signing_key.load_values()
        auth = {
            "entity":resource_utils.get_by_gid(data.auth['entity'], model_only = True, session = session),
            "keys":{
                "sign":signing_key
            }
        }
    for i in range(0, data.loop):
        for message in data.messages:

            print("Waiting...")
            time.sleep(message.delay)
            print("started")
            if message.type in handler_mapping.handler_map:
                handler = handler_mapping.handler_map[message.type][0](message.value, session = session, auth = auth)
                result.append(handler.result)
                if handler.commit:
                    session.commit()
                    session.expunge_all()


    return {"messages":result}






@app.get("/read")
async def read_root():
    # Use the global tcp_writer/tcp_reader in your endpoints
    if tcp_writer:
        tcp_writer.write(b"ping")
        await tcp_writer.drain()
    return {"status": "sent data over TCP"}

@app.post("/push")
async def send_data():
    if tcp_writer:
        tcp_writer.write("ping".encode())
        await tcp_writer.drain()
    return {"status": "success"}