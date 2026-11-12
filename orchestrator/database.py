# app/database.py
from sqlmodel import Session, create_engine

sqlite_url = "sqlite:///./database.db"
engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})

def get_session():
    with Session(engine) as session:
        yield session  # FastAPI will inject this session and close it automatically
