rm *.db
rm alembic/versions/*.py
alembic revision --autogenerate -m "description"
alembic upgrade head