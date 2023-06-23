from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import Base, Node

# You might want to move the db_path to a config file
db_path = 'sqlite:///azure_spot_balancer/db/nodes.db'
engine = create_engine(db_path)

Session = sessionmaker(bind=engine)

def create_tables():
    Base.metadata.create_all(engine)

def add_node(node):
    session = Session()
    session.add(node)
    session.commit()
    session.close()

# Add other utility functions here as needed for fetching, updating, and deleting records
