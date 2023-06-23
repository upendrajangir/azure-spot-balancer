from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class NodePool(Base):
    __tablename__ = "nodepools"
    name = Column(String, nullable=False, unique=True, primary_key=True)
    type = Column(String, nullable=False)  # e.g. "spot" or "ondemand"
    region = Column(String, nullable=False)
    family = Column(String, nullable=False)
    current_node_count = Column(Integer, nullable=False)
    min_node_count = Column(Integer, nullable=False)
    max_node_count = Column(Integer, nullable=False)


class Node(Base):
    __tablename__ = "nodes"
    name = Column(String, nullable=False, unique=True, primary_key=True)
    status = Column(String, nullable=False)  # e.g. "running", "evicted", etc.
    nodepool_id = Column(Integer, ForeignKey("nodepools.name"))
    vm_size = Column(String, nullable=False)
    is_spot = Column(Boolean, nullable=False)
    eviction_time = Column(String, nullable=True)
    current_workload_count = Column(Integer, nullable=False)
    nodepool = relationship("NodePool", back_populates="nodes")


class Workload(Base):
    __tablename__ = "workloads"
    name = Column(String, nullable=False, unique=True, primary_key=True)
    status = Column(String, nullable=False)  # e.g. "running", "pending", etc.
    deployment_name = Column(String, nullable=False)
    namespace = Column(String, nullable=False)
    cpu_limit = Column(Integer, nullable=False)
    memory_limit = Column(Integer, nullable=False)
    graceful_termination_period = Column(Boolean, nullable=False)
    node_id = Column(Integer, ForeignKey("nodes.name"))
    node = relationship("Node", back_populates="workloads")
