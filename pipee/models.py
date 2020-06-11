from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class OpLog(Base):
    __tablename__ = 'pipeline_oplog'

    id = Column(Integer, primary_key=True)
    pipeline_id = Column(Integer, ForeignKey("pipee.id", ondelete="CASCADE"))
    message = Column(Text)
    time = Column(DateTime)
    log_type = Column(String(20))


class Pipeline(Base):
    __tablename__ = 'pipee'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    key = Column('key', String(400), index=True, unique=True)
    stage = Column('stage', String(400))
    message = Column('message', Text)
    context = Column('context', Text)
    created_time = Column('created_time', DateTime)
    modified_time = Column('modified_time', DateTime)
    modifier = Column('modifier', String(100))
