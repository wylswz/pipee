from sqlalchemy.orm import sessionmaker

from pipee.models import Pipeline
from pipee.system.connections import Connection

engine = Connection.get_engine()
Connection.init_db()
Session = sessionmaker(bind=engine)


class PipelineDAO:

    @classmethod
    def insert_pipeline(cls, ppl: Pipeline):
        session = Session()
        session.add(ppl)
        session.commit()

    @classmethod
    def update_pipeline(cls, ppl: Pipeline):
        pass

    @classmethod
    def delete_pipeline(cls, id: int):
        session = Session()
        session.query(Pipeline).filter(Pipeline.id == id).delete(synchronize_session='fetch')

    @classmethod
    def delete_pipeline_by_key(cls, key: str):
        session = Session()
        session.query(Pipeline).filter(Pipeline.key.like(key)).delete(synchronize_session='fetch')

    @classmethod
    def get_pipeline(cls, id: int) -> Pipeline:
        session = Session()
        return session.query(Pipeline).filter(Pipeline.id == id).one_or_none()

    @classmethod
    def get_pipeline_by_key(cls, key: str) -> Pipeline:
        # Return None if not found
        session = Session()
        return session.query(Pipeline).filter(Pipeline.key.like(key)).one_or_none()


class OplogDAO:

    @classmethod
    def insert_oplog(cls, OpLog):
        pass

    @classmethod
    def get_oplog(cls, ppl_id):
        pass
