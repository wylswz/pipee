from sqlalchemy.orm import sessionmaker

from pipee.models import PipelineEntry
from pipee.system import Connection

engine = Connection.get_engine()
Connection.init_db()
Session = sessionmaker(bind=engine)


class PipelineDAO:

    @classmethod
    def insert_pipeline(cls, ppl: PipelineEntry):
        session = Session()
        session.add(ppl)
        session.commit()

    @classmethod
    def update_pipeline(cls, ppl: PipelineEntry):
        pass

    @classmethod
    def delete_pipeline(cls, id: int):
        session = Session()
        session.query(PipelineEntry).filter(PipelineEntry.id == id).delete(synchronize_session='fetch')

    @classmethod
    def delete_pipeline_by_key(cls, key: str):
        session = Session()
        session.query(PipelineEntry).filter(PipelineEntry.key.like(key)).delete(synchronize_session='fetch')

    @classmethod
    def get_pipeline(cls, id: int) -> PipelineEntry:
        session = Session()
        return session.query(PipelineEntry).filter(PipelineEntry.id == id).one_or_none()

    @classmethod
    def get_pipeline_by_key(cls, key: str) -> PipelineEntry:
        # Return None if not found
        session = Session()
        return session.query(PipelineEntry).filter(PipelineEntry.key.like(key)).one_or_none()


class OplogDAO:

    @classmethod
    def insert_oplog(cls, OpLog):
        pass

    @classmethod
    def get_oplog(cls, ppl_id):
        pass
