from sqlalchemy.orm import sessionmaker

from pipee.models import PipelineEntry
from pipee.system import Connection
from pipee.system import settings




class PipelineDAO:

    Session = None
    session=None
    if settings.PERSISTENCE_ENABLED:
        engine = Connection.get_engine()
        Connection.init_db()
        Session = sessionmaker(bind=engine)
        session=Session()

    @classmethod
    def insert_pipeline(cls, ppl: PipelineEntry):
        #session = Session()
        cls.session.add(ppl)
        cls.session.commit()

    @classmethod
    def update_pipeline(cls, key, fields):
        #session = Session()
        cls.session.query(PipelineEntry).filter(PipelineEntry.key.like(key)).update(fields,synchronize_session=False)
        cls.session.commit()

    @classmethod
    def delete_pipeline(cls, id: int):
        #session = Session()
        cls.session.query(PipelineEntry).filter(PipelineEntry.id == id).delete(synchronize_session='fetch')
        cls.session.commit()

    @classmethod
    def delete_pipeline_by_key(cls, key: str):
        #session = Session()
        print("delete")
        cls.session.query(PipelineEntry).filter(PipelineEntry.key.like(key)).delete(synchronize_session='fetch')
        cls.session.commit()

        
    @classmethod
    def get_pipeline(cls, id: int) -> PipelineEntry:
        #session = Session()
        return cls.session.query(PipelineEntry).filter(PipelineEntry.id == id).one_or_none()

    @classmethod
    def get_pipeline_by_key(cls, key: str) -> PipelineEntry:
        # Return None if not found
        #session = Session()
        return cls.session.query(PipelineEntry).filter(PipelineEntry.key.like(key)).one_or_none()


class OplogDAO:

    @classmethod
    def insert_oplog(cls, OpLog):
        pass

    @classmethod
    def get_oplog(cls, ppl_id):
        pass
