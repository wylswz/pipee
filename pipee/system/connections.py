import copy
import os

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine.url import make_url

from pipee.models import Base


class Connection:
    engine: Engine = None

    @classmethod
    def _db_exists(cls, db_name: str) -> bool:
        insp: Inspector = inspect(cls.engine)
        dbs = insp.get_schema_names()
        return str in dbs

    @classmethod
    def _get_db_server_address(cls):
        """
        
        """
        address = cls._get_db_address()
        url = make_url(address)
        server_url = copy.copy(url)
        server_url.database = None
        return server_url

    @classmethod
    def _get_db_address(cls):
        return make_url(os.environ.get("PIPELINE_DB_ADDRESS"))

    @classmethod
    def _build_engine(cls):
        """
        """
        server_addr = str(cls._get_db_server_address())
        db_name = str(cls._get_db_address().database)
        if cls.engine is None:
            cls.engine = create_engine(server_addr)
            if cls._db_exists(db_name):
                cls.engine = create_engine(cls._get_db_address())
            else:
                driver = cls._get_db_address().drivername
                if driver in ['mysql']:
                    cls.engine.execute("CREATE DATABASE IF NOT EXISTS {0}".format(db_name))
                    cls.engine = create_engine(cls._get_db_address())
                elif driver in ['sqlite']:
                    pass

    @classmethod
    def get_engine(cls):
        if cls.engine is None:
            cls._build_engine()
        return cls.engine

    @classmethod
    def init_db(cls):
        Base.metadata.create_all(cls.engine)
