from dotenv import load_dotenv
load_dotenv("test/test.env")

import unittest
import os
from pipee.models import PipelineEntry as Ppl
from pipee.dao import PipelineDAO
from pipee.system.connections import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from datetime import datetime

from pydantic import BaseModel
from typing import List




class DaoTestCase(unittest.TestCase):
    def setUp(self):
        self.ppl1 = {
            'key':"key1",
            'stage':"stage1",
            'message':"this is a message",
            'context':'{}',
            'created_time':datetime(2020,3,5),
            'modified_time':datetime(2020,3,6),
            'modifier':"test"
        }

        self.ppl2 = {
            'key':"key2",
            'stage':"stage2",
            'message':"this is a mesdsage",
            'context':'{}',
            'created_time':datetime(2020,3,5),
            'modified_time':datetime(2020,3,6),
            'modifier':"test"
        }


        


class TestPipelineDAO(DaoTestCase):
    def test_env(self):
        self.assertEqual("sqlite://", os.environ.get("PIPELINE_DB_ADDRESS"))
    def test_dao_conn(self):
        self.assertEqual('sqlite://', str(Connection._get_db_address()))
        self.assertEqual('sqlite://', str(Connection._get_db_server_address()))
        
        engine:Engine = Connection.get_engine()
        Connection.init_db()

        # Assert table existance
        self.assertTrue(engine.has_table("pipee"))
        self.assertTrue(engine.has_table("pipeline_oplog"))

    def test_pipeline_dao_logic(self):
        # Test duplicate insertion
        PipelineDAO.insert_pipeline(Ppl(**self.ppl1))
        with self.assertRaises(IntegrityError):
            PipelineDAO.insert_pipeline(Ppl(**self.ppl1))

        # Test deletion
        PipelineDAO.delete_pipeline_by_key('key1')
        PipelineDAO.delete_pipeline_by_key('key1')
        self.assertIsNone(PipelineDAO.get_pipeline_by_key('key1'))
        PipelineDAO.insert_pipeline(Ppl(**self.ppl1))
        PipelineDAO.insert_pipeline(Ppl(**self.ppl2))
        id = PipelineDAO.get_pipeline_by_key('key2').id
        PipelineDAO.delete_pipeline(id)
        self.assertIsNone(PipelineDAO.get_pipeline(id))







if __name__ == '__main__':
    unittest.main()