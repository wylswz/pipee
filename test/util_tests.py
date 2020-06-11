import unittest

from pipee.utils import kw_retval, is_kwargs




class TestUtils(unittest.TestCase):

    def test_kw_retval(self):
        self.assertTrue(is_kwargs(kw_retval(a=1,b=2)))


if __name__ =='__main__':
    unittest.main()