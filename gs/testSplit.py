import unittest

from gs.split import MySplit

class TestDict(unittest.TestCase):

    def test_chackPair(self):
        ms = MySplit("20280129")
        with open('./testPairFile/test.sql', 'r') as fr:
            for line in fr:
                header,body =  ms.getHeader(line)
                self.assertTrue(ms.checkPair(header,body))

    def test_chackPairWrong(self):
        ms = MySplit("20280129")
        with open('./testPairFile/singleTest.sql', 'r') as fr:
            for line in fr:
                header,body =  ms.getHeader(line)
                self.assertTrue(ms.checkPair(header,body))
    # def test_key(self):
    #     d = Dict()
    #     d['key'] = 'value'
    #     self.assertEquals(d.key, 'value')
    #
    # def test_attr(self):
    #     d = Dict()
    #     d.key = 'value'
    #     self.assertTrue('key' in d)
    #     self.assertEquals(d['key'], 'value')
    #
    # def test_keyerror(self):
    #     d = Dict()
    #     with self.assertRaises(KeyError):
    #         value = d['empty']
    #
    # def test_attrerror(self):
    #     d = Dict()
    #     with self.assertRaises(AttributeError):
    #         value = d.empty