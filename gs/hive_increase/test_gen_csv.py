# -*- coding:utf-8 -*-
import unittest

from gs.hive_increase.gen_csv import MyGenCSV

class TestGenCsv2(unittest.TestCase):
    def setUp(self):
        self.gcsv = MyGenCSV('2018')
        ##实例化了被测试模块中的类

    def skipTest(self, reason):
        print 'ok'

    def test_StripSpaceAndBracket(self):
        str = "(id,name );"
        print " id ".strip()
        print len(self.gcsv.stripSpaceAndBracket(str).split(","))
        print self.gcsv.stripSpaceAndBracket(str).split(",")
        print str

    def testCheckEscapePair(self):
        with open("./test.txt") as fr:
            for line in fr:
                items = line.split(',')
                estr = repr(items[8]).strip('"')
                nstr = repr(items[9])
                print items[8],estr,len(estr) ,len(repr(items[8]))
                print items[9],nstr,len(nstr),len(items[9])
                print items[11]
                if items[8].endswith("\\'"):
                    print '8 ok'
                if items[9].endswith("\\'"):
                    print '9 ok'
                self.assertFalse(self.gcsv.checkEscapeOdd(items[8]))
                self.assertTrue(self.gcsv.checkEscapeOdd(items[9]))
                self.assertTrue(self.gcsv.checkEscapeOdd(items[10]))
                self.assertTrue(self.gcsv.checkEscapeOdd(items[11]))
                self.assertFalse(self.gcsv.checkEscapeOdd(items[12]))
                # self.gcsv.checkEscapeOdd(items[8])
                # self.gcsv.checkEscapeOdd(items[9])
                # self.gcsv.checkEscapeOdd(items[10])
                # self.gcsv.checkEscapeOdd(items[11])
                # self.gcsv.checkEscapeOdd(items[12])
                # import re
                # find_escape_reg = re.compile(r".*?(\\+)'$", re.M | re.I | re.S)
                # g1 = find_escape_reg.match(items[8]).group(1)
                # print 'find group:',g1,len(g1)
                # g9 = find_escape_reg.match(items[9]).group(1)
                # print 'find group:', g9, len(g9)
                # g10 = find_escape_reg.match(items[10]).group(1)
                # print 'find group:', g10, len(g10)