# -*- coding:utf-8 -*-
import unittest,datetime

from gs.hive_increase.load_hive import LoadHive

class TestLoadHive(unittest.TestCase):

    def test_get_id(self):
        pass
        # lh = LoadHive("20180322")
        # lh.load_data("prism_20180131")

    def gen_yesterday(self):
        pass


today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
print yesterday.strftime('%Y%m%d')

today2 = "20180301"
print int(today2[0:4]),int(today2[4:6]),int(today2[6:8])
yesterday2 =datetime.datetime(int(today2[0:4]),int(today2[4:6]),int(today2[6:8])) - datetime.timedelta(days=1)
print yesterday2.strftime('%Y%m%d')
print 'ok2'