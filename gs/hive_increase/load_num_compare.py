#!/usr/bin/python
#encoding:utf8
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

import os,json,re

class LoadHiveNumCompare:
    common_tab = ["human", "company"]
    company_tab = ["annual_report", "company_abnormal_info", "company_change_info",
                   "company_check_info", "company_equity_info", "company_illegal_info",
                   "company_investor", "company_mortgage_info", "company_punishment_info", "company_staff",
                   "mortgage_change_info", "mortgage_pawn_info", "mortgage_people_info"]
    report_conn_tab = ["report_change_record", "report_equity_change_info",
                       "report_out_guarantee_info", "report_webinfo"]
    report_s_conn_tab = ["report_outbound_investment", "report_shareholder"]
    analytic_info = {'tb':
                         {"update":
                              {'equal': 'yes',
                               'h_update_count': 10,
                               'csv_update_count': 10},
                          "delete": {
                              'equal': 'yes',
                              'h_delete_count': 10,
                              'csv_delete_count': 10}
                          }
                     }
    def __init__(self,date):
        # self.hqlpath = "/mnt2/gs/gs_check/%s" % date
        # self.csvpath = "/mnt2/gs/gs_split/%s" % date
        self.hqlpath = "../%s" % date
        self.csvpath = "../%s" % date
        self.hivenuminfo = {}
        self.init_hive_num()

    def init_hive_num(self):
        numdata = "%s/hivenum.txt" % self.hqlpath
        if not os.path.exists(numdata):
            print("hive num info missed")
            exit(-2)
        with open(numdata,'r') as fr:
            for line in fr.readlines():
                if line == "":
                    continue
                line = line.lstrip('[').rstrip(']\n')
                items = line.split(',')
                if items < 2:
                    continue
                self.hivenuminfo[items[0]] = items[1]

    def hive_check_num(self):
        for tb in self.common_tab + self.company_tab + self.report_s_conn_tab + self.report_conn_tab:
            self.analytic_info[tb] = {}
            if self.hivenuminfo.has_key("%s_update" % tb):
                h_update_res = self.hivenuminfo["%s_update" % tb]
            else:
                h_update_res = -1

            c_update_num = self.get_file_count("%s/%s_update_u.csv" % (self.csvpath, tb))

            self.analytic_info[tb]["update"] = {"h_update_count": h_update_res,
                                                 "csv_update_count": str(c_update_num)}
            if int(h_update_res) == c_update_num:
                self.analytic_info[tb]["update"].update({"equal": "yes"})
            else:
                self.analytic_info[tb]["update"].update({"equal": "no"})

            if self.hivenuminfo.has_key("%s_delete" % tb):
                h_delete_res = self.hivenuminfo["%s_delete" % tb]
            else:
                h_delete_res = -1
            w_delete_num = self.get_file_count("%s/%s_delete.csv" % (self.csvpath, tb))
            self.analytic_info[tb]["delete"] = {"h_delete_count": h_delete_res,
                                                "csv_delete_count": str(w_delete_num)}
            if int(h_delete_res) == w_delete_num:
                self.analytic_info[tb]["delete"].update({"equal": "yes"})
            else:
                self.analytic_info[tb]["delete"].update({"equal": "no"})
            # except Exception,e:
            #     print e
        with open("%s/compare_num.json" % self.hqlpath, 'a') as fw:
            fw.write(json.dumps(self.analytic_info))
        print self.analytic_info



    def get_file_count(self, file):
        if not os.path.exists(file):
            return -1
        count = 0
        thefile = open(file, 'rb')
        while True:
            buffer = thefile.read(8192 * 1024)
            if not buffer:
                break
            count += buffer.count('\n')
        thefile.close()
        return count

if __name__ == "__main__":
    lc = LoadHiveNumCompare("20181")
    # lc = LoadHiveNumCompare(sys.argv[1])
    lc.hive_check_num()