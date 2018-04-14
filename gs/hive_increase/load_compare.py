#!/usr/bin/python
#encoding:utf8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os,json

class LoadHiveCompare:
    common_tab = ["human", "company"]
    company_tab = ["annual_report", "company_abnormal_info", "company_change_info",
                   "company_check_info", "company_equity_info", "company_illegal_info",
                   "company_investor", "company_mortgage_info", "company_punishment_info", "company_staff",
                   "mortgage_change_info", "mortgage_pawn_info", "mortgage_people_info"]
    report_conn_tab = ["report_change_record", "report_equity_change_info",
                       "report_out_guarantee_info", "report_webinfo"]
    report_s_conn_tab = ["report_outbound_investment", "report_shareholder"]
    __FIELD_TERMINATER = "\t"
    diff_info = {"tablename":{"right":"yes","id":{"cols":[],"hive":"line","local":"line"}}}
    def __init__(self,date):
        self.date = date
        self.csvpath = "/mnt2/gs/gs_split/%s" % date
        self.hqlpath = "/mnt2/gs/gs_check/%s/compare" % date
        # self.csvpath = "../%s" % date
        # self.hqlpath = "../%s" % date
        if not os.path.exists(self.hqlpath):
            os.mkdir(self.hqlpath)

    def compare_data(self,num):
        for table in self.common_tab + self.company_tab + self.report_s_conn_tab + self.report_conn_tab:
        # for table in ["company_change_info"]:
            try:
                hive_info = {}
                self.diff_info[table] = {}
                os.system("/usr/bin/hive -e 'select * from prism_%s.%s_update order by rand() limit %d;' > %s/%s_hive_get.txt"%(self.date,table,num,self.hqlpath,table))
                fhc = open("%s/%s_hive_get.txt" % (self.hqlpath, table), 'r').readlines()
                for hcline in fhc:
                    if hcline == "":continue
                    hitems = hcline.split(self.__FIELD_TERMINATER)
                    hive_info[hitems[0]] = hitems

                fcompare = open("%s/%s_compare.json" % (self.hqlpath, table), 'a')
                with open("%s/%s_update_u.csv" % (self.csvpath, table), 'r') as flc:
                    for lcline in flc:
                        if lcline == "":continue
                        litems = lcline.split(self.__FIELD_TERMINATER)
                        if not hive_info.has_key(litems[0]):continue
                        hitems = hive_info[litems[0]]
                        if len(litems) != len(hitems):
                            self.diff_info[table].update({litems[0]:{"cols":[-1],"hive":hitems,"local":litems}})
                            continue
                        for i in range(0,len(litems)):
                            if not (litems[i] == hitems[i] or litems[i].upper() == hitems[i]):
                                if self.diff_info[table].has_key(litems[0]):
                                    self.diff_info[table][litems[0]]["cols"].append(i)
                                else:
                                    self.diff_info[table][litems[0]] = {"cols": [i], "hive": hitems, "local": litems}
                fcompare.write(json.dumps(self.diff_info)+'\n')
            except Exception,e:
                print e

if __name__ == "__main__":
    # lp = LoadHiveCompare("20181")
    lp = LoadHiveCompare(sys.argv[1])
    lp.compare_data(1000)