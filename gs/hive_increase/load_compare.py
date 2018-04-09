#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys,os,json

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
        # self.csvpath = "/mnt2/gs/gs_split/%s" % date
        # self.hqlpath = "/mnt2/gs/gs_check/%s/compare" % date
        self.csvpath = "../%s" % date
        self.hqlpath = "../%s" % date
        if not os.path.exists(self.hqlpath):
            os.mkdir(self.hqlpath)

    def compare_data(self,num):
        # for table in self.common_tab + self.company_tab + self.report_s_conn_tab + self.report_conn_tab:
        for table in ["company_change_info"]:
            try:
                self.diff_info[table] = {}
                os.system("/usr/bin/hive -e 'select * from prism_%s.%s_update order by rand() limit %d;' > %s/%s_hive_get.txt"%(self.date,table,num,self.hqlpath,table))
                fhc = open("%s/%s_hive_get.txt" % (self.hqlpath, table), 'r').readlines()
                flc = open("%s/%s_update_u.csv" % (self.csvpath, table), 'r').readlines()
                flc = sorted(flc, self.sort_func)
                fhc = sorted(fhc, self.sort_func)
                fcompare = open("%s/%s_compare.txt" % (self.hqlpath, table), 'a')
                hindex,lindex = 0,0
                while lindex < len(flc) and hindex < len(fhc):
                    lcols = flc[lindex].split(self.__FIELD_TERMINATER)
                    hcols = fhc[hindex].split(self.__FIELD_TERMINATER)
                    if not lcols[0] == hcols[0]:
                        lindex += 1
                        continue
                    else:
                        if len(lcols) != len(hcols):
                            self.diff_info[table].update({[lcols[0]]:{"cols":[-1],"hive":hcols,"local":lcols}})
                            continue
                        for i in range(0,len(lcols)):
                            if not (lcols[i] == hcols[i] or lcols[i].upper() == hcols[i]):
                                if self.diff_info[table].has_key(lcols[0]):
                                    self.diff_info[table][lcols[0]]["cols"].append(i)
                                else:
                                    self.diff_info[table][lcols[0]] = {"cols": [i], "hive": hcols, "local": lcols}
                        hindex += 1
                        lindex += 1
                fcompare.write(json.dumps(self.diff_info)+'\n')
            except Exception,e:
                print e

    def sort_func(self,x,y):
        xitems = x.split(self.__FIELD_TERMINATER)
        yitems = y.split(self.__FIELD_TERMINATER)
        if int(xitems[0]) < int(yitems[0]):
            return -1
        if int(xitems[0]) == int(yitems[0]):
            return 0
        else:
            return 1
if __name__ == "__main__":
    lp = LoadHiveCompare("20181")
    lp.compare_data(1000)