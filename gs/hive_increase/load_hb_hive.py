#!/usr/bin/python
#encoding:utf8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os,datetime,json,re

class LoadHive:
    #
    common_tab = ["human", "company"]
    company_tab = ["annual_report","company_abnormal_info","company_change_info","company_category_20170411","company_category_code_20170411",
                   "company_check_info","company_equity_info", "company_illegal_info",
                   "company_investor", "company_mortgage_info", "company_punishment_info", "company_staff",
                   "mortgage_change_info", "mortgage_pawn_info", "mortgage_people_info"]
    report_conn_tab = ["report_change_record","report_equity_change_info",
                       "report_out_guarantee_info","report_webinfo"]
    report_s_conn_tab = ["report_outbound_investment","report_shareholder"]
    # common_tab = []
    # company_tab = []
    # report_conn_tab = []
    # report_s_conn_tab = []
    analytic_info = {'tb':
                         {"update":
                              {'equal':'yes',
                               'h_update_count':10,
                               'csv_update_count':10},
                          "delete":{
                              'equal': 'yes',
                               'h_delete_count':10,
                               'csv_delete_count':10}
                          }
                     }
    def __init__(self,date):
        self.date = date
        self.csvpath = "/mnt2/gs/gs_split/%s" % date
        # self.hqlpath = "/mnt2/gs/gs_check/%s" % date
        self.hqlpath = "../%s" % date
        if not os.path.exists(self.hqlpath):
            os.mkdir(self.hqlpath)
        self.hqldata = "%s/hqload.txt"%self.hqlpath
        self.hqcdata = "%s/hqcopy.txt"%self.hqlpath
        self.dbtoday = "prism_%s" % self.date

    def load_data(self,dbpre="prism_20180611",dborigin="prism_hb",dbdel="prism_delete"):
        self.clean_command_file([self.hqldata,self.hqcdata])
        h_load_task = open(self.hqldata, "a")
        h_copy_task = open(self.hqcdata, "a")

        h_load_task.write('create database if not exists %s;\n\n' % self.dbtoday)
        h_load_task.write("create table %s.company_update_id like %s.company_update_id;\n\n" % (self.dbtoday,dbpre))
        h_load_task.write("load data local inpath \"%s/comp_conn_id.csv\" into table %s.company_update_id;\n\n" % (self.csvpath, self.dbtoday))

        for tb in self.common_tab + self.company_tab + self.report_s_conn_tab + self.report_conn_tab:
            upath = "%s/%s_update.csv" % (self.csvpath, tb)
            h_load_task.write("create table %s.%s_update like %s.%s;\n\n" % (self.dbtoday,tb,dbpre,tb))
            h_load_task.write("load data local inpath \"%s\" into table %s.%s_update;\n\n" % (upath,self.dbtoday,tb))

            if tb in self.common_tab:
                idname = "id"
            elif tb in self.company_tab:
                idname = "company_id"
            elif tb in self.report_conn_tab:
                idname = "annualreport_id"
            elif tb in self.report_s_conn_tab:
                idname = "annual_report_id"

            h_copy_task.write("insert into %s.%s select * from  %s.%s_update;\n\n" % (dborigin, tb, self.dbtoday, tb))
            if tb not in self.common_tab:
                h_copy_task.write("insert into %s.%s_delete select concat(%s,'_',id) as cid,id from  %s.%s_update;\n\n" % (dbdel, tb, idname,self.dbtoday, tb))

        h_copy_task.write("set hive.mapred.mode=nonstrict;use %s;create table company_update_all as select * from company as a where a.id in (select b.id from company_update_id as b);\n\n" % self.dbtoday)
        h_copy_task.close()
        h_load_task.close()
        # os.system("/usr/bin/hive -f %s" % self.hqldata)
        # os.system("/usr/bin/hive -f %s" % self.hqcdata)



    def clean_command_file(self,filenames):
        for file in filenames:
            if os.path.exists(file):
                os.remove(file)

if __name__ == "__main__":
    # if len(sys.argv) == 1:
    #     print "parameter wrong!"
    #     exit(1)
    # elif len(sys.argv) == 2 and len(sys.argv[1])== 8:
    #     today = sys.argv[1]
    #     yesterday = datetime.datetime(int(today[0:4]), int(today[4:6]), int(today[6:8])) - datetime.timedelta(days=1)
    #     yesterday = yesterday.strftime('%Y%m%d')
    # elif len(sys.argv) == 3:
    #     today = sys.argv[1]
    #     yesterday = sys.argv[2]
    # else:
    #     print "parameter wrong!"
    #     exit(1)
    lh = LoadHive("20180324")
    # lh.load_data("prism_%s"%yesterday)
    lh.load_data()