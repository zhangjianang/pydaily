#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys,os,datetime,json,re

class LoadHive:
    # "company_category_code_20170411",
    common_tab = ["human", "company"]
    company_tab = ["annual_report","company_abnormal_info","company_change_info",
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
        self.date=date
        self.csvpath = "/mnt2/gs/gs_split/%s" % date
        self.hqlpath = "/mnt2/gs/gs_check/%s" % date
        if not os.path.exists(self.hqlpath):
            os.mkdir(self.hqlpath)
        self.hqldata = "%s/hqload.txt"%self.hqlpath
        self.hqcdata = "%s/hqcopy.txt"%self.hqlpath
        self.dbtoday = "prism_%s" % self.date

    def load_data(self,dbpre="prism1"):
        self.clean_command_file([self.hqldata,self.hqcdata])
        h_load_task = open(self.hqldata, "a")
        h_copy_task = open(self.hqcdata, "a")

        h_load_task.write('create database %s;\n\n' % self.dbtoday)
        for tb in self.common_tab + self.company_tab + self.report_s_conn_tab + self.report_conn_tab:
            upath = "%s/%s_update_u.csv" % (self.csvpath, tb)
            h_load_task.write("create table %s.%s like %s.%s;\n\n" % (self.dbtoday,tb,dbpre,tb))
            h_load_task.write("create table %s.%s_update like %s.%s;\n\n" % (self.dbtoday,tb,dbpre,tb))
            h_load_task.write("load data local inpath \"%s\" into table %s.%s_update;\n\n" % (upath,self.dbtoday,tb))

            dpath = "%s/%s_delete.csv" % (self.csvpath, tb)

            if tb in self.common_tab:
                idname = "id"
            elif tb in self.company_tab:
                idname = "company_id"
            elif tb in self.report_conn_tab:
                idname = "annualreport_id"
            elif tb in self.report_s_conn_tab:
                idname = "annual_report_id"
            if self.common_tab.count(tb) == 0:
                h_load_task.write("create table %s.%s_delete like %s.%s_delete;\n\n" % (self.dbtoday,tb,dbpre,tb))
                h_load_task.write("load data local inpath \"%s\" into table %s.%s_delete;\n\n" % (dpath,self.dbtoday,tb))

                h_copy_task.write("set hive.mapred.mode=nonstrict; insert into %s.%s select * from  %s.%s as a where a.%s not in " \
                                  "(select b.%s from %s.%s_update as b union all select c.%s from %s.%s_delete as c);\n"
                                  % (self.dbtoday, tb, dbpre, tb, idname, idname, self.dbtoday, tb, idname, self.dbtoday,tb))
                h_copy_task.write("insert into %s.%s select * from  %s.%s_update;\n" % (self.dbtoday, tb, self.dbtoday, tb))

            else:
                h_copy_task.write("set hive.mapred.mode=nonstrict; insert into %s.%s select * from  %s.%s as a where a.%s not in " \
                                  "(select b.%s from %s.%s_update as b );\n"
                                  % (self.dbtoday, tb, dbpre, tb, idname, idname, self.dbtoday, tb))
                h_copy_task.write("insert into %s.%s select * from  %s.%s_update;\n" % (self.dbtoday, tb, self.dbtoday, tb))

        h_copy_task.close()
        h_load_task.close()
        os.system("/usr/bin/hive -f %s" % self.hqldata)
        os.system("/usr/bin/hive -f %s" % self.hqcdata)
        # self.hive_check_num()
        self.get_id_count("company")

    def hive_check_num(self):
        for tb in self.common_tab + self.company_tab + self.report_s_conn_tab + self.report_conn_tab:
            self.analytic_info[tb] = {}
            try:
                h_update_res = os.popen("/usr/bin/hive -e 'select count(*) from %s.%s_update;'"%(self.dbtoday,tb)).read().splitlines()[0]
                w_update_res = os.popen("wc -l %s/%s_update_u.csv"%(self.csvpath,tb)).read().splitlines()[0]
                self.analytic_info[tb]["update"] = {"h_update_count": h_update_res,
                                                     "csv_update_count": w_update_res.split()[0]}
                if int(h_update_res) == int(w_update_res.split()[0]):
                    self.analytic_info[tb]["update"].update({"equal": "yes"})
                else:
                    self.analytic_info[tb]["update"].update({"equal": "no"})

                if self.common_tab.count(tb) == 1:
                    idcount = self.get_id_count(tb)
                    self.analytic_info[tb]["update"].update({"src_id_count": idcount})
                    if self.analytic_info[tb]["update"]["equal"] == "yes":
                        if idcount != int(self.analytic_info[tb]["update"]["h_update_count"]):
                            self.analytic_info[tb]["update"]["equal"] = "no"
                else:
                    h_delete_res = \
                    os.popen("/usr/bin/hive -e 'select count(*) from %s.%s_delete;'" % (self.dbtoday, tb)).read().splitlines()[0]
                    w_delete_res = os.popen("wc -l %s/%s_delete.csv" % (self.csvpath, tb)).read().splitlines()[0]
                    self.analytic_info[tb]["delete"] = {"h_delete_count": h_delete_res,
                                                        "csv_delete_count": w_delete_res.split()[0]}
                    if int(h_delete_res) == int(w_delete_res.split()[0]):
                        self.analytic_info[tb]["delete"].update({"equal": "yes"})
                    else:
                        self.analytic_info[tb]["delete"].update({"equal": "no"})
            except Exception,e:
                print e
        with open("%s/compare_num.json" % self.hqlpath, 'a') as fw:
            fw.write(json.dumps(self.analytic_info))
        print self.analytic_info

    def get_id_count(self,tb):
        find_company_reg = re.compile(r"\(([0-9]+)\,", re.M | re.I | re.S)
        allid = []
        with open("%s/u%s.sql"%(self.csvpath,self.date)) as fr:
            for line in fr:
                if line.startswith("REPLACE INTO %s " % tb):
                    findobj = find_company_reg.findall(line)
                    if findobj:
                        allid += findobj
        uid = list(set(allid))
        if tb == "company":
            with open("%s/update_id.txt" % self.hqlpath,'a') as fw:
                fw.write("\n".join(uid))
        return len(uid)

    def clean_command_file(self,filenames):
        for file in filenames:
            if os.path.exists(file):
                os.remove(file)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print "parameter wrong!"
        exit(1)
    elif len(sys.argv) == 2 and len(sys.argv[1])== 8:
        today = sys.argv[1]
        yesterday = datetime.datetime(int(today[0:4]), int(today[4:6]), int(today[6:8])) - datetime.timedelta(days=1)
        yesterday = yesterday.strftime('%Y%m%d')
    elif len(sys.argv) == 3:
        today = sys.argv[1]
        yesterday = sys.argv[2]
    else:
        print "parameter wrong!"
        exit(1)
    lh = LoadHive(today)
    lh.load_data("prism_%s"%yesterday)