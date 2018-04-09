#!/usr/bin/python
# -*- coding:utf-8 -*-
import os,re


def check_os():
    try:
        csvpath = "/data/gs_check/%s" %"20180322"
        w_update_res = os.popen("wc -l %s/%s_update_u.csv" % (csvpath, 'company')).read().splitlines()[0]
        h_update_res = os.popen("/usr/bin/hive -e 'select count(*) from %s.%s_update;'" % ("prism_20180322", "company")).read().splitlines()[0]

        print 'hive c:',h_update_res
        print 'wc c:',w_update_res.split()[0]
        if int(h_update_res) == int(w_update_res.split()[0]):
            print 'equal'
    except Exception,e:
        print e

# check_os()

def get_company_id():
    hqlpath = "/data/gs_load/%s" %"20180322"
    os.mkdir(hqlpath)
    find_company_reg = re.compile(r"\(([0-9]+)\,", re.M | re.I | re.S)
    allid = []
    with open("../20180401/u20180401.sql") as fr:
        for line in fr:
            if line.startswith("REPLACE INTO company "):
                findobj = find_company_reg.findall(line)
                if findobj:
                   allid += findobj
    print 'all:',len(allid)
    uid = list(set(allid))
    print 'quc:',len(uid)
    with open("%s/update_id.txt" % hqlpath, 'a') as fw:
        fw.write("\n".join(uid))

if __name__ == "__main__":
    get_company_id()