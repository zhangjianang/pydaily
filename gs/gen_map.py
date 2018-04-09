#!/usr/bin/python
# -*- coding:utf-8 -*-
import re,sys,os

class MyGenAnnualCompMap:
    def __init__(self):
        pass
    def process_one_sql(self,line, fwper):
        op, t, table, detail = line.split(" ", 3)
        if op.lower() == "replace" and table.lower() == "annual_report":
            first_id= re.search(r'VALUES \((\d+),(\d+),', detail, re.M | re.I |re.S)
            other_id= re.findall(r'\),\((\d*),(\d*),', detail, re.M | re.I |re.S)
            fwper.write("%s--%s\n"%(first_id.group(1),first_id.group(2)))
            for perids in other_id:
                fwper.write("%s--%s\n"%(perids[0],perids[1]))

    def combineLines(self,path, filename, fout):
        last_line = ""
        for line in open("%s/%s" % (path, filename)):
            line = line.replace("\r", "")
            if line.strip() == "": continue
            if line.startswith("INSERT") or line.startswith("REPLACE") or line.startswith("DELETE"):
                cur = last_line
                last_line = line
            else:
                last_line = last_line.replace("\n", "")
                last_line += line.strip() + "\n"
                continue
            if cur == "": continue
            # print "----",cur
            self.process_one_sql(cur, fout)
            # print last_line
        if last_line != "":
            # print "----",last_line
            self.process_one_sql(last_line, fout)

    def loopDo(self,date):
        # path = ("/data/updatesql/download/%s"%(date))
        path = ("./%s" % (date))
        fout = open("%s/annual_comp.txt" % (path), 'a')
        for i in range(0, 3):
            filename = "%s_%s.sql" % (date, i)
            if os.path.exists("%s/%s" % (path, filename)):
                self.combineLines(path, filename, fout)
        fout.close()

if __name__ == '__main__':
    # loopDo(sys.argv[1])
    mys = MyGenAnnualCompMap()
    mys.loopDo("20180130")
