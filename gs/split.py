#!/usr/bin/python
# -*- coding:utf-8 -*-
import re,sys,os
class MySplit:
    find_annual_id_reg = re.compile(r'annualreport_id\s*=\s*(\d+);', re.M | re.I | re.S)
    split_header_reg = re.compile(r'(.*) VALUES (.*)', re.M | re.I | re.S)

    first_annual_comp_id_reg = re.compile(r'VALUES \((\d+),(\d+),', re.M | re.I |re.S)
    other_annual_comp_id_reg = re.compile(r'\),\((\d*),(\d*),',  re.M | re.I |re.S)

    def __init__(self,date):
        self.an_comp_map={}
        self.date = date
        self.path = ("./%s" % (self.date))
        self.fwper = open("%s/u%s.sql" % (self.path, self.date), 'a')
        self.loopDo()

    def genAnnualCompMap(self,cur):
        op, t, table, detail = cur.split(" ", 3)
        if op.lower() == "replace" and table.lower() == "annual_report":
            first_id= self.first_annual_comp_id_reg.search(detail)
            other_id= self.other_annual_comp_id_reg.findall(detail)
            # fwper.write("%s--%s\n"%(first_id.group(1),first_id.group(2)))
            if first_id.group(1):
                self.an_comp_map[first_id.group(1)] = first_id.group(2)
            for perids in other_id:
                self.an_comp_map[perids[0]] = perids[1]

    def splitOneSql(self, cur):
        header, body = self.getHeader(cur)
        if header and (not body):
            self.fwper.write(header + "\n")
            return
        elif (not header) and (not body):
            return
        lines = body.split("),(")
        if len(lines)==1:
            self.fwper.write(cur)
            return
        curline=""
        for perline in lines:
            if perline.endswith('null') or perline.endswith('\'') or perline.endswith(');'):
                if not curline=="":
                    curline+=")，("+perline
                else:
                    curline=perline
            else:
                if not curline=="":
                    curline+=")，("+perline
                else:
                    curline=perline
                continue
            if curline.startswith('('):
                self.fwper.write(header + " VALUES " + curline + ");\n")
            elif curline.endswith(');'):
                self.fwper.write(header + " VALUES (" + curline + "\n")
            else:
                self.fwper.write(header + " VALUES (" + curline + ");\n")
            curline=""
    def getHeader(self,line):
        line = line.strip()
        if not len(line) or line.startswith('#'):
            return None,None
        op, t, table, detail = line.split(" ", 3)
        if op.lower() == "delete":
            return line, None
        # import pdb;pdb.set_trace()
        matchObj = self.split_header_reg.match(line)
        if matchObj:
            return matchObj.group(1), matchObj.group(2)
        else:
            return None, None

    def checkPair(self,header, body):
        if not header and not body:
            return True
        if not header or not body or not len(header.strip()) or not len(body.strip()):
            return False
        headnum = len(header.split(','))
        linenum = 0
        items = body.split(',')
        for peritem in items:
            peritem = peritem.strip()
            if linenum == 0:
                peritem = peritem.strip('(')
            if linenum == headnum-1:
                peritem = peritem.strip("\n").strip(");")
            if peritem.endswith('\''):
                linenum += 1
            elif peritem == 'null' or peritem.isdigit():
                linenum += 1
        if headnum == linenum:
            return True
        else:
            print("headnum %i linenum %i \nheader is %s \nbody is %s"%(headnum,linenum,header,body))
            return False

    def rmEnterAndSplit(self, path, filename):
        last_line = ""
        with open("%s/%s" % (path, filename)) as fr:
            for line in fr:
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
                self.splitOneSql(cur)
            if last_line != "":
                self.splitOneSql(last_line)

    def loopDo(self):
        for i in range(0,3):
            filename = "%s_%s.sql" % (self.date, i)
            if os.path.exists("%s/%s" % (self.path, filename)):
                self.rmEnterAndSplit(self.path, filename)

if __name__ == '__main__':
    mys = MySplit("20180301")

