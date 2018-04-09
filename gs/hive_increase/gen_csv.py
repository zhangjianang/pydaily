#!/usr/bin/python
# -*- coding:utf-8 -*-
import re,sys,os
class MyGenCSV:
    # 单独测试
    # common_tab = ["company"]
    # company_tab = []
    # report_conn_tab = []
    # report_s_conn_tab = []
    common_tab = ["human","company"]
    company_tab = ["annual_report","company_abnormal_info","company_category_code_20170411","company_change_info",
                   "company_check_info","company_equity_info", "company_illegal_info",
                   "company_investor", "company_mortgage_info", "company_punishment_info", "company_staff",
                   "mortgage_change_info", "mortgage_pawn_info", "mortgage_people_info"]
    report_conn_tab = ["report_change_record","report_equity_change_info",
                       "report_out_guarantee_info","report_webinfo"]
    report_s_conn_tab = ["report_outbound_investment","report_shareholder"]

    delete_multi = []
    keep_line_info = {'company_category_demo':{'del_val':{'company_id_val':'line_num'},'del_index':'company_id','company_id_val':{'tid':'line_num'}}}
    update_files = {}
    delete_files = {}
    wrong_files = {}
    __LINE_TERMINATER = "),("
    __LINE_TER_REPLACE= ");("
    __FIELD_TERMINATER = ","
    __FIELD_TERMINATER_REPLACE = ";"
    __FIELD_TERMINATER_UNIQ = "\t"
    __INSERT_LINE_COL = 2

    find_escape_reg = re.compile(r".*?(\\+)'$", re.M | re.I | re.S)

    def __init__(self,date):
        self.total_line_num = 0
        self.origin_line_num = 0
        self.date = date
        # self.path = ("../%s" % (self.date))
        self.path = ("/mnt1/gs/gs_split/%s" % (self.date))
        self.srcpath = ("/mnt1/gs/download/%s" % (self.date))
        if os.path.exists(self.path):
            print 'file exist'
        else:
            os.mkdir(self.path)
            self.alldata = open("%s/u%s.sql" % (self.path, self.date), 'a')
            self.analytic_file = open("%s/ana_%s.sql" % (self.path, self.date), 'a')
            self.createFilesMap()
            self.loopDo()
            print 'finish'

    def createFilesMap(self):
        for table in self.common_tab + self.company_tab + self.report_s_conn_tab + self.report_conn_tab:
            self.update_files[table] = open("%s/%s_update.csv" % (self.path, table), 'a')
            self.delete_files[table] = open("%s/%s_delete.csv" % (self.path, table), 'a')
            self.wrong_files[table] = open("%s/%s_wrong.csv" % (self.path, table), 'a')
            idkey = "id"
            if self.common_tab.count(table) > 0:
                idkey = "id"
            elif self.company_tab.count(table) > 0:
                idkey = "company_id"
            elif self.report_conn_tab.count(table) > 0:
                idkey = "annualreport_id"
            elif self.report_s_conn_tab.count(table) > 0:
                idkey = "annual_report_id"
            self.keep_line_info[table] = {"key_row_num":-1,"del_val":{},'del_index':idkey}

    def splitOneSql(self, line):
        if not len(line) or line.startswith('#'):
            return
        self.alldata.write(line)
        self.total_line_num += 1
        self.origin_line_num += 1

        line = line.strip()
        op, t, table, detail = line.split(" ", 3)
        if op == "DELETE":
            self.deleteGen(table, detail)
        elif op == "REPLACE":
            self.replaceGen(table, detail)

    def deleteGen(self, table, detail):
        w,keyid,ins,ids = detail.split(" ",3)
        ids = self.stripSpaceAndBracket(ids).split(self.__FIELD_TERMINATER)
        self.total_line_num += 1
        for id in ids:
            self.delete_files[table].write(id+"\n")
            if keyid == self.keep_line_info[table]['del_index']:
                if self.keep_line_info[table].has_key(id):
                    self.analytic_file.write("%s|:|%d|:|del|:|%s\n"%(table,self.origin_line_num,self.keep_line_info[table][id]))
                    self.keep_line_info[table][id] = {}
                    self.keep_line_info[table]['del_val'][id] = self.total_line_num
            else:
                self.wrong_files[table].write("del err:" + str(id) +",line num:" + self.total_line_num + "\n")

    def replaceGen(self, table, detail):
        details = detail.split("VALUES",1)
        if len(details) < 2:
            self.wrong_files[table].write(details + "\n")
            return
        lines = details[1].split(self.__LINE_TERMINATER)
        preline = ""
        for curline in lines:
            if preline == "":
                preline = curline
            else:
                preline += self.__LINE_TER_REPLACE + curline
            if curline.endswith('null') or curline.endswith('\'') or curline.endswith(');'):
                preline=preline.strip()
                terms = self.replaceCommaInQuotes(preline)
                keys = self.stripSpaceAndBracket(details[0]).split(self.__FIELD_TERMINATER)
                self.total_line_num += 1
                if len(keys) == len(terms):
                    self.addIdAndLineNum(table, keys, terms, self.total_line_num)
                    terms.insert(self.__INSERT_LINE_COL, str(self.total_line_num))
                    self.update_files[table].write(self.__FIELD_TERMINATER.join(terms)+"\n")
                else:
                    self.wrong_files[table].write(self.__FIELD_TERMINATER.join(terms)+"\n")
                preline = ""

    # 增加去重信息
    def addIdAndLineNum(self,table,keys,terms,line_num):
        knum = keys.index(self.keep_line_info[table]['del_index'])
        if knum >= 0 and knum < len(terms):
            self.keep_line_info[table]['key_row_num'] = knum
            if self.keep_line_info[table].has_key(terms[knum]):
                if self.keep_line_info[table][terms[knum]].has_key(terms[0]):
                    self.analytic_file.write("%s|:|%d|:|%d|:|%s|:|%s\n " % (table, self.origin_line_num,line_num,terms[knum],terms[0]))
                self.keep_line_info[table][terms[knum]][terms[0]] = line_num
            else:
                self.keep_line_info[table][terms[knum]] = {terms[0]: line_num}

    def stripSpaceAndBracket(self,data):
        data = data.strip()
        if data.startswith('('):
            data = data.lstrip('(')
        if data.endswith(')'):
            data = data.rstrip(')')
        elif data.endswith(');'):
            data = data.rstrip(');')
        return data

    def replaceCommaInQuotes(self, line):
        allterm = []
        items = self.stripSpaceAndBracket(line).split(self.__FIELD_TERMINATER)
        preterm = ""
        for curterm in items:
            curterm = curterm.strip()
            if curterm == "26842246":
                print "ok"
            if preterm == "":
                preterm = curterm
            else:
                preterm += self.__FIELD_TERMINATER_REPLACE + curterm
            if preterm.startswith("'"):
                if len(preterm) > 1 and curterm.endswith("'") and not self.checkEscapeOdd(curterm):
                    allterm.append(preterm)
                    preterm = ""
                else:
                    continue
            elif curterm == "null" or curterm.isdigit():
                allterm.append(preterm)
                preterm = ""
        return allterm

    # 查看\结尾奇数个
    def checkEscapeOdd(self, item):
        if item.endswith("\\'"):
            matchObj = self.find_escape_reg.match(item)
            if matchObj:
                if len(matchObj.group(1)) % 2 == 0:
                    return False
                else:
                    return True
        return False

    def checkKeyValuePair(self,key,num):
        keys = key.split(self.__FIELD_TERMINATER)
        if len(keys)==num:
            return True
        else:
            return False

    def removeDuplicatedRow(self):
        for table in self.common_tab + self.company_tab + self.report_s_conn_tab + self.report_conn_tab:
            self.update_files[table].close()
            uw = open("%s/%s_update_u.csv" % (self.path, table),'a')
            count = 0
            with open("%s/%s_update.csv" % (self.path, table)) as fr:
                for line in fr :
                    count +=1
                    if line.strip() == "": continue
                    terms = line.strip('\n').split(self.__FIELD_TERMINATER)
                    keynum = self.keep_line_info[table]['key_row_num']
                    if self.keep_line_info[table].has_key(terms[keynum]):
                        if self.keep_line_info[table][terms[keynum]].has_key(terms[0])>0:
                            keepnum = self.keep_line_info[table][terms[keynum]][terms[0]]
                            try:
                                pop_num = terms.pop(self.__INSERT_LINE_COL)
                                if keepnum == int(pop_num):
                                    uw.write(self.__FIELD_TERMINATER_UNIQ.join(terms)+"\n")
                            except Exception, e:
                                print e
                print count

    def rmEnterAndSplit(self, path, filename):
        last_line = ""
        with open("%s/%s" % (path, filename)) as fr:
            self.origin_line_num = 0
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
        for root, dirs, files in os.walk(self.srcpath):
            for file in files:
                if file.startswith(self.date):
                    self.rmEnterAndSplit(self.srcpath, file)
            self.removeDuplicatedRow()

if __name__ == '__main__':
    MyGenCSV("20180401")
    # MyGenCSV(sys.argv[1])