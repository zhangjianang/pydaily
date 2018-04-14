#!/usr/bin/python
#encoding:utf8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import smtplib,os,json
import commands
from email.mime.text import MIMEText
from email.header import Header

class SendMailCheck:
    common_tab = ["human", "company"]
    company_tab = ["annual_report", "company_abnormal_info", "company_category_20170411", "company_change_info",
                   "company_check_info", "company_equity_info", "company_illegal_info",
                   "company_investor", "company_mortgage_info", "company_punishment_info", "company_staff",
                   "mortgage_change_info", "mortgage_pawn_info", "mortgage_people_info"]
    report_conn_tab = ["report_change_record", "report_equity_change_info",
                       "report_out_guarantee_info", "report_webinfo"]
    report_s_conn_tab = ["report_outbound_investment", "report_shareholder"]
    def __init__(self,date):
        self.path = ("/mnt2/gs/gs_split/%s" % date)
        self.numcomparepath = ("/mnt2/gs/gs_check/%s" % date)
        self.infocomparepath = ("/mnt2/gs/gs_check/%s/compare" % date)
        self.date = date
        # self.path = ("../%s" % date)
        # self.numcomparepath = ("../%s" % date)
        # self.infocomparepath = ("../%s" % date)
        self.splitinfo = {}
        self.init_split_info(self.numcomparepath)

    def colect_info(self):
        mailinfo = []
        for table in self.common_tab + self.company_tab + self.report_s_conn_tab + self.report_conn_tab:
            updatefile,wrongfile = self.get_split_info(table)
            uniqeupdate = self.get_file_size("%s/%s_update_u.csv" % (self.path, table))
            mailinfo.append("\n-------------------- %s split size check\n"%table)
            if not wrongfile.isdigit():
                mailinfo.append("error! %s splited wrong number is %s "%(table,wrongfile))
            else:
                mailinfo.append("%s splited wrong number is %s " % (table, wrongfile))

            mailinfo.append("%s size is :%s " % (table,str(updatefile)))
            mailinfo.append("%s unique size is :%s M" % (table,str(uniqeupdate)))

            # load 条数检查
            mailinfo.append("\n ---------------------%s load num check\n"%table)
            mailinfo.extend(self.get_compare_num_info(table))
            mailinfo.append("\n ---------------------%s load info check\n"%table)
            mailinfo.extend(self.get_compare_info(table))
            mailinfo.append("\n\n --------------------------------------------------------------------------------\n\n")
        return "\n".join(mailinfo)

    def get_file_size(self, filePath):
        if os.path.exists(filePath):
            filePath = unicode(filePath, 'utf8')
            fsize = os.path.getsize(filePath)
            fsize = fsize / float(1024 * 1024)
            return int(fsize)
        else:
            return -1

    def init_split_info(self,path):
        if not os.path.exists("%s/splitinfo.txt" % path):
            return
        fr = open("%s/splitinfo.txt" % path).readlines()
        for line in fr:
            line = line.strip("\n")
            items = line.split(" ")
            if len(items) < 9: continue
            if items[-1].endswith(".csv"):
                tname = items[-1].rstrip(".csv")
                self.splitinfo[tname] = items[-5]

    def get_split_info(self,table):
        upkey = "%s_update"%table
        wrongkey = "%s_wrong"%table
        if self.splitinfo.has_key(upkey):
            up_size = self.splitinfo[upkey]
        else:
            up_size = "-1M"
        if self.splitinfo.has_key(wrongkey):
            wrong_size = self.splitinfo[wrongkey]
        else:
            wrong_size = "-1M"
        return (up_size,wrong_size)

    def get_compare_num_info(self,table):
        print table
        res = []
        numinfodata ="%s/compare_num.json" % self.numcomparepath
        if not os.path.exists(numinfodata):
            res.append("error! compare num info missed!")
            return res
        with open(numinfodata, 'r') as load_num:
            loadnumdict = json.load(load_num)
        if not loadnumdict.has_key(table):
            res.append("error! %s compare info missed!" % table)
        else:
            if loadnumdict[table]["update"]["equal"] == "no":
                if table == "company":
                    srcidcount = "0"
                    if loadnumdict[table]["update"].has_key("src_id_count"):
                        srcidcount = loadnumdict[table]["update"]["src_id_count"]
                    res.append("error! %s update compare num wrong csv:%s,hive:%s,src_id:%s!" % (
                        table, loadnumdict[table]["update"]["csv_update_count"],
                        loadnumdict[table]["update"]["h_update_count"],
                        srcidcount
                    ))
                else:
                    res.append("error! %s update compare num wrong csv:%s,hive:%s !" % (
                        table, loadnumdict[table]["update"]["csv_update_count"],
                        loadnumdict[table]["update"]["h_update_count"]))
            else:
                res.append("%s update compare num %s" % (table,loadnumdict[table]["update"]["h_update_count"]))
            if self.common_tab.count(table) == 0:
                if loadnumdict[table]["delete"]["equal"] == "no":
                    res.append("error! %s delete compare num wrong csv:%s,hive:%s !" % (
                        table, loadnumdict[table]["delete"]["csv_delete_count"],
                        loadnumdict[table]["delete"]["h_delete_count"]))
                else:
                    res.append("%s delete compare num %s" % (table,loadnumdict[table]["delete"]["h_delete_count"]))
        return res

    def get_compare_info(self,table):
        # load 内容检查
        res = []
        data = "%s/%s_compare.json" % (self.infocomparepath, table)
        if not os.path.exists(data):
            res.append("error! %s hive compare info missed !"%table)
            return res
        with open(data, 'r') as load_info:
            loadinfodict = json.load(load_info)
            if loadinfodict.has_key(table):
                if len(loadinfodict[table]) == 0:
                    res.append(table+" compare info right!")
                else:
                    res.append(""+",".join(loadinfodict[table].keys()))
            else:
                res.append("error! %s compare info missed !"%table)
        return res

    def send_mail(self):
        # 第三方 SMTP 服务
        mail_host = "smtp.126.com"  # 设置服务器
        mail_user = "zhangjianang151@126.com"  # 用户名
        mail_pass = "zja112358"  # 口令

        sender = 'zhangjianang151@126.com'
        receivers = ["zhangjianang151@126.com"]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
        maininfo = self.colect_info()
        print maininfo
        message = MIMEText(maininfo, 'plain', 'utf-8')
        message['From'] = Header("ang", 'utf-8')
        message['To'] = Header("测试", 'utf-8')

        subject = self.date+' daily 检查'
        message['Subject'] = Header(subject, 'utf-8')

        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
            smtpObj.login(mail_user, mail_pass)
            smtpObj.sendmail(sender, receivers, message.as_string())
            print "邮件发送成功"
        except smtplib.SMTPException:
            print "Error: 无法发送邮件"
if __name__ == "__main__":
    sm = SendMailCheck(sys.argv[1])
    # sm = SendMailCheck("20181")
    sm.send_mail()