#!/usr/bin/python
# -*- coding:utf-8 -*-
import smtplib,os,json,sys
import commands
from email.mime.text import MIMEText
from email.header import Header

class SendMailCheck:
    common_tab = ["human", "company"]
    company_tab = ["annual_report", "company_abnormal_info", "company_category_code_20170411", "company_change_info",
                   "company_check_info", "company_equity_info", "company_illegal_info",
                   "company_investor", "company_mortgage_info", "company_punishment_info", "company_staff",
                   "mortgage_change_info", "mortgage_pawn_info", "mortgage_people_info"]
    report_conn_tab = ["report_change_record", "report_equity_change_info",
                       "report_out_guarantee_info", "report_webinfo"]
    report_s_conn_tab = ["report_outbound_investment", "report_shareholder"]
    def __init__(self,date):
        # self.path = ("/data/gs/gs_split/%s" % date)
        # self.numcomparepath = ("/data/gs/gs_check/%s" % date)
        # self.infocomparepath = ("/data/gs/gs_check/%s/compare" % date)
        self.path = ("../%s" % date)
        self.numcomparepath = ("../%s" % date)
        self.infocomparepath = ("../%s" % date)
    def colect_info(self):
        mailinfo = []
        for table in self.common_tab + self.company_tab + self.report_s_conn_tab + self.report_conn_tab:
            wrongfile = self.get_FileSize("%s/%s_wrong.csv" % (self.path,table))
            updatefile = self.get_FileSize("%s/%s_update.csv" % (self.path,table))
            uniqeupdate = self.get_FileSize("%s/%s_update_u.csv" % (self.path,table))
            mailinfo.append("--------------------- %s split size check---------------------\n"%table)
            if not wrongfile == 0:
                mailinfo.append("error! %s splited wrong number is %d"%(table,wrongfile))
            if not (updatefile > 0 and uniqeupdate > 0):
                mailinfo.append("%s csv update info missed !"%table)
            else:
                mailinfo.append(table+" size is :" + str(updatefile))
                mailinfo.append(table+" unique size is :" + str(uniqeupdate))
            # load 条数检查
            mailinfo.append("\n ---------------------%s load num check---------------------\n"%table)
            mailinfo.extend(self.get_compare_num_info(table))
            mailinfo.append("\n ---------------------%s load info check---------------------\n"%table)
            mailinfo.extend(self.get_compare_info(table))
        return "\n".join(mailinfo)

    def get_FileSize(self,filePath):
        if os.path.exists(filePath):
            filePath = unicode(filePath, 'utf8')
            fsize = os.path.getsize(filePath)
            fsize = fsize / float(1024 * 1024)
            return int(fsize)
        else:
            return -1

    def get_compare_num_info(self,table):
        res = []
        numinfodata ="%s/compare_num.json" % self.numcomparepath
        if not os.path.exists(numinfodata):
            return res.append("error! compare num info missed!")
        with open(numinfodata, 'r') as load_num:
            loadnumdict = json.load(load_num.readlines())
        if not loadnumdict.has_key(table):
            res.append("error! %s compare info missed!" % table)
        else:
            if loadnumdict[table]["update"]["equal"] == "no":
                if table == "company":
                    res.append("error! %s update compare num wrong csv:%s,hive:%s,src_id:%s!" % (
                        table, loadnumdict[table]["update"]["csv_update_count"],
                        loadnumdict[table]["update"]["h_update_count"],
                        loadnumdict[table]["update"]["src_id_count"]
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
        data = "%s/%s_compare.txt" % (self.infocomparepath, table)
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

        message = MIMEText(self.colect_info(), 'plain', 'utf-8')
        message['From'] = Header("ang", 'utf-8')
        message['To'] = Header("测试", 'utf-8')

        subject = 'daily 检查'
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
    # sm = SendMailCheck(sys.argv[1])
    sm = SendMailCheck("20181")
    sm.send_mail()