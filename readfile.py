#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb
import re

def allCheck():
    aall=[]
    for i in range(500):
        if i < 10:
            num = '0000'+str(i)
        elif i < 100:
            num = '000'+str(i)
        else:
           num = '00'+str(i)
        aall += myread(num)
    print checkNum(aall)

def myread(num):
    #path="D://sth.txt"
    #fread = open(path,'r')
    path="/data/sx_data/distinct"
    fread = open("%s/part-%s"%(path,num),'r')
    start = 0
    sql = "select count(*) from companyinfo where name in ('"
    com = []
    for line in fread:
        if start%100 == 0 :
            if start != 0:
                com.append(sql[0:len(sql)-2]+")")
            sql ="select count(*) from companyinfo where name in ('"+line.split("~|~")[0]+"','"
        else:
            sql += line.split("~|~")[0]+"','"
        start +=1
    com.append(sql[0:len(sql)-2]+")")
    fread.close()
    return com

def checkNum(com):
    # 打开数据库连接
    db = MySQLdb.connect("bigdata-iac-info.crnvw81t0dcp.rds.cn-north-1.amazonaws.com.cn","finder","xsycommercial123","commercial" )
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    allsum = 0
    # 使用execute方法执行SQL语句
    for sql in com:
        cursor.execute(sql)
        # 使用 fetchone() 方法获取一条数据
        data = cursor.fetchone()
        grp = re.search(r'([0-9]+)',str(data),re.M|re.I)
        if grp:
            allsum += int(grp.group())
    # 关闭数据库连接
    db.close()
    return allsum

allCheck()
