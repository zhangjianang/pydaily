#!/usr/bin/python
import MySQLdb

for i in range(500):
    if i < 10:
        num = '0000'+str(i)
    elif i < 100:
        num = '000'+str(i)
    else:
       num = '00'+str(i)


def genName(num):
    path="/data/sx_data/distinct"
    fread = open("%s/part-%s"%(path,num),'r')
    lines = fread.readlines()
    names="'"
    for line in lines :
        names += line.split("~|~")[0]+"','"

    sql = "select count(*) from companyinfo where name in ("+names[0:len(names)-2]+")"
    fread.close()
    return sql

def checkNum(sql):
    # 打开数据库连接
    db = MySQLdb.connect("bigdata-iac-info.crnvw81t0dcp.rds.cn-north-1.amazonaws.com.cn","ufinder","xsycommercial123","commercial" )
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # 使用execute方法执行SQL语句
    cursor.execute(sql)

    # 使用 fetchone() 方法获取一条数据
    data = cursor.fetchone()

    # 关闭数据库连接
    db.close()

    print data

print checkNum(genName("00000"))
