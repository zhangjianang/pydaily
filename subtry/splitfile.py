#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys,datetime

print "脚本名：", sys.argv[0]

def  splitfile(num):
    d4 = datetime.date.today() + datetime.timedelta(days=-1)
    path = "./"
    if len(sys.argv) > 1 :
        path += sys.argv[1]
    else:
        path += d4.strftime("%Y%m%d")
    fw =[]
    for i in range(0,num):
        fw.append(open("%s/update_id_%s.txt"%(path,str(i)), "a"))
    with open("%s/update_id.txt"%(path),'r') as fread:
        for line in fread:
            id = int(line.strip("\n"))
            fw[(id) % num].write(line)
    for i in range(0,num):
        fw[i].close()

splitfile(5)


