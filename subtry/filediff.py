#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
print "脚本名：", sys.argv[0]

def myread(name1,name2):
    #path="/data/updatesql/split/compair/"
    path = "."
    f1 = open("%s/%s"%(path,name1),'r').readlines()
    f2 = open("%s/%s"%(path,name2),'r').readlines()

    intersection = [v for v in f1 if v in f2]
    with open("same.txt", "a") as fe:
        fe.truncate()
        for a in intersection:
            fe.write(a + "\n")


#myread(sys.argv[1],sys.argv[2])
myread("0119.txt","0120.txt")
