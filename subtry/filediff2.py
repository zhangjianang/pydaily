#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys
print "脚本名：", sys.argv[0]

def myread(name1,name2):
    #path="/data/updatesql/split/compair/"
    path = "."
    f2set = set()

    intersection = []
    with open("%s/%s"%(path,name2),'r') as f2:
         for line in f2:
             f2set.add(line)

    with open("%s/%s" % (path, name1), 'r') as f1:
        for line in f1:
             if line in f2set:
                 intersection.append(line)

    with open("same2.txt", "a") as fe:
        fe.truncate()
        for a in intersection:
            fe.write(a + "\n")

#myread(sys.argv[1],sys.argv[2])
myread("0119.txt","0120.txt")
