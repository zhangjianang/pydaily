#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys

class ArgTest:
    def __init__(self,date):
        self.date = date

if __name__ == "__main__":
    print len(sys.argv)
    print sys.argv[0]
    print sys.argv[1]
    print sys.argv[2]
    print "param_length:",len(sys.argv[1])