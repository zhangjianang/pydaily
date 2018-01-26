#!/usr/bin/python
#-*- coding:utf-8 -*-
import os
from io import StringIO
def myread():
    with open('D://sth.txt', 'r') as f:
        u = f.readlines()
        for line in u:
            print line

#查看环境变量中某个值
# print os.getenv("JAVA_HOME")
# #获取整个环境变量
# print os.environ
#
# # 查看当前目录的绝对路径:
# print os.path.abspath('.')
#
# os.path.join(os.path.abspath('.'), 'testdir')

#print type(os.system("dir"))
a = StringIO(os.popen("dir").read())



# 首先把新目录的完整路径表示出来:
# os.path.join('/Users/michael', 'testdir')
# # 然后创建一个目录:
# os.mkdir('/Users/michael/testdir')
# 删掉一个目录:
