#!/usr/bin/python
# -*- coding: UTF-8 -*-

import thread
import time,json

def print_time(threadName,delay):
    count=0
    while count<5:
        time.sleep(delay)
        count+=1
        print "%s:%s"%(threadName,time.ctime(time.time()))

# try:
#     thread.start_new_thread(print_time,('mythrea_1',2))
#     thread.start_new_thread(print_time,('mythrea_2',2))
# except:
#     print "sth is wrong while starting thread"
# else:
#     print "ok"

if __name__=="__main__":
    print json.dumps({"we are":"champion"})