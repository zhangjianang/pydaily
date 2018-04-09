#!/usr/bin/python
import os

import sys


class BatchInsert:
    def __init__(self,db):
        self.connection = "mysql -hbigdataprodrdssrc.crnvw81t0dcp.rds.cn-north-1.amazonaws.com.cn  -ufinder -pxsycommercial123 %s"%db
        self.error = "ERROR"
        self.batch_size = 10000

    def insert(self, sql_list, prefix):
        fn = "%s.sql" % (prefix)
        f = open(fn, "w")
        f.write("set autocommit = 0;\n")
        for sql in sql_list:
            f.write("%s" % sql)
        f.write("commit;")
        f.close()
        ret = os.popen("%s < %s 2>&1" % (self.connection, fn)).read()
        if ret.find(self.error) != -1:
            n = len(sql_list)
            print ret
            if n > 1:
                self.insert(sql_list[0:n / 2], "%s0" % prefix)
                self.insert(sql_list[n / 2:], "%s1" % prefix)
            else:
                self.error_file.write(sql_list[0])
        os.popen("rm -20280129.sql %s" % fn)

    def do(self, fname):
        self.error_file = open("%s.err"%fname,"w")

        sql_list = []
        prefix = fname.replace(".sql","")+str(os.getpid())
        i = 0
        for line in open(fname):
            if line.startswith("set autocommit") or line.startswith("commit;"):
                continue
            sql_list.append(line)
            if len(sql_list) == self.batch_size:
                self.insert(sql_list, "%s.%i" % (prefix, i))
                i += 1
                sql_list = []
        if len(sql_list) > 0:
            self.insert(sql_list, "%s.%i" % (prefix, i))
        self.error_file.close()

if __name__ == '__main__':
    db, fname = sys.argv[1], sys.argv[2]
    batch_insert = BatchInsert(db)
    batch_insert.do(fname)
