#!/usr/bin/python
#encoding:utf8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import json,sys
import datetime,time
import urllib2
from elasticsearch.client import Elasticsearch as Elasticsearch
import threading
import argparse
class Mysql2Es():
    config = {
        "db":{
            "host":"192.168.0.196",
            "user":"finder",
            "passwd":"xsycommercial123",
            "db":"prism1",
            "charset":"utf8"
            },
        "max_query":"select max(id) from company",
        "query":"select id,name,company_org_type,reg_status from company",
        "index":{
            "host":["http://192.168.0.196:9200","http://192.168.0.197:9200","http://192.168.0.198:9200"],
            "_index":"company0606",
            "_type":"company"
            },    
        "action":"index",
        "_id":"id"
        }


    def __init__(self,start_id=0,max_id=100000,step=10000,id_file=None,config=None):
        if config != None:
            self.config = json.loads(open(config).read())
        #===================================================================
        # connect to mysql
        #===================================================================
        self.db = None 
        try:
            self.db = MySQLdb.connect(**self.config["db"])
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            sys.exit (1)
        
        #===================================================================
        # query select from table
        #===================================================================
        
        self.cursor = self.db.cursor()   
        #self.cursor.execute(self.config["max_query"])
        self.start_id = start_id
        self.max_id = max_id
        self.step = step
        self.id_file = id_file
        self.limit = 50000
        
        self.action = self.config['action']
        self.metadata = {"_index":self.config["index"]["_index"],"_type":self.config["index"]["_type"]}
        
        self.es = Elasticsearch(self.config["index"]["host"])
        
        self.mutex = threading.Lock()
        self.thread_num = 0
        self.db_data=[]
        self.complete = False
    def __del__(self):
        if self.cursor != None:
            self.cursor.close()        
        if self.db != None:
            self.db.close()        


    def post_to_es(self,bulk_data):
        while True:
            #print len(bulk_data)
            try:
                res = self.es.bulk(body = bulk_data, refresh = True)
                break
            except Exception,e:
                print e
            #f=open("r.txt","a");f.write("%s"%res)
        if self.mutex.acquire():
            self.thread_num -= 1
            self.mutex.release()
    
    def get_quota(self):
        while True:
            if self.mutex.acquire():
                if self.thread_num < 3:
                    self.thread_num += 1
                    print "post thread ",self.thread_num
                    self.mutex.release()
                    break
                else:
                    #  print "wait...",thread_num
                    time.sleep(0.1)
                self.mutex.release()

    
    def query_data_from_db(self,start_id,max_id):
        while start_id < max_id:
            query = self.config["query"]+" where id between %s and %s"%(start_id,start_id+self.step)
            print query,datetime.datetime.now()
            start_id += self.step
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            if len(rows) == 0: continue
            while len(self.db_data) > 4:
                time.sleep(0.1)
            self.db_data.insert(0,rows)
        self.complete = True
    
    def query_data_from_db_by_id(self,id_file):
        try:
            f = open(id_file)
        except Exception,e:
            print e
            self.complete = True
            sys.exit()
        while True:
            n = 0;ids = []
            for id in f:
                if id.strip().isdigit():
                    ids.append(str(id.strip()))
                    n+=1;
                if n >1000: break
            if n == 0: break
            query = self.config["query"]+" where id in(%s)"%(",".join(ids))
            print query,datetime.datetime.now()
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            if len(rows) == 0: continue
            while len(self.db_data) > 4:
                time.sleep(0.1)
            self.db_data.insert(0,rows)
        self.complete = True

    def do(self):
        count = 0
        print self.id_file
        if self.id_file != None:
            t = threading.Thread(target=self.query_data_from_db_by_id,args=(self.id_file,))
        else:
            t = threading.Thread(target=self.query_data_from_db,args=(self.start_id,self.max_id,))
        t.start()
        
        
        bulk_data = []
        while not self.complete:
            while not self.complete and len(self.db_data) == 0:
                print self.complete
                time.sleep(0.1)
        
            if len(self.db_data) > 0:
                rows = self.db_data.pop()
            else:
                break
        
            for row in rows:
                # print count
                if row[2] and row[2].startswith('个体') or (row[2] == None and row[3] == None):
                    continue
                obj = {}
                _id = None
                for i in range(2):
                    if row[i] == None: continue
                    if isinstance(row[i],datetime.datetime):
                        obj[self.cursor.description[i][0]] = str(row[i])
                    else:
                        obj[self.cursor.description[i][0]] = row[i]
                    if self.cursor.description[i][0] == self.config["_id"]:
                        _id = row[i]
                if _id == None:
                    continue
                metadata = self.metadata
                metadata["_id"] = _id 
                bulk_data.append(json.dumps({self.action:self.metadata}))
                if self.action != "delete":
                    bulk_data.append(json.dumps(obj))
                count += 1
                if count%self.limit == 0:
                    # self.get_quota()
                    # t = threading.Thread(target=self.post_to_es,args=(bulk_data,))
                    # t.start()
                    self.es.bulk(body=bulk_data, refresh=True)
                    bulk_data = []

        if count>0 and count%self.limit != 0:
            print count
            res = self.es.bulk(body = bulk_data, refresh = True)
            #print res
        
        

if __name__ == '__main__':
        parser = argparse.ArgumentParser(description='Import data from mysql to es.')
        parser.add_argument('-s','--start',nargs = '?',type=int,default=0)
        parser.add_argument('-e','--end',nargs = '?', type=int,default=100000)
        parser.add_argument('-f','--file',nargs = '?', type=str,default=None)
        parser.add_argument('-c','--config',nargs = '?', type=str,default=None)
        args = parser.parse_args()
        mysql2es = Mysql2Es(args.start,args.end,10000,args.file,config=args.config)
        mysql2es.do()

        

