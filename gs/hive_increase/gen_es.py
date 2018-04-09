#!/usr/bin/python
# -*- coding:utf-8 -*-
import sys, os,json
from elasticsearch.client import Elasticsearch as Elasticsearch

class GenEsInfo:
    def __init__(self,date,predate):
        self.date = date
        self.idpath = "/data/gs/gs_check/%s" % date
        self.esiddata = "%s/es_info.txt" % self.idpath
        self.es = Elasticsearch(["http://192.168.0.196:9200","http://192.168.0.197:9200","http://192.168.0.198:9200"])
        self.gen_es("prism_%s"%date,"prism_%s"%predate)

    def gen_es(self,db,dbpre,mode=10000):
        os.system("/usr/bin/hive -e 'set hive.mapred.mode=nonstrict; select a.id,a.name,a.company_org_type,a.reg_status from %s.company_update as a where a.id not in (select b.id from %s.company as b)'>> %s"%(db,dbpre,self.esiddata))
        fwrite = open(self.esiddata, "a")
        fwrite.write("\n#------------以下为更新----------------\n")
        os.system("""/usr/bin/hive -e 'set hive.mapred.mode=nonstrict;select a.id,a.name,a.company_org_type,a.reg_status from %s.company_update as a where a.id not in 
                              (select c.id from %s.company as b join %s.company_update as c on b.id = c.id and b.name =c.name);' >> %s"""
                              %(db,dbpre,db,self.esiddata))
        fwrite.close()
        with open(self.esiddata) as fr:
            count,bulk_data = 0,[]
            for line in fr.readlines():
                if line.startswith("#"): continue
                idname = line.split("\t")
                if idname[2] and idname[2].startswith('个体') or (idname[2] == None and idname[3] == None):
                    count += 1
                    done = {"name": idname[1], "id": idname[0]}
                    dtwo = {"index": {"_index": "company", "_type": "company", "_id": idname[0]}}
                    bulk_data.append(json.dumps(done, ensure_ascii=False))
                    bulk_data.append(json.dumps(dtwo))
                    if count%mode == 0:
                        self.es.bulk(body=bulk_data, refresh=True)
                        bulk_data =[]
            if len(bulk_data) >0 :
                self.es.bulk(body=bulk_data, refresh=True)
if __name__ == "__main__":
    gs = GenEsInfo("20180322","20180321")
