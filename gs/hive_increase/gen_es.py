#!/usr/bin/python
#encoding:utf8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import sys, os,json,datetime,io
# from elasticsearch.client import Elasticsearch as Elasticsearch

class GenEsInfo:
    def __init__(self,date,predate):
        self.date = date
        # self.idpath = "/mnt2/gs/gs_check/%s" % date
        self.idpath = "../%s" % date
        self.hesdata = "%s/es_info.txt" % self.idpath
        self.esdata = "%s/es_gen_t.txt" % self.idpath
        # self.es = Elasticsearch(["http://192.168.0.196:9200","http://192.168.0.197:9200","http://192.168.0.198:9200"])
        self.gen_es("prism_%s"%date,"prism_%s"%predate)

    def gen_es(self,db,dbpre,mode=10000):
        # os.system("/usr/bin/hive -e 'set hive.mapred.mode=nonstrict; select a.id,a.name,a.company_org_type,a.reg_status from %s.company_update as a where a.id not in (select b.id from %s.company as b)'>> %s"%(db,dbpre,self.esiddata))
        # fwrite = io.open(self.hesdata, "a", encoding="utf-8")
        # fwrite.write(u"\n#------------以下为更新----------------\n")
        #os.system("""/usr/bin/hive -e 'set hive.mapred.mode=nonstrict;select a.id,a.name,a.company_org_type,a.reg_status from %s.company_update as a where a.id not in
        #                       (select c.id from %s.company as b join %s.company_update as c on b.id = c.id and b.name =c.name);' >> %s"""
        #                       %(db,dbpre,db,self.esiddata))
        # fwrite.close()
        tmpwrite = open(self.esdata,"a")
        with open(self.hesdata) as fr:
            count,bulk_data = 0,[]
            for line in fr:
                if line.startswith("#") or line == "": continue
                idname = line.split("\t")
                if len(idname) < 4:
                    print line
                    continue
                if idname[2] and idname[2].find('个体') > 0 :
                    if idname[1] and len(idname[1]) < 20:
                        continue
                elif (idname[2] == None and idname[3] == None):
                    continue
                count += 1
                done = {"name": idname[1], "id": idname[0]}
                dtwo = {"index": {"_index": "company", "_type": "company", "_id": idname[0]}}
                bulk_data.append(json.dumps(done, ensure_ascii=False))
                bulk_data.append(json.dumps(dtwo))
                if count%mode == 0:
                    # self.es.bulk(body=bulk_data, refresh=True)
                    tmpwrite.write("\n".join(bulk_data))
                    bulk_data =[]
            if len(bulk_data) >0 :
                # self.es.bulk(body=bulk_data, refresh=True)
                tmpwrite.write("\n".join(bulk_data))

if __name__ == "__main__":
    # if len(sys.argv) == 1:
    #     print "parameter wrong!"
    #     exit(1)
    # elif len(sys.argv) == 2 and len(sys.argv[1]) == 8:
    #     today = sys.argv[1]
    #     yesterday = datetime.datetime(int(today[0:4]), int(today[4:6]), int(today[6:8])) - datetime.timedelta(days=1)
    #     yesterday = yesterday.strftime('%Y%m%d')
    # elif len(sys.argv) == 3:
    #     today = sys.argv[1]
    #     yesterday = sys.argv[2]
    # else:
    #     print "parameter wrong!"
    #     exit(1)
    gs = GenEsInfo("20181","20180321")
    # gs = GenEsInfo(today,yesterday)
