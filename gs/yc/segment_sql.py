#!/usr/bin/python
import re,os,sys
import MySQLdb
import datetime
import ntpath
class SegmentSql:
	config = [
		{"no":0,"default":True, "properties":{ "host":"192.168.0.196","user":"finder","passwd":"xsycommercial123","db":"prism1"}},
		{"no":1,"default":False,"properties":{ "host":"192.168.0.197","user":"finder","passwd":"xsycommercial123","db":"prism1"}},
		{"no":2,"default":False,"properties":{ "host":"192.168.0.198","user":"finder","passwd":"xsycommercial123","db":"prism1"}}
		]

	config = [
#		{"no":-1,"default":True, "properties":{ "host":"prod-iac-src.cpjun6wiviel.rds.cn-north-1.amazonaws.com.cn","user":"finder","passwd":"xsycommercial123","db":"prism1"}},
		{"no":0,"default":True, "properties":{ "host":"bigdataprodrdssrc.crnvw81t0dcp.rds.cn-north-1.amazonaws.com.cn","user":"finder","passwd":"xsycommercial123","db":"commerce0"}},
		{"no":1,"default":False, "properties":{ "host":"bigdataprodrdssrc.crnvw81t0dcp.rds.cn-north-1.amazonaws.com.cn","user":"finder","passwd":"xsycommercial123","db":"commerce1"}},
		{"no":2,"default":False, "properties":{ "host":"bigdataprodrdssrc.crnvw81t0dcp.rds.cn-north-1.amazonaws.com.cn","user":"finder","passwd":"xsycommercial123","db":"commerce2"}},
		{"no":3,"default":False, "properties":{ "host":"bigdataprodrdssrc.crnvw81t0dcp.rds.cn-north-1.amazonaws.com.cn","user":"finder","passwd":"xsycommercial123","db":"commerce3"}},
		{"no":4,"default":False, "properties":{ "host":"bigdataprodrdssrc.crnvw81t0dcp.rds.cn-north-1.amazonaws.com.cn","user":"finder","passwd":"xsycommercial123","db":"commerce4"}},
		]

	common_tables = ["human","annual_report"]
	company_tables =["company_abnormal_info","company_category","company_category_20170411","company_category_code_20170411","company_change_info","company_change_info_new","company_check_info","company_equity_info","company_ids","company_illegal_info","company_investor","company_mortgage_info","company_punishment_info","company_staff","company_investor","mortgage_change_info","mortgage_pawn_info","mortgage_people_info"]
	report_tables = ["report_change_record","report_equity_change_info","report_out_guarantee_info","report_outbound_investment","report_shareholder","report_webinfo"]
	
	id_reg = re.compile("VALUES \((\d+)")
	company_id_reg = re.compile("VALUES \(\d+,(\d+)")
	report_id_reg = re.compile("VALUES \(\d+,(\d+)")
	delete_id_reg = re.compile("company_id = (\d+)")
	n = 0
	db_num = 5
	dbs = {}
	files = {}

	def __init__(self,sql_file,out_path):
		self.sql_file = sql_file
		self.out_path = out_path 
		os.system("mkdir -p %s"%out_path)
                self.id_file = open(self.out_path+"/update_id.txt","w")
		self.parse_config()
		self.create_files_map()
		pass

	def create_files_map(self):
		for tab in self.common_tables:
			self.files[tab]=[open(self.out_path+"/"+tab+".sql","w"),0]
		tabs = ["company"]
                #import pdb;pdb.set_trace()
		tabs.extend(self.company_tables)
		tabs.extend(self.report_tables)


		for tab in tabs:
                        if tab == 'report_change_record':
                            import pdb;pdb.set_trace()
			for i in range(self.db_num):
				self.files[tab+str(i)]=[open(self.out_path+"/"+tab+"."+str(i)+".sql","w"),0]
	
	def write_info_to_files(self,info):
		for f in self.files:
			self.files[f][0].write("%s\n"%info)
	
	def create_exe_script(self):
		print "please execute following bash script:"
		for conf in self.config:
			fname = "%s/%s.%d.sh"%(self.out_path,ntpath.basename(self.sql_file),conf["no"])
			shell = open(fname,"w")
			command = []
			conn = "mysql -h%s -u%s -p%s %s"%(conf["properties"]["host"],conf["properties"]["user"],conf["properties"]["passwd"],conf["properties"]["db"])
			if conf["default"]:
				for f in self.files.values():
					if not f[0].name.split(".")[-2].isdigit() and f[0].name.find("/human.") == -1:
                                                #import pdb; pdb.set_trace()  
						command.append("%s < %s"%(conn,f[0].name))

                        if conf["no"] >= 0:
                                for f in self.files.values():
                                    if f[0].name.find("/human.") != -1:        
					command.append("%s < %s"%(conn,f[0].name))
		
			for f in self.files.values():
				if f[0].name.split(".")[-2] == str(conf["no"]):
					command.append("%s < %s"%(conn,f[0].name))
            

			shell.write("\n".join(command))
			print shell.name




	def parse_config(self):
		for conf in self.config:
			db = MySQLdb.connect(**conf["properties"])
			if conf["default"]:
				self.dbs["default"] = db.cursor()
			self.dbs[conf["no"]] = db.cursor()

	def get_company_id(self,sql):
		#print sql
		op,t,table,detail = sql.split(" ",3)
		ids=[]
		#print table,"--->",detail
		if op.lower() == 'insert' or op.lower() == 'replace':
			if table == 'company':
				ids = self.id_reg.findall(detail)
			elif table == 'company_category_20170411':
				ids = self.id_reg.findall(detail)
			elif table in self.company_tables:
				ids = self.company_id_reg.findall(detail)
			elif table == 'human--':
				id = self.id_reg.findall(detail)
				q = "select company_id from company_staff where staff_id = %d"%(int(id[0]))
				#print q
				self.dbs['default'].execute(q)
				for r in self.dbs['default'].fetchall():
					ids.append(str(r[0]))
				q = "select company_id from company_investor where investor_id = %d"%(int(id[0]))
				#print q
				self.dbs['default'].execute(q)
				for r in self.dbs['default'].fetchall():
					ids.append(str(r[0]))
				if len(ids) == 0: ids.append('0')
			elif table in self.report_tables:
				id = self.report_id_reg.findall(detail)
				q = "select company_id from annual_report where id = %d"%(int(id[0]))
				self.dbs['default'].execute(q)
				for r in self.dbs['default'].fetchall():
					ids.append(str(r[0]))
				if len(ids) == 0: ids.append('0')
		elif op.lower() == 'delete':
			if table not in self.common_tables:
				ids = self.delete_id_reg.findall(detail)
			pass

		return op,table,ids
	
	def get_route(self,ids):
		return set([int(id)%self.db_num for id in ids])
        def write_updte_ids(self,ids):
                self.id_file.write("%s\n"%"\n".join(ids))
	
	def process_one_sql(self,cur):
		try:
                        import pdb
                        pdb.set_trace()
			#print cur
			#print "----------------------------------------"
			op,tab,ids = self.get_company_id(cur)
                        self.write_updte_ids(ids)
			#print op,tab,ids
			routes = self.get_route(ids)
			for r in routes:
				self.files[tab+str(r)][0].write("%s"%cur)
				self.files[tab+str(r)][1] += 1
				if self.files[tab+str(r)][1]%10000 == 0:
					self.files[tab+str(r)][0].write("commit;\n")
			if len(routes) == 0:
				self.files[tab][0].write("%s"%cur)
				self.files[tab][1] += 1
				if self.files[tab][1]%10000 == 0:
					self.files[tab][0].write("commit;\n")
			self.n += 1
			if self.n%10000 == 0:
				print self.n,datetime.datetime.now()
				#break
		except Exception,e:
			print e
		#	print line
			pass

	
	def process(self):
		self.write_info_to_files("set autocommit=0;")
		last_line = ""
		for line in open(self.sql_file):
			line = line.replace("\r","")
			if line.strip() == "": continue
			if line.startswith("INSERT") or line.startswith("REPLACE") or line.startswith("DELETE"):
				cur = last_line
				last_line = line
			else:
                                last_line = last_line.replace("\n","")
				last_line += line.strip()+"\n"
				continue
			if cur == "":continue
                        #print "----",cur
			self.process_one_sql(cur)
			#print last_line
		if last_line != "":
                        #print "----",last_line
			self.process_one_sql(last_line)	
		self.write_info_to_files("commit;")
		


if __name__ == '__main__':

	segmenter = SegmentSql(sys.argv[1],sys.argv[2])
	segmenter.process()
	segmenter.create_exe_script()
		
	
