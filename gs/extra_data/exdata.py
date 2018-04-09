#!/usr/bin/python
# -*- coding:utf-8 -*-

import MySQLdb
import hashlib
import json
import codecs
const={
    "AH": "安徽",
    "BJ": "北京",
    "CN": "国家工商总局",
    "CQ": "重庆",
    "FJ": "福建",
    "GD": "广东",
    "GS": "甘肃",
    "GX": "广西",
    "GZ": "贵州",
    "HAINAN": "海南",
    "HB": "河北",
    "HLJ": "黑龙江",
    "HN": "河南",
    "HUBEI": "湖北",
    "HUNAN": "湖南",
    "JL": "吉林",
    "JS": "江苏",
    "JX": "江西",
    "LN": "辽宁",
    "NMG": "内蒙古",
    "NX": "宁夏",
    "QH": "青海",
    "SC": "四川",
    "SD": "山东",
    "SH": "上海",
    "SHANXI": "陕西",
    "SX": "山西",
    "TJ": "天津",
    "XJ": "新疆",
    "XZ": "西藏",
    "YN": "云南",
    "ZJ": "浙江",
    '':"-"
}

def readfile(path,name):
    filename = "%s/%s"%(path,name)
    properties= {"host": "192.168.0.196", "user": "finder", "passwd": "xsycommercial123", "db": "prism1"}
    db = MySQLdb.connect(**properties)
    m2 = hashlib.md5()

    fb = codecs.open("%s/%s_base.txt"%(path,name),'a',"utf-8")
    freg = codecs.open("%s/%s_reglocation.txt"%(path,name),'a',"utf-8")
    fcap = codecs.open("%s/%s_registeredCapital.txt"%(path,name),'a',"utf-8")
    fphone = codecs.open("%s/%s_phoneNumber.txt"%(path,name),'a',"utf-8")
    cursor = db.cursor()
    with open("%s/%s.txt"%(path,name),'r') as fr:
        for name in fr.readlines():
            name = (name.strip('\n')).strip('\r').strip()

            m2.update(name)
            q = "select json from companyinfo where name=\"%s\""%m2.hexdigest()
            cursor.execute(q)
            data = cursor.fetchone()

            name = unicode(name, "utf-8")
            if not data:
                fb.write("%i-%s-%s\n" % (0, name, "-"))
                freg.write("%i-%s-%s\n" % (0, name, "-"))
                fcap.write( "%i-%s-%s\n" % (0, name, "-"))
                fphone.write("%i-%s-%s\n" % (0, name, "-"))
                continue

            jstr = json.loads(data[0])
            id = jstr['id']
            base =jstr['base'] if jstr.has_key('base') else ''
            fb.write("%i-%s-%s\n"%(id,name,unicode(const[base.upper()],"utf-8")))
            freg.write( "%i-%s-%s\n"%(id,name,jstr['regLocation'] if jstr.has_key('regLocation') else ''))
            fcap.write("%i-%s-%s\n"%(id,name,jstr['registeredCapital'] if jstr.has_key('registeredCapital') else ''))

            if jstr.has_key('annualReportList'):
                if len(jstr['annualReportList'])>0:
                    if (jstr['annualReportList'][-1]).has_key('baseInfo') and (jstr['annualReportList'][-1]['baseInfo']).has_key('phoneNumber'):
                        fphone.write("%i-%s-%s\n"%(id,name,jstr['annualReportList'][-1]['baseInfo']['phoneNumber']))
                        continue
            fphone.write("%i-%s-%s\n"%(id,name,""))

def test():
    print const['sx'.upper()]
    src = '{"regStatus":"存续（在营、开业、在册）","annualReportList":[{"baseInfo":{"employeeNum":"企业选择不公示","totalAssets":"企业选择不公示","totalProfit":"企业选择不公示","totalLiability":"企业选择不公示","companyName":"广河县江东皮业有限公司","postcode":"731300","totalSales":"企业选择不公示","retainedProfit":"企业选择不公示","totalTax":"企业选择不公示","reportYear":"2013","totalEquity":"企业选择不公示","creditCode":"91622924561144100K","phoneNumber":"13399300016","postalAddress":"广河县祁家集乡黄赵家村","primeBusProfit":"企业选择不公示","manageState":"存续","email":"732444325@qq.com"},"shareholderList":[{"paidType":"2000年8月15日","subscribeAmount":"20","subscribeType":"货币","subscribeTime":966297600000,"investorName":"马俊虎","paidTime":966297600000,"paidAmount":"20"},{"paidType":"2000年8月15日","subscribeAmount":"80","subscribeType":"货币","subscribeTime":966297600000,"investorName":"马春贵","paidTime":966297600000,"paidAmount":"80"},{"paidType":"2000年8月15日","subscribeAmount":"10","subscribeType":"货币","subscribeTime":966297600000,"investorName":"马春伟","paidTime":966297600000,"paidAmount":"10"}]},{"baseInfo":{"employeeNum":"企业选择不公示","totalAssets":"企业选择不公示","totalProfit":"企业选择不公示","totalLiability":"企业选择不公示","companyName":"广河县江东皮业有限公司","postcode":"731300","totalSales":"企业选择不公示","retainedProfit":"企业选择不公示","totalTax":"企业选择不公示","reportYear":"2014","totalEquity":"企业选择不公示","creditCode":"91622924561144100K","phoneNumber":"13399300016","postalAddress":"广河县祁家集乡黄赵家村","primeBusProfit":"企业选择不公示","manageState":"存续","email":"732444325@qq.com"},"shareholderList":[{"paidType":"2000年8月15日","subscribeAmount":"80","subscribeType":"货币","subscribeTime":966297600000,"investorName":"马春贵","paidTime":966297600000,"paidAmount":"80"},{"paidType":"2000年8月15日","subscribeAmount":"20","subscribeType":"货币","subscribeTime":966297600000,"investorName":"马俊虎","paidTime":966297600000,"paidAmount":"20"},{"paidType":"2000年8月15日","subscribeAmount":"10","subscribeType":"货币","subscribeTime":966297600000,"investorName":"马春伟","paidTime":966297600000,"paidAmount":"10"}]},{"baseInfo":{"employeeNum":"企业选择不公示","totalAssets":"企业选择不公示","totalProfit":"企业选择不公示","totalLiability":"企业选择不公示","companyName":"广河县江东皮业有限公司","postcode":"731300","totalSales":"企业选择不公示","retainedProfit":"企业选择不公示","totalTax":"企业选择不公示","reportYear":"2015","totalEquity":"企业选择不公示","creditCode":"91622924561144100K","phoneNumber":"13629309988","postalAddress":"广河县祁家集乡黄赵家村","primeBusProfit":"企业选择不公示","manageState":"开业","email":"ghah8591@126.com"},"shareholderList":[{"paidType":"2010年11月1日","subscribeAmount":"5","subscribeType":"货币","subscribeTime":1288569600000,"investorName":"马春伟","paidTime":1288569600000,"paidAmount":"5"},{"paidType":"2010年11月1日","subscribeAmount":"100","subscribeType":"货币","subscribeTime":1288569600000,"investorName":"马春贵","paidTime":1288569600000,"paidAmount":"100"},{"paidType":"2010年11月1日","subscribeAmount":"5","subscribeType":"货币","subscribeTime":1288569600000,"investorName":"马俊虎","paidTime":1288569600000,"paidAmount":"5"}]},{"baseInfo":{"employeeNum":"企业选择不公示","totalAssets":"企业选择不公示","totalProfit":"企业选择不公示","totalLiability":"企业选择不公示","companyName":"广河县江东皮业有限公司","postcode":"731300","totalSales":"企业选择不公示","retainedProfit":"企业选择不公示","totalTax":"企业选择不公示","reportYear":"2016","totalEquity":"企业选择不公示","creditCode":"91622924561144100K","phoneNumber":"0930-5621987","postalAddress":"甘肃省临夏州广河县祁家集镇黄赵家村","primeBusProfit":"企业选择不公示","manageState":"开业","email":"285816892@qq.com"},"shareholderList":[{"paidType":"2010年11月1日","subscribeAmount":"10","subscribeType":"货币","subscribeTime":1288569600000,"investorName":"马春伟","paidTime":1288569600000,"paidAmount":"10"},{"paidType":"2010年11月1日","subscribeAmount":"80","subscribeType":"货币","subscribeTime":1288569600000,"investorName":"马春贵","paidTime":1288569600000,"paidAmount":"80"},{"paidType":"2010年11月1日","subscribeAmount":"20","subscribeType":"货币","subscribeTime":1288569600000,"investorName":"马俊虎","paidTime":1288569600000,"paidAmount":"20"}]}],"flag":1,"regCapital":"110.000000万人民币","industry":"皮革、毛皮、羽毛及其制品和制鞋业","updateTimes":1517310698000,"type":1,"legalPersonName":"马春贵","regNumber":"622924200001315","creditCode":"91622924561144100K","registeredCapital":1100000.000000,"mortgageList":[],"approvedTime":1474588800000,"fromTime":1288569600000,"companyOrgType":"有限责任公司(自然人投资或控股)","term":"2010-11-01 08:00:00.0至2020-11-01 08:00:00.0","currency":"人民币","id":71422344,"orgNumber":"561144100","sourceFlag":"http://qyxy.baic.gov.cn/","correctCompanyId":71422344,"comChanInfoList":[],"actualCapital":"","estiblishTime":1288569600000,"companyType":0,"regInstitute":"广河县工商行政管理局","category_code":"193","investorListAll":[{"amount":0.0,"companyId":71422344,"name":"马春贵","id":2569067,"type":"自然人股东"},{"amount":0.0,"companyId":71422344,"name":"马俊虎","id":1868295,"type":"自然人股东"},{"amount":0.0,"companyId":71422344,"name":"马春伟","id":481306,"type":"自然人股东"}],"businessScope":"轻革鞣制、牛羊原毛原皮的收购、销售、加工、皮衣、皮鞋、帽子、皮带零售。","regLocation":"广河县祁家集乡黄赵家村","comAbnoInfoList":[],"staffListAll":[{"companyId":71422344,"name":"马俊虎","id":1868295,"type":101,"typeJoin":"监事"},{"companyId":71422344,"name":"马春伟","id":481306,"type":101,"typeJoin":"监事"},{"companyId":71422344,"name":"马春贵","id":2569067,"type":101,"typeJoin":"执行董事"}],"legalPersonId":2569067,"companyId":71422344,"parent_id":0,"name":"广河县江东皮业有限公司","updatetime":1517310698000,"base":"gs"}'
    import json
    json = json.loads(src)
    print json['base'],json['regLocation'],json['registeredCapital'],json['annualReportList'][-1]['baseInfo']['phoneNumber']

readfile('./','names')
# test()