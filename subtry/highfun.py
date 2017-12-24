#!/usr/bin/python
#-*- encoding:UTF-8 -*-

myl = ['adam', 'LISA', 'barT']
def fun(istr):
    tmp = istr.lower()
    return tmp[0].upper()+tmp[1:len(tmp)]
#print map(fun,myl)

def prod(*num):
    return reduce(lambda x,y:x*y,num)
l1 = [1,2,3,4]
print prod(*l1)
