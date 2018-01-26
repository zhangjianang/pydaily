#!/usr/bin/python
# -*- coding:utf-8 -*-

myl = ['adam', 'LISA', 'barT']
def fun(istr):
    tmp = istr.lower()
    return tmp[0].upper()+tmp[1:len(tmp)]
#print map(fun,myl)

def prod(*num):
    return reduce(lambda x,y:x*y,num)
l1 = [1,2,3,4]
#print prod(*l1)


#闭包
#返回闭包时牢记的一点就是：返回函数不要引用任何循环变量，或者后续会发生变化的变量。
def  efun():
    f =[]
    for i in range(1,4):
        def fun(tmp):
            def ifun():
                return tmp*tmp
            return ifun
        f.append(fun(i))
    return f

#匿名函数
def nfun():
    f = []
    for i in range(1,4):
        def fun(tmp):
            return lambda :tmp*tmp
        f.append(fun(i))
    return f

f1,f2,f3 = nfun()
print f1()
print f2()
print f3()

""""""
print u"#偏函数"
def int2(num):
    return int(num,base=2)
print int2("110")

import functools

int2 = functools.partial(int,base=2)
print int2("101")
