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

f1,f2,f3 = efun()
print f1()
print f2()
print f3()
