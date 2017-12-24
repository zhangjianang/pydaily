#!/usr/bin/python
import os

#os.system("dir")

class Parent(object):
    """docstring for Parent."""
    def __init__(self, arg):
        super(Parent, self).__init__()
        self.arg = arg



def fun(n):
    if n== 1:
        return n
    else:
        return n*fun(n-1)


#print fun(10000)


def newfun(num,product):
    if num ==1:
        return product
    else:
        return newfun(num-1,num*product)

def backfun(n):
    return newfun(n,1)



def listfun(n):
    l1=range(n)
    t1 = ("we",1,"are","the",3)
    d1 = {"name":"ang","gender":"male","age":10}
    for i,j in d1.iteritems():
        print i,j
    for v in d1.itervalues():
        print v

def gen():
    l1 = [x for x in os.listdir("../")]
    l2  = [x*x for x in range(5) if x !=2]
    g1 = (x for x in os.listdir("../"))

    for v in genfun(7):
        print v


def genfun(num):
    n,a,b = 0,0,1
    while n < num :
        yield b
        tmp = b
        b = a+b
        a = tmp
        #a, b = b, a+b
        n += 1

gen()
