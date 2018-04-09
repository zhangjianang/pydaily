class Point:
    def __init__(self,name):
        self.name = name

    def __del__(self):
        print self.name,"delete"
    def __str__(self):
        print " name: ",self.name

class Child(Point):
    def __init__(self,name,age):
        Point(name)
        self.age=age
    def __str__(self):
        print ' age: ',self.age


if __name__ == "__main__":
    p1 = Point("first")
    p2 = Point("first2")
    p3 = Point("first3")

    print id(p1),id(p2),id(p3)
    del p1,p2,p3

    print "Point.__doc__:", Point.__doc__
    print "Point.__name__:", Point.__name__
    print "Point.__module__:", Point.__module__
    print "Point.__bases__:", Point.__bases__
    print "Point.__dict__:", Point.__dict__

    c = Child("ang",18)
    str(c)