#!/usr/bin/python
import re
istr = "(414L,)"

grp = re.search('([0-9]+)',istr,re.M|re.I)
allsum = 0
if grp:
    allsum += int(grp.group())
    print grp.group()
    print allsum
else:
    print "no match"

if False:
    line = "Cats are smarter than dogs"

    matchObj = re.match( r'(.*) are (.*?) .*', line, re.M|re.I)

    if matchObj:
       print "matchObj.group() : ", matchObj.group()
       print "matchObj.group(1) : ", matchObj.group(1)
       print "matchObj.group(2) : ", matchObj.group(2)
    else:
       print "No match!!"


s1 = [1,2,3]
s2 = [4,5]
s1 +=s2
print s1
