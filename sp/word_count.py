#!/usr/bin/python
#encoding:utf8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import re,os

from pyspark import SparkContext
sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///F:/ang.log")
wordCount = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a, b : a + b)
print(wordCount.foreach(lambda x:x))