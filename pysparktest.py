import pyspark
from pyspark import SparkContext
sc = SparkContext()

def convertlist(lst):
	newlst = [0 for x in range(len(lst))]
	for i in range(len(lst)):
		newlst[i] = lst[i] + 2
	return newlst

b = sc.textFile("test.csv",2)
print b.glom().collect()
c = b.map(lambda x : map(int, x.split(',')))
print c.glom().collect()
d = c.map(convertlist)
print d.glom().collect()