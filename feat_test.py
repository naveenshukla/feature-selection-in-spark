import pyspark
from pyspark import SparkContext
sc = SparkContext()

d = sc.textFile("write_test_colon.csv",2)
e = d.map(lambda x: map(int , x.split(',')))
print e.glom().collect()