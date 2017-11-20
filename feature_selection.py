import pyspark
from pyspark import SparkContext
import time
sc = SparkContext()

import math
from random import *
eps = 0.00001

def convertlist(lst):
  newlst = [0 for x in range(len(lst))]
  for i in range(len(lst)):
    newlst[i] = lst[i] + 3
  return newlst
# data_RDD  = sc.parallelize([[1,2,3,2],[2,4,5,4],[1,6,7,2],[1,8,9,3],[1,3,4,4],[2,5,6,3],[2,7,8,4],[3,9,1,4]], 2)
inst = 63
feat = 2000
pp = sc.textFile("write_test_colon.csv",2)
qq = pp.map(lambda x : map(int, x.split(',')))
data_RDD = qq.map(convertlist)
print data_RDD.glom().collect()
# a = [[0 for x in range(0,feat)] for y in range(0,inst)]
# for i in range(0,feat-1):
#   for j in range(0,inst):
#     a[j][i] = randint(1,9)
# for i in range(0,inst):
#   a[i][feat-1] = (a[i][2]  + a[i][3])%10
# for i in range(0,inst):
#   if a[i][feat-1] is 0 :
#     a[i][feat-1] = randint(1,9)
# data_RDD = sc.parallelize(a, 2)
# print data_RDD.glom().collect()
# print data_RDD.glom().collect()


def transpose(nf):
  def _transpose(index, iterator):
    part = list(iterator)
    Matrix = [[0 for x in range(len(part))] for y in range(nf)]
    for j in range(len(part)):
      for i in range(nf):
        Matrix[i][j] = part[j][i]
    for k in range(nf):
      yield (k, (index, Matrix[k]))
  return _transpose



def columnarTransformation(nf, npart, rdd): 
  part_RDD = rdd.mapPartitionsWithIndex(transpose(nf))
  sorted_RDD = part_RDD.sortByKey(ascending = True)
  partitioned_RDD = sorted_RDD.repartition(2)
  return partitioned_RDD


def get2DHist(rdd, yind, bycol):
  ycol = bycol.value
  def _get2DHist(iterator):
    part = list(iterator)
    ysize = 10
    for (k, (block, v)) in part:
      xsize = 10
      m = [[0 for x in range(xsize)] for y in range(ysize)]
      for e in range(0,len(v)):
        i = v[e]
        j = ycol[block][1][e]
        m[i][j] = m[i][j] + 1
      yield  (k ,m) 
  return _get2DHist    


def sumList(a, b):
  r = len(a)
  c = len(a[0])
  Matrix  = [[0 for x in range(r)] for y in range(c)]
  for i in range(0,r):
    for j in range(0,c):
      Matrix[i][j] = a[i][j] + b[i][j]
  return Matrix


def get2DHistogram(rdd, yind, bycol):
#   print rdd.glom().collect()
  part_RDD = rdd.mapPartitions(get2DHist(rdd, yind, bycol))
#   print part_RDD.glom().collect()
  return (part_RDD.reduceByKey(sumList))


def computeMI(hist_rdd, ni):
  def _computeMI((k,v)):
    r = len(v)
    c = len(v[0])
    mi = 0
    matrix = [[0.0 for x in range(r)] for y in range(c)]
    for i in range(0,r):
      for j in range(0,c):
        matrix[i][j] = v[i][j]/float(ni)
    my = [0.0 for x in range(r)]
    for i in range(0, r):
      sum = 0.0
      for j in range(0, c):
        sum = sum + matrix[i][j]
      my[i] = sum
    mx = [0 for y in range(c)]
    for j in range(0,c):
      sum = 0.0
      for i in range(0,r):
        sum = sum + matrix[i][j]
      mx[j] = sum
    for i in range(0,r):
      for j in range(0,c):
        px = mx[i]
        py = my[j]
        pxy = matrix[i][j]
        if abs(px - 0.0)>=eps and abs(py - 0.0)>=eps and abs(pxy - 0.0)>=eps:
          mi  = mi + pxy*(math.log(pxy/(px*py), 2))
    return (k, mi)
  return _computeMI


def computeMutualInfo(hist_rdd, ni):
  mi_rdd = hist_rdd.map(computeMI(hist_rdd, ni))
  return mi_rdd


def computeRR(rdd, yind, ni):
  ycol = rdd.lookup(yind)
  bycol = sc.broadcast(ycol)
  hist_rdd = get2DHistogram(rdd, yind, bycol).sortByKey(ascending = True)
  mi_rdd = computeMutualInfo(hist_rdd, ni)
  return mi_rdd



def maxValue(set):
  def _maxValue(iterator):
    dict = list(iterator)
    mv = -1
    mi = -1
    for (a, b) in  dict:
      if(b > mv and a not in set):
        mv = b
        mi = a
    return [(mi, mv)]
  return _maxValue


def calculateMax(rdd, set):
  mx = rdd.mapPartitions(maxValue(set))
  var = mx.collect()
  mv = -1
  mi = -1
  for (a, b) in var:
    if (b > mv) : 
      mv = b
      mi = a
  return mi


#main algorithm
def main_fs(d, ni, tf, ns, npart, cindex):
  dc = columnarTransformation(tf, npart, d)
  rel_rdd = computeRR(dc, cindex, ni)
  set = []
  set.append(cindex)
  pbest = calculateMax(rel_rdd, set)
  set.append(pbest)
  while( len(set) <= ns ):
    red_rdd = computeRR(dc, pbest, ni)
    temp_rdd = red_rdd.map(lambda (a,b) : (a, b/float(tf)))
    merge = sc.union([rel_rdd, temp_rdd])
    rel_rdd = merge.reduceByKey(lambda a, b : a - b)
    pbest = calculateMax(rel_rdd, set)
    set.append(pbest)
  return set

ff = open("write_time.txt",'w')
qq = open("write_feat.txt",'w')
for i in range(19):
  start = time.time()
  number = i*10
  selected = main_fs(data_RDD, inst, feat , number, 2, feat-1)
  end = time.time()
  ff.write(str(end - start) + "\n")
  qq.write(str(selected) + "\n")
