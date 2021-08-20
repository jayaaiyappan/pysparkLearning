# for running spark in pycharm in windows follow
# https://kaizen.itversity.com/setup-spark-development-environment-pycharm-and-python/

from pyspark.context import SparkContext
from pyspark.context import SparkConf

# sc = SparkContext(master="local[*]", appName="RDD Examples").getOrCreate()
custom_config = SparkConf()
custom_config.setMaster("local[*]").setAppName("Spark Examples")
# custom_config.set("spark.executor.memory", "2g")
sc = SparkContext(conf=custom_config)
sc.setLogLevel("ERROR")

src_list = list(range(0, 10))

# create RDD from Parallel Collections
src_list_rdd = sc.parallelize(src_list)
print("source list", src_list_rdd.take(10))
# source list [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

# ----------------------------------------------------------------------------------
# --------------------------Narrow Transformations----------------------------------
# ----------------------------------------------------------------------------------
# 1. Filter
filter_odd_rdd = src_list_rdd.filter(lambda x: x % 2 == 0)
print("filter_odd", filter_odd_rdd.take(5))
# filter_odd [0, 2, 4, 6, 8]

# 2. Map
map_even_mod10_rdd = filter_odd_rdd.map(lambda x: (x % 10, x))
print("map_even_mod10", map_even_mod10_rdd.take(5))
# map_even_mod10 [(0, 0), (2, 2), (4, 4), (6, 6), (8, 8)]

# 3. FlatMap
flatmap_even_mod10_rdd = filter_odd_rdd.flatMap(lambda x: (x % 10, x))
print("flatmap_even_mod10", flatmap_even_mod10_rdd.take(5))
# flatmap_even_mod10 [0, 0, 2, 2, 4]

# 4. Union
rdd_union_even_flatmap_mod10 = filter_odd_rdd.union(flatmap_even_mod10_rdd)
print("rdd_union_even_mod10", rdd_union_even_flatmap_mod10.take(10))
# rdd_union_even_mod10 [0, 2, 4, 6, 8, 0, 0, 2, 2, 4]

# ----------------------------------------------------------------------------------
# --------------------------Wide Transformations----------------------------------
# ----------------------------------------------------------------------------------

# 1. Distinct
rdd_distinct_even_flatmap_mod10 = rdd_union_even_flatmap_mod10.distinct()
print("rdd_distinct_even_flatmap_mod10", rdd_distinct_even_flatmap_mod10.take(10))
# rdd_distinct_even_flatmap_mod10 [0, 2, 4, 6, 8]

# 2. GroupBy
rdd_groupby = src_list_rdd.groupBy(lambda x: x % 2)
op = rdd_groupby.collect()
print(op)
# [(0, <pyspark.resultiterable.ResultIterable object at 0x000001FD9A804D60>), (1,
# <pyspark.resultiterable.ResultIterable object at 0x000001FD9A84E280>)]
print([(i, list(j)) for i, j in op])
# [(0, [0, 2, 4, 6, 8]), (1, [1, 3, 5, 7, 9])]
# same loop without comprehension
for i, j in op:
    print((i, list(j)))
# (0, [0, 2, 4, 6, 8])
# (1, [1, 3, 5, 7, 9])

# 3. Reduce -- intermediate combiner. i.e each node/executor
# TODO

# 4. join - Performs a hash join across the cluster.
x = sc.parallelize([("a", 1), ("b", 4)])
y = sc.parallelize([("a", 2), ("a", 3)])
innerjoin = x.join(y)
print(innerjoin.collect())
# [('a', (1, 2)), ('a', (1, 3))]
print(innerjoin.toDebugString())
# b'(16) PythonRDD[13] at collect at C:\\Users\\Jaya\\PycharmProjects\\pysparkLearning\\RDD Transformations.py:58 []\n
# |   MapPartitionsRDD[12] at mapPartitions at PythonRDD.scala:133 []\n
# |   ShuffledRDD[11] at partitionBy at NativeMethodAccessorImpl.java:0 []\n
# +-(16) PairwiseRDD[10] at join at C:\\Users\\Jaya\\PycharmProjects\\pysparkLearning\\RDD Transformations.py:57 []\n
# |   PythonRDD[9] at join at C:\\Users\\Jaya\\PycharmProjects\\pysparkLearning\\RDD Transformations.py:57 []\n
# |   UnionRDD[8] at union at NativeMethodAccessorImpl.java:0 []\n
# |   PythonRDD[6] at RDD at PythonRDD.scala:53 []\n
# |   ParallelCollectionRDD[4] at readRDDFromFile at PythonRDD.scala:262 []\n
# |   PythonRDD[7] at RDD at PythonRDD.scala:53 []\n
# |   ParallelCollectionRDD[5] at readRDDFromFile at PythonRDD.scala:262 []'
leftouterjoin = x.leftOuterJoin(y)
print(leftouterjoin.collect())
# [('b', (4, None)), ('a', (1, 2)), ('a', (1, 3))]
print(leftouterjoin.toDebugString())
# b'(16) PythonRDD[13] at collect at C:\\Users\\Jaya\\PycharmProjects\\pysparkLearning\\RDD Transformations.py:72 []\n
# |   MapPartitionsRDD[12] at mapPartitions at PythonRDD.scala:133 []\n
# |   ShuffledRDD[11] at partitionBy at NativeMethodAccessorImpl.java:0 []
# \n +-(16) PairwiseRDD[10] at leftOuterJoin at C:\\Users\\Jaya\\PycharmProjects\\pysparkLearning\\RDD Transformations.py:71 []\n
# |   PythonRDD[9] at leftOuterJoin at C:\\Users\\Jaya\\PycharmProjects\\pysparkLearning\\RDD Transformations.py:71 []\n
# |   UnionRDD[8] at union at NativeMethodAccessorImpl.java:0 []\n
# |   PythonRDD[6] at RDD at PythonRDD.scala:53 []\n
# |   ParallelCollectionRDD[4] at readRDDFromFile at PythonRDD.scala:262 []\n
# |   PythonRDD[7] at RDD at PythonRDD.scala:53 []\n
# |   ParallelCollectionRDD[5] at readRDDFromFile at PythonRDD.scala:262 []'

# 5. sortBy

# 6. GroupByKey

# 7. ReduceByKey
rdd_key = sc.parallelize(["apple", "air", "car"])
rdd_map_tuple = rdd_key.map(lambda y: (y[0], y))
print("rdd_map_tuple", rdd_map_tuple.collect())
# rdd_map_tuple [('a', 'apple'), ('a', 'air'), ('c', 'car')]
rdd_reducebyky = rdd_map_tuple.reduceByKey(lambda x, y: x + y)
print("rdd_reducebyky", rdd_reducebyky.collect())
# rdd_reducebyky [('a', 'appleair'), ('c', 'car')]

# 8.FoldByKey

# 9. Zip TO NOTE: Can only zip RDDs with same number of elements in each partition
rdd_odd = src_list_rdd.filter(lambda x: x % 2 != 0)
print("rdd_odd", rdd_odd.take(5))

rdd_zip = filter_odd_rdd.zip(rdd_odd)
print("rdd_zip", rdd_zip.collect())

# 10. ZipwithIndex
print("No of Partition", filter_odd_rdd.getNumPartitions())
rdd_zip_withIndex = filter_odd_rdd.zipWithIndex()
print("rdd_zip_withIndex", rdd_zip_withIndex.collect())

# you can goto folder that contains the py file and type cmd and then can run the code using spark-submit <confg
# param> py file name mapPartition
rdd3 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
print("RDD3 Output")
print(rdd3.glom().collect())


# RDD3 Output
# [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]


def f(x): yield sum(x)


print("Map Partition Output")
print(rdd3.mapPartitions(f).collect())

# Map Partition Output
# [15, 40]
# Fold By Key


def f1(x, y): return x+y


rdd4 = rdd3.map(lambda x: (x % 2, x))
print(rdd4.foldByKey(0, f1).collect())
# [(0, 30), (1, 25)]
