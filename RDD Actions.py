# ------------------------------------------------------------------------
# Action - DAG gets finalized
# ------------------------------------------------------------------------
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName("RDD Actions").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

li = ["Vaishnavi", "Aravind", "Arunraj", "Devi", "David", "Arjun", "Aravind"]
rdd = sc.parallelize(li)

print("rdd: ", rdd.collect())
# rdd:  ['Vaishnavi', 'Aravind', 'Arunraj', 'Devi', 'David', 'Arjun', 'Aravind']

print("rdd first: ", rdd.first())
# rdd first:  Vaishnavi

print("rdd Count: ", rdd.count())
# rdd Count:  7

rdd1 = rdd.filter(lambda x: x != 'Devi').map(lambda a: (a[0], a))
print("rdd1: ", rdd1.collect())
# rdd1:  [('V', 'Vaishnavi'), ('A', 'Aravind'), ('A', 'Arunraj'), ('D', 'David'), ('A', 'Arjun'), ('A', 'Aravind')]

print("rdd1 take(5)", rdd1.take(5))
# rdd1 take(5) [('V', 'Vaishnavi'), ('A', 'Aravind'), ('A', 'Arunraj'), ('D', 'David'), ('A', 'Arjun')]

print("rdd1 takeSample(5)", rdd1.takeSample(True, 5))
# OP: rdd1 takeSample(5) [('A', 'Arunraj'), ('A', 'Arjun'), ('V', 'Vaishnavi'), ('A', 'Aravind'), ('D', 'David')]

print("rdd1 takeOrdered(5)", rdd1.takeOrdered(5))
# rdd1 takeOrdered(5) [('A', 'Aravind'), ('A', 'Aravind'), ('A', 'Arjun'), ('A', 'Arunraj'), ('D', 'David')]

res = rdd1.reduce(lambda x, y: x+y)
print("rdd1 reduce", res)
# rdd1 reduce ('V', 'Vaishnavi', 'A', 'Aravind', 'A', 'Arunraj', 'D', 'David', 'A', 'Arjun', 'A', 'Aravind')

print("rdd1 reduce op type", type(res))
# rdd1 reduce op type <class 'tuple'>

rddnumeric = sc.parallelize([1, 4, 2, 7, 12])
print("rddnumeric takeOrdered(5) ", rddnumeric.takeOrdered(5))
# rddnumeric takeOrdered(5)  [1, 2, 4, 7, 12]

print("rddnumeric takeOrdered Reverse(5) ", rddnumeric.takeOrdered(5, key=lambda x: -x))
# rddnumeric takeOrdered Reverse(5)  [12, 7, 4, 2, 1]

res = rddnumeric.reduce(lambda x, y: x+y)
print("rdd1 reduce", res)
# rdd1 reduce 26

print("rdd1 reduce op type", type(res))
# rdd1 reduce op type <class 'int'>

print("rddnumeric.min()", rddnumeric.min())
# rddnumeric.min() 1

print("rddnumeric.max()", rddnumeric.max())
# rddnumeric.max() 12

print("rddnumeric.sum()", rddnumeric.sum())
# rddnumeric.sum() 26

print("rddnumeric.stdev()", rddnumeric.stdev())
# rddnumeric.stdev() 3.9698866482558417

print("rddnumeric.stats()", rddnumeric.stats())
# rddnumeric.stats() (count: 5, mean: 5.2, stdev: 3.9698866482558417, max: 12, min: 1)

print("rddnumeric.sample(withReplacement=True, fraction=0.1",
      rddnumeric.sample(withReplacement=True, fraction=0.2).collect())
# rddnumeric.sample(withReplacement=True, fraction=0.1 [7]

rdd2 = rdd.map(lambda x: (x[0], x))

print("rdd2 collect: ", rdd2.collect())
# rdd2 collect:  [('V', 'Vaishnavi'), ('A', 'Aravind'), ('A', 'Arunraj'), ('D', 'Devi'), ('D', 'David'), ('A', 'Arjun'), ('A', 'Aravind')]

print("rdd2 Partitions Count: ", rdd2.getNumPartitions())
# rdd2 Partitions Count:  8
# 8 because of (data locality. Depends on process local/ Node Local/ rack local
# process - depends on the number of cores in one node. if local[*] is used.
# https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/performance_optimization/data_locality.html

print("rdd2 countbyKey: ", rdd2.countByKey())
# rdd2 countbyKey:  defaultdict(<class 'int'>, {'V': 1, 'A': 4, 'D': 2})

print("rdd2 countByValue: ", rdd2.countByValue())
# rdd2 countByValue:  defaultdict(<class 'int'>, {('V', 'Vaishnavi'): 1, ('A', 'Aravind'): 2, ('A', 'Arunraj'): 1,
# ('D', 'Devi'): 1, ('D', 'David'): 1, ('A', 'Arjun'): 1})




