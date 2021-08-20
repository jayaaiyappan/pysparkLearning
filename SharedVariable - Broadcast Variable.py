# https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName("RDD Actions").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Shared Variables
# Broadcast (This is immutable) Use Case : broadcasting dictionary variables.
# disadv :  network IO

# -------------------------------------------------
# broadcasting variable
# -------------------------------------------------
bc = sc.broadcast(10)
# print("broadcast", bc.value)
# broadcast 10


def g(x):
    return x + bc.value


li = [1, 2, 3, 4, 5]
rdd = sc.parallelize(li)
# print("rdd", rdd.collect())
# rdd [1, 2, 3, 4, 5]
rdd1 = rdd.map(lambda x: g(x))
# print("rdd1", rdd1.collect())
# rdd1 [11, 12, 13, 14, 15]

# -------------------------------------------------
# use broadcast variables on RDD
# -------------------------------------------------
states = {"NY": "New York", "CA": "California", "FL": "Florida"}
broadcastStates = sc.broadcast(states)

data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA"),
        ("Maria", "Jones", "USA", "FL")
        ]

rdd = sc.parallelize(data)


def state_convert(code):
    return broadcastStates.value[code]


result = rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).collect()
# print("broadcasting on rdd result:", result)
# broadcasting on rdd result: [('James', 'Smith', 'USA', 'California'), ('Michael', 'Rose', 'USA', 'New York'),
# ('Robert', 'Williams', 'USA', 'California'), ('Maria', 'Jones', 'USA', 'Florida')]

# -----------------------------end-------------------------------------------------
# ---------------------------------------------------------------------------------
# use broadcast variables on DataFrame
# ---------------------------------------------------------------------------------

columns = ["firstname", "lastname", "country", "state"]
df = spark.createDataFrame(data=data, schema=columns)
# df.printSchema()
# root
#  |-- firstname: string (nullable = true)
#  |-- lastname: string (nullable = true)
#  |-- country: string (nullable = true)
#  |-- state: string (nullable = true)

# df.show(truncate=False)
# +---------+--------+-------+-----+
# |firstname|lastname|country|state|
# +---------+--------+-------+-----+
# |James    |Smith   |USA    |CA   |
# |Michael  |Rose    |USA    |NY   |
# |Robert   |Williams|USA    |CA   |
# |Maria    |Jones   |USA    |FL   |
# +---------+--------+-------+-----+

result = df.rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).toDF(columns)
# result.show(truncate=False)

# +---------+--------+-------+----------+
# |firstname|lastname|country|state     |
# +---------+--------+-------+----------+
# |James    |Smith   |USA    |California|
# |Michael  |Rose    |USA    |New York  |
# |Robert   |Williams|USA    |California|
# |Maria    |Jones   |USA    |Florida   |
# +---------+--------+-------+----------+
