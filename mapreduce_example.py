from pyspark import SparkContext


sc = SparkContext("local", "Example")

mylist = [1, 5, 10, 2, 20, 3, 5]
rdd = sc.parallelize(mylist)

rdd_rounded = rdd.map(lambda v: round(v))
result = rdd_rounded.reduce(lambda v1, v2: v1 + v2)

print(result)