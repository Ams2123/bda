# Spark Cheat sheet

## how to create a sparkSession: 

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .appName("Example") \
        .getOrCreate()
```

## how to create a rdd and its function

### from existing collection

```python
data = [10, 20, 30, 40, 50]
rdd = spark.sparkContext.parallelize(data)
```

### from external source

```python
rdd = spark.sparkContext.textFile("input.txt")
```

### applying a transformation 

```python
rdd2 = rdd1.map(lambda x: x * x)
```

### filtering an rdd

```python
even_rdd = rdd.filter(lambda x: x % 2 == 0)
```

### flatMap trnasformation

```python
words = lines.flatMap(lambda line: line.split())
```

> Consider the input: **{“big data”,“apache spark”}**.  
Using map() with a split operation may conceptually produce: **{[big, data], [apache,spark]}**.  
Using flatMap() produces: **{big, data,apache,spark}**

## actions

### count()  
returns the number of elements in an RDD
```python
n = rdd.count()
```
### collect()  
 returns all elements of an RDD to the Driver
```python
rdd.collect()
```
### first() 
 returns the first element of an RDD
```python
rdd.first()
```
### take()  
 returns a limited number of elements.
```python
rdd.take(5)
```
### reduce()  
aggregates the elements of the RDD using a specified commutative and associative binary operator.
```python
rdd.reduce(lambda a, b: a + b)
rdd.reduceByKey(lambda a, b: a + b) # returns as a new rdd
rdd.reduceByKeyLocally(lambda a, b: a + b) # returns it as python dicitionary
```
### saveAsTextFile()  
```python
rdd.saveAsTextFile("path/to/output_directory")
```
