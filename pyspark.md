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

## DataFrame
### creating from a collection
```python
data = [
        (101, "Chennai", "Laptop", 55000),
        (102, "Delhi", "Mobile", 30000),
        (103, "Mumbai", "Tablet", 22000)
       ]
columns = ["TransactionID", "City", "Product", "Amount"]
salesDF = spark.createDataFrame(data, columns)
```
### creating from a csv file
```python
salesDF = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("sales.csv")
```
### Displaying data
```python
salesDF.show()
```
### Selecting Columns
```python
salesDF.select("City", "Amount").show()
```
### Filtering rows
```python
```python
salesDF.select("City", "Amount").show()
#or equivalently
highValueDF = salesDF.filter("Amount > 25000")
```
### Adding a new column
```python
from pyspark.sql.functions import col
newDF = salesDF.withColumn(
                "Tax",
                col("Amount") * 0.18
        )
```
### grouping
```python
salesDF.groupBy("City").sum("Amount").show()
```
### common data operations 
| Operation | Purpose |
| :--- | :--- |
| `select()` | Select specified columns. |
| `filter()` / `where()` | Select rows satisfying a condition. |
| `withColumn()` | Create or replace a column. |
| `groupBy()` | Group rows according to specified columns. |
| `orderBy()` | Order rows according to one or more columns. |
| `join()` | Combine rows from different DataFrames. |
| `distinct()` | Remove duplicate rows. |
| `drop()` | Remove specified columns. |

### actions and transformations

## Sample Code
### Example 1
```python
data = [1, 2, 3, 4, 5, 6]
rdd = spark.sparkContext.parallelize(data)
even = rdd.filter(lambda x: x % 2 == 0)
squared = even.map(lambda x: x * x)
result = squared.collect()
```

###Example 2
```python
high_value = sales.filter(lambda x: x.amount > 10000)
city_sales = high_value.map(lambda x: (x.city, x.amount))
totals = city_sales.reduceByKey(lambda a, b: a + b)
result = totals.collect()
```

