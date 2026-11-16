# Big Data Analytics — Solved Numericals and Coding Problems

**Compiled from the five uploaded lecture-note PDFs**

## Source files and extraction notes

- `BDA_L1.pdf` — Unit 1: Introduction to Big Data, HDFS and MapReduce.
- `BDA_M2_merged.pdf` — Unit 2: Cloud Computing with AWS.
- `BDA_M4.pdf` — Apache Spark and PySpark.
- `BDA_M5-1-115.pdf` — Apache Hive, Kafka and streaming examples.
- `BDA_M5.pdf` — overlapping, shorter copy of the Hive/Kafka notes. Its solved examples are also present in the 115-page version, so duplicate material is included only once.

The sections below collect worked calculations, traced examples, pseudocode, HiveQL and PySpark code shown in the notes. They are grouped by source/unit rather than rewritten as new questions. Where the notes give an illustrative fragment rather than a complete runnable program, it is labelled accordingly.

---

# 1. Unit 1 — Introduction to Big Data, HDFS and MapReduce

## 1.1 HDFS storage required with replication

**Source:** `BDA_L1.pdf`, PDF page 93.

The notes give the approximate storage formula:

$$
\text{Storage Required} \approx rS
$$

where:
- $r$ = replication factor
- $S$ = original file size

**Given:**
- Original file size $S = 1\text{ TB}$
- Replication factor $r = 3$

**Calculation:**

$$
\text{Storage Required} \approx 3 \times 1 = 3\text{ TB}
$$

**Answer:** Approximately **3 TB** of storage is required.

## 1.2 Small-files problem in HDFS

**Source:** `BDA_L1.pdf`, PDF page 101.

The notes compare:

1. One file of size **1 GB**.
2. One million files of size **1 KB** each.

The total data size is approximately similar, but the second arrangement requires NameNode metadata for **one million file entries**. It therefore places much greater pressure on the NameNode.

**Conclusion:** HDFS is optimized for a smaller number of large files rather than millions of very small files.

## 1.3 Distributed-processing time calculation

**Source:** `BDA_L1.pdf`, PDF page 137.

**Given:**
- Dataset size = $10\text{ TB}$
- Processing rate of one computer = $100\text{ MB/s}$
- Ignore all other overheads.

Using the decimal conversion in the notes:

$$
10\text{ TB} = 10 \times 10^6\text{ MB}
$$

$$
\text{Processing time}
= \frac{10 \times 10^6\text{ MB}}{100\text{ MB/s}}
= 100000\text{ seconds}
$$

Convert seconds to hours:

$$
\frac{100000}{3600} \approx 27.8\text{ hours}
$$

**Answer:** Approximately **27.8 hours** on one computer.

The notes also point out that parallel processing can reduce the time, but speedup is not perfectly proportional to the number of machines because communication, synchronization, scheduling, disk access and failure handling introduce overhead.

## 1.4 Total sales by product — key-value representation and Reduce

**Source:** `BDA_L1.pdf`, PDF pages 139 and 141.

**Input transactions:**

```text
(Laptop, 50000)
(Phone, 20000)
(Laptop, 60000)
```

The product name is used as the key and the sales amount as the value. Values belonging to the same key are grouped.

A separate Reduce example in the notes uses:

```text
(Laptop, [50000, 60000, 45000])
```

The Reducer calculates:

$$
50000 + 60000 + 45000 = 155000
$$

**Output:**

```text
(Laptop, 155000)
```

## 1.5 Shuffle and Sort example

**Source:** `BDA_L1.pdf`, PDF page 141.

**Mapper output:**

```text
(cat, 1)
(dog, 1)
(cat, 1)
(bird, 1)
(dog, 1)
```

After shuffle and sort, values are grouped by key:

```text
(bird, [1])
(cat, [1, 1])
(dog, [1, 1])
```

The Reducer can then process each key independently.

## 1.6 MapReduce Word Count — complete worked example

**Source:** `BDA_L1.pdf`, PDF pages 145–148.

The notes demonstrate Word Count using the input:

```text
big data big
```

### Map stage

Each word is emitted as a key-value pair with value `1`:

```text
(big, 1)
(data, 1)
(big, 1)
```

### Shuffle and Sort

Values with the same key are grouped:

```text
(big, [1, 1])
(data, [1])
```

### Reduce stage

```text
Reduce(big, [1, 1]) = (big, 2)
Reduce(data, [1])   = (data, 1)
```

### Final output

```text
big     2
data    1
```

### Mapper pseudocode

```text
map(key, value):

    words = split(value)

    for each word in words:
        emit(word, 1)
```

### Reducer pseudocode

```text
reduce(word, values):

    total = 0

    for each value in values:
        total = total + value

    emit(word, total)
```

### Additional Reduce illustration

For:

```text
word = data
values = [1, 1, 1, 1]
```

$$
1 + 1 + 1 + 1 = 4
$$

Output:

```text
(data, 4)
```

## 1.7 MapReduce word count — second worked input

**Source:** `BDA_L1.pdf`, PDF pages 145–146.

The notes also show a larger word-count result:

| Word | Count |
|---|---:|
| big | 2 |
| data | 3 |
| distributed | 1 |
| is | 3 |
| useful | 1 |
| valuable | 1 |

The Reduce operations shown are:

```text
Reduce(big, [1, 1])           = (big, 2)
Reduce(data, [1, 1, 1])       = (data, 3)
Reduce(distributed, [1])      = (distributed, 1)
Reduce(is, [1, 1, 1])         = (is, 3)
Reduce(useful, [1])           = (useful, 1)
Reduce(valuable, [1])         = (valuable, 1)
```

## 1.8 MapReduce sales aggregation — worked example

**Source:** `BDA_L1.pdf`, PDF page 151.

The sales values are grouped by product:

```text
(Laptop, 50000)
(Phone, 20000)
(Laptop, 60000)
(Tablet, 30000)
(Phone, 25000)
(Laptop, 40000)
```

After shuffle and sort:

```text
(Laptop, [50000, 60000, 40000])
(Phone, [20000, 25000])
(Tablet, [30000])
```

Reducer output:

```text
(Laptop, 150000)
(Phone, 45000)
(Tablet, 30000)
```

## 1.9 Combiner — local aggregation example

**Source:** `BDA_L1.pdf`, PDF pages 153–155.

A Mapper may generate:

```text
(data, 1)
```

one thousand times.

A local Combiner can aggregate these into:

```text
(data, 1000)
```

This reduces the number of intermediate records transferred over the network.

### Why local averages cannot simply be averaged

The notes give two machines containing:

```text
Machine 1: {10, 20}
Machine 2: {100}
```

Local means:

$$
\text{Mean}_1 = \frac{10+20}{2}=15
$$

$$
\text{Mean}_2 = 100
$$

Taking the mean of the two local means:

$$
\frac{15+100}{2}=57.5
$$

But the actual global mean is:

$$
\frac{10+20+100}{3}=\frac{130}{3}\approx43.33
$$

**Conclusion:** Simply combining local averages gives an incorrect result. A correct approach must preserve both the local sum and local count.

## 1.10 Partitioner example

**Source:** `BDA_L1.pdf`, PDF page 168.

The notes give the conceptual rule:

$$
\text{Reducer Number} = \operatorname{hash}(key)\bmod R
$$

where $R$ is the number of Reducers.

For two Reducers, one possible assignment is:

```text
Reducer 1: {apple, cat}
Reducer 2: {banana, dog}
```

All occurrences of the same key must be assigned to the same Reducer.

---

# 2. Unit 2 — Cloud Computing with AWS

## 2.1 CPU utilization calculation

**Source:** `BDA_M2_merged.pdf`, PDF page 11.

The notes define:

$$
\text{Utilization} =
\frac{\text{Resources Used}}{\text{Resources Available}}\times100
$$

**Given:**
- Available CPU cores = 20
- Average CPU cores used = 4

**Calculation:**

$$
\text{CPU Utilization}=\frac{4}{20}\times100=20\%
$$

**Answer:** CPU utilization is **20%**. Approximately **80%** of CPU capacity remains unused during average operation.

## 2.2 Physical server and virtual-machine resource allocation

**Source:** `BDA_M2_merged.pdf`, PDF page 27.

**Physical server:**

```text
16 CPU cores
64 GB RAM
```

It hosts four virtual machines:

| Virtual machine | vCPUs | RAM |
|---|---:|---:|
| VM1 | 4 | 16 GB |
| VM2 | 4 | 16 GB |
| VM3 | 4 | 16 GB |
| VM4 | 4 | 16 GB |
| **Total allocated** | **16** | **64 GB** |

The four VMs together are allocated the full stated CPU and RAM capacity of the physical server.

## 2.3 Server consolidation example

**Source:** `BDA_M2_merged.pdf`, PDF page 35.

**Initial arrangement:**
- 10 physical servers.
- Each operates at approximately 10%–20% average utilization.

The organization may consolidate the workloads onto **2 or 3 more powerful physical servers** using virtual machines.

The notes identify possible reductions in:
- hardware cost;
- electricity consumption;
- cooling requirements;
- rack-space usage;
- maintenance burden.

This is an illustrative consolidation example; the notes do not provide a numerical utilization calculation for the consolidated servers.

## 2.4 Cloud availability calculation

**Source:** `BDA_M2_merged.pdf`, PDF page 65.

The notes define:

$$
\text{Availability} =
\frac{\text{Operational Time}}{\text{Total Time}}\times100
$$

**Given:**
- Operational time = 719 hours
- Total observation period = 720 hours

**Calculation:**

$$
\text{Availability}=\frac{719}{720}\times100
\approx99.86\%
$$

**Answer:** Approximately **99.86% availability**.

## 2.5 Horizontal scaling example

**Source:** `BDA_M2_merged.pdf`, PDF pages 154 and 162.

The notes show the scaling sequence:

```text
1 → 2 → 4 → 8 servers
```

A separate EC2 example gives:

- Normal demand: 2 EC2 instances.
- Peak demand: 2 → 4 → 8 instances.
- When demand decreases: 8 → 4 → 2 instances.

The load balancer distributes requests among available healthy instances, while Auto Scaling changes the number of instances.

## 2.6 VPC and subnet address-range example

**Source:** `BDA_M2_merged.pdf`, PDF page 131.

The notes use:

```text
VPC = 10.0.0.0/16
```

Possible addresses:

```text
10.0.1.10
10.0.2.25
10.0.3.100
```

Possible subnets:

```text
Subnet 1 = 10.0.1.0/24
Subnet 2 = 10.0.2.0/24
```

The example illustrates dividing a VPC address range into smaller network segments. The notes do not work through subnet host-count calculations.

---

# 3. Unit 4 — Apache Spark and PySpark

## 3.1 Spark task-result aggregation

**Source:** `BDA_M4.pdf`, PDF page 24.

A distributed sales dataset is divided into four partitions:

```text
P1, P2, P3, P4
```

The tasks produce:

```text
T1 → 2500
T2 → 3000
T3 → 2200
T4 → 2300
```

Combine the partial results:

$$
2500+3000+2200+2300=10000
$$

**Answer:** **10,000 sales records**.

## 3.2 RDD map transformation

**Source:** `BDA_M4.pdf`, PDF page 38.

Given:

```text
RDD1 = {1, 2, 3, 4}
f(x) = x²
```

Result:

```text
RDD2 = {1, 4, 9, 16}
```

PySpark:

```python
rdd2 = rdd1.map(lambda x: x * x)
```

## 3.3 RDD filter transformation

**Source:** `BDA_M4.pdf`, PDF page 38.

Given:

```text
RDD1 = {1, 2, 3, 4, 5, 6}
```

Retain only even numbers:

```text
RDD2 = {2, 4, 6}
```

PySpark:

```python
even_rdd = rdd.filter(lambda x: x % 2 == 0)
```

## 3.4 flatMap example

**Source:** `BDA_M4.pdf`, PDF page 39.

Input:

```text
{"big data", "spark analytics"}
```

After splitting and flattening:

```text
{big, data, spark, analytics}
```

PySpark:

```python
words = lines.flatMap(lambda line: line.split())
```

The notes also compare:

```text
map(split)     → {[big, data], [apache, spark]}
flatMap(split) → {big, data, apache, spark}
```

## 3.5 Lazy evaluation — code example

**Source:** `BDA_M4.pdf`, PDF page 42.

```python
rdd2 = rdd1.filter(lambda x: x > 10)
rdd3 = rdd2.map(lambda x: x * 2)

result = rdd3.count()
```

The first two operations are transformations. `count()` is an action and triggers the required computation.

## 3.6 Spark Word Count — complete worked example

**Source:** `BDA_M4.pdf`, PDF pages 48–49.

Input lines:

```text
"big data big"
"spark big data"
```

After `flatMap()`:

```text
{big, data, big, spark, big, data}
```

After `map()`:

```text
{(big, 1), (data, 1), (big, 1),
 (spark, 1), (big, 1), (data, 1)}
```

After aggregation by key:

```text
(big, 3)
(data, 2)
(spark, 1)
```

Final output:

```text
big   → 3
data  → 2
spark → 1
```

The notes summarize the Spark sequence as:

```text
flatMap → map → reduceByKey → Action
```

## 3.7 Compact RDD coding example

**Source:** `BDA_M4.pdf`, PDF page 49.

```python
data = [1, 2, 3, 4, 5, 6]

rdd = spark.sparkContext.parallelize(data)

even = rdd.filter(lambda x: x % 2 == 0)

squared = even.map(lambda x: x * x)

result = squared.collect()
```

Logical operations:
1. Create an RDD from the collection.
2. Filter even numbers.
3. Square the retained numbers.
4. Collect the result.

The resulting values are:

```text
[4, 16, 36]
```

## 3.8 City-wise sales analysis using RDDs

**Source:** `BDA_M4.pdf`, PDF pages 53–56.

**Requirement:** For transactions with an amount greater than 10,000, calculate the total sales amount for each city.

The notes first show a sample of the original dataset:

| City | Amount |
|---|---:|
| Chennai | 15000 |
| Delhi | 5000 |
| Mumbai | 20000 |
| Chennai | 4000 |
| Delhi | 25000 |

After `filter(amount > 10000)` only these rows remain (Delhi 5000 and Chennai 4000 are dropped):

| City | Amount |
|---|---:|
| Chennai | 15000 |
| Mumbai | 20000 |
| Delhi | 25000 |

### Step 1 — Filter

```python
high_value = sales.filter(lambda x: x.amount > 10000)
```

### Step 2 — Create city-amount pairs

```python
city_sales = high_value.map(lambda x: (x.city, x.amount))
```

### Step 3 — Aggregate by key

```python
totals = city_sales.reduceByKey(lambda a, b: a + b)
```

### Step 4 — Obtain the result

```python
result = totals.collect()
```

For the `reduceByKey()` step, the notes use a separate set of key–value records:

```text
(Chennai, 15000)
(Delhi,   18000)
(Chennai, 30000)
(Mumbai,  12000)
(Delhi,   25000)
```

The worked calculation shown in the notes is:

$$
\text{Chennai}=15000+30000=45000
$$

$$
\text{Delhi}=18000+25000=43000
$$

$$
\text{Mumbai}=12000
$$

**Final totals shown in the notes:**

```text
Chennai : 45000
Delhi   : 43000
Mumbai  : 12000
```

The notes explain that `filter()`, `map()` and `reduceByKey()` are transformations, while `collect()` is an action.

## 3.9 Create a DataFrame from local data

**Source:** `BDA_M4.pdf`, PDF page 66.

```python
columns = ["TransactionID", "City", "Product", "Amount"]

salesDF = spark.createDataFrame(data, columns)
```

The resulting DataFrame is illustrated as:

| TransactionID | City | Product | Amount |
|---:|---|---|---:|
| 101 | Chennai | Laptop | 55000 |
| 102 | Delhi | Mobile | 30000 |
| 103 | Mumbai | Tablet | 22000 |

## 3.10 Create a DataFrame from a CSV file

**Source:** `BDA_M4.pdf`, PDF page 66.

```python
salesDF = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("sales.csv")
```

Display the DataFrame:

```python
salesDF.show()
```

## 3.11 Add a calculated tax column

**Source:** `BDA_M4.pdf`, PDF page 68.

**Given:** Tax rate = 18%.

$$
Tax=0.18\times Amount
$$

PySpark:

```python
from pyspark.sql.functions import col

newDF = salesDF.withColumn(
    "Tax",
    col("Amount") * 0.18
)
```

## 3.12 Group and aggregate DataFrame rows

**Source:** `BDA_M4.pdf`, PDF pages 68–69.

Input:

| City | Amount |
|---|---:|
| Chennai | 15000 |
| Delhi | 18000 |
| Chennai | 30000 |
| Mumbai | 12000 |
| Delhi | 25000 |

Code:

```python
salesDF.groupBy("City").sum("Amount").show()
```

Output:

| City | Total |
|---|---:|
| Chennai | 45000 |
| Delhi | 43000 |
| Mumbai | 12000 |

## 3.13 City-wise sales analysis using the DataFrame API

**Source:** `BDA_M4.pdf`, PDF page 83.

**Requirement:** For transactions with an amount greater than 10,000, calculate the total sales amount for each city.

```python
resultDF = salesDF \
    .filter("Amount > 10000") \
    .groupBy("City") \
    .sum("Amount")

resultDF.show()
```

Logical sequence:

```text
Sales DataFrame
      ↓
Filter Amount > 10000
      ↓
Group by City
      ↓
Sum Amount
      ↓
Display Result
```

## 3.14 Equivalent Spark SQL solution

**Source:** `BDA_M4.pdf`, PDF page 84.

Create a temporary view:

```python
salesDF.createOrReplaceTempView("sales")
```

Execute the query:

```python
resultDF = spark.sql("""
    SELECT City, SUM(Amount) AS TotalSales
    FROM sales
    WHERE Amount > 10000
    GROUP BY City
""")

resultDF.show()
```

## 3.15 Accumulator example

**Source:** `BDA_M4.pdf`, PDF page 95.

Three tasks encounter malformed records:

```text
T1 → 5 malformed records
T2 → 3 malformed records
T3 → 7 malformed records
```

Combined value:

$$
5+3+7=15
$$

PySpark accumulator initialization:

```python
badRecords = spark.sparkContext.accumulator(0)
```

The notes describe tasks incrementing the accumulator when malformed records are encountered.

## 3.16 Feature vector construction using VectorAssembler

**Source:** `BDA_M4.pdf`, PDF page 117.

Input columns:

```text
Age, Income, Visits, TimeSpent
```

Code:

```python
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=["Age", "Income", "Visits", "TimeSpent"],
    outputCol="features"
)

preparedDF = assembler.transform(customerDF)
```

Conceptual feature vector:

```text
features = [Age, Income, Visits, TimeSpent]
```

## 3.17 Training and test data split

**Source:** `BDA_M4.pdf`, PDF page 120.

The notes use an 80:20 split:

```python
trainDF, testDF = preparedDF.randomSplit(
    [0.8, 0.2],
    seed=42
)
```

Conceptually:

$$
D=D_{train}\cup D_{test}
$$

## 3.18 Logistic Regression using Spark ML

**Source:** `BDA_M4.pdf`, PDF page 121.

```python
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label"
)

model = lr.fit(trainDF)

predictions = model.transform(testDF)
```

The notes identify:
- `lr` as the Estimator.
- `model` as the fitted Model.
- `predictions` as the transformed test DataFrame.

## 3.19 k-Means clustering using Spark ML

**Source:** `BDA_M4.pdf`, PDF pages 124–125.

The notes describe the k-means objective as minimizing the within-cluster sum of squared distances:

$$
J=\sum_{j=1}^{k}\sum_{x_i\in C_j}\|x_i-\mu_j\|^2
$$

Code:

```python
from pyspark.ml.clustering import KMeans

kmeans = KMeans(
    featuresCol="features",
    k=3,
    seed=42
)

model = kmeans.fit(dataDF)

predictions = model.transform(dataDF)
```

## 3.20 Spark ML Pipeline example

**Source:** `BDA_M4.pdf`, PDF pages 127–128.

The notes show a pipeline with feature assembly followed by Logistic Regression.

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

assembler = VectorAssembler(
    inputCols=["Age", "Income", "Visits"],
    outputCol="features"
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label"
)

pipeline = Pipeline(
    stages=[assembler, lr]
)

pipelineModel = pipeline.fit(trainDF)

predictions = pipelineModel.transform(testDF)
```

---

# 4. Unit 5 — Apache Hive, Kafka and Streaming

## 4.1 HiveQL — create a database and table

**Source:** `BDA_M5-1-115.pdf`, PDF pages 47–49.

### Create a Hive table

The sales dataset contains:

```text
(TransactionID, City, Product, Quantity, Amount)
```

```sql
CREATE TABLE sales (
    TransactionID INT,
    City STRING,
    Product STRING,
    Quantity INT,
    Amount DOUBLE
);
```

### Create a table using ORC

```sql
CREATE TABLE sales (
    TransactionID INT,
    City STRING,
    Product STRING,
    Quantity INT,
    Amount DOUBLE
)
STORED AS ORC;
```

### Create a comma-delimited text table

Input data:

```text
101,Chennai,Laptop,1,55000
102,Delhi,Mobile,2,30000
103,Chennai,Mobile,1,28000
104,Delhi,Laptop,1,62000
```

HiveQL:

```sql
CREATE TABLE sales_text (
    TransactionID INT,
    City STRING,
    Product STRING,
    Quantity INT,
    Amount DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

## 4.2 HiveQL — load data

**Source:** `BDA_M5-1-115.pdf`, PDF page 51.

### Load from a local file

```sql
LOAD DATA LOCAL INPATH
'/home/user/sales.csv'
INTO TABLE sales_text;
```

### Load from HDFS

```sql
LOAD DATA INPATH
'/data/sales.csv'
INTO TABLE sales_text;
```

### Replace existing table data

```sql
LOAD DATA LOCAL INPATH
'/home/user/sales.csv'
OVERWRITE INTO TABLE sales_text;
```

## 4.3 HiveQL — insert and retrieve data

**Source:** `BDA_M5-1-115.pdf`, PDF page 52.

### Insert a row

```sql
INSERT INTO TABLE sales_text
VALUES
(105, 'Mumbai', 'Tablet', 2, 40000);
```

### Insert query results by replacing target data

```sql
INSERT OVERWRITE TABLE sales_text
SELECT *
FROM another_sales_table;
```

### Select all columns

```sql
SELECT *
FROM sales_text;
```

### Select specific columns

```sql
SELECT City, Product, Amount
FROM sales_text;
```

## 4.4 HiveQL — filtering rows

**Source:** `BDA_M5-1-115.pdf`, PDF page 53.

Input:

| City | Product | Amount |
|---|---|---:|
| Chennai | Laptop | 55000 |
| Delhi | Mobile | 30000 |
| Chennai | Mobile | 28000 |
| Delhi | Laptop | 62000 |

Query:

```sql
SELECT City, Product, Amount
FROM sales_text
WHERE Amount > 50000;
```

Output:

| City | Product | Amount |
|---|---|---:|
| Chennai | Laptop | 55000 |
| Delhi | Laptop | 62000 |

### Multiple conditions

```sql
SELECT *
FROM sales_text
WHERE City = 'Chennai'
  AND Amount > 30000;
```

## 4.5 HiveQL — aggregate functions and numerical calculation

**Source:** `BDA_M5-1-115.pdf`, PDF page 54.

Using the four Amount values:

```text
55000
30000
28000
62000
```

### Total using SUM

```sql
SELECT SUM(Amount)
FROM sales_text;
```

Calculation:

$$
55000+30000+28000+62000=175000
$$

**Output:** `175000`

### Average using AVG

```sql
SELECT AVG(Amount)
FROM sales_text;
```

Calculation:

$$
\text{Average}=\frac{175000}{4}=43750
$$

**Output:** `43750`

The notes also distinguish:
- `COUNT(*)` — counts all rows.
- `COUNT(Column)` — counts rows where the specified column is not `NULL`.

## 4.6 HiveQL — GROUP BY worked example

**Source:** `BDA_M5-1-115.pdf`, PDF page 55.

Input:

| City | Amount |
|---|---:|
| Chennai | 55000 |
| Delhi | 30000 |
| Chennai | 28000 |
| Delhi | 62000 |

Query:

```sql
SELECT City, SUM(Amount) AS TotalSales
FROM sales_text
GROUP BY City;
```

Calculations:

$$
\text{Chennai}=55000+28000=83000
$$

$$
\text{Delhi}=30000+62000=92000
$$

Output:

| City | TotalSales |
|---|---:|
| Chennai | 83000 |
| Delhi | 92000 |

## 4.7 HiveQL — HAVING worked example

**Source:** `BDA_M5-1-115.pdf`, PDF page 56.

Query:

```sql
SELECT City, SUM(Amount) AS TotalSales
FROM sales_text
GROUP BY City
HAVING SUM(Amount) > 85000;
```

City totals from the previous example:

```text
Chennai = 83000
Delhi   = 92000
```

Only Delhi satisfies `TotalSales > 85000`.

Output:

| City | TotalSales |
|---|---:|
| Delhi | 92000 |

### Combined WHERE and HAVING

```sql
SELECT City, SUM(Amount) AS TotalSales
FROM sales_text
WHERE Amount > 25000
GROUP BY City
HAVING SUM(Amount) > 85000;
```

## 4.8 HiveQL — sorting and LIMIT

**Source:** `BDA_M5-1-115.pdf`, PDF pages 57–58.

### Global ordering

```sql
SELECT City, Amount
FROM sales_text
ORDER BY Amount DESC;
```

Example ordered Amount values:

```text
62000, 55000, 30000, 28000
```

### Partition-level sorting

```sql
SELECT City, Amount
FROM sales_text
SORT BY Amount DESC;
```

`SORT BY` does not necessarily guarantee a globally sorted result.

### Return at most five rows

```sql
SELECT *
FROM sales_text
LIMIT 5;
```

### Return the five highest-value transactions

```sql
SELECT *
FROM sales_text
ORDER BY Amount DESC
LIMIT 5;
```

## 4.9 HiveQL — INNER JOIN worked example

**Source:** `BDA_M5-1-115.pdf`, PDF page 59.

Products table:

| ProductID | Category |
|---|---|
| P1 | Electronics |
| P2 | Mobile |
| P3 | Furniture |

Query:

```sql
SELECT s.TransactionID,
       p.Category,
       s.Amount
FROM sales s
INNER JOIN products p
ON s.ProductID = p.ProductID;
```

Output shown in the notes:

| TransactionID | Category | Amount |
|---:|---|---:|
| 101 | Electronics | 55000 |
| 102 | Mobile | 30000 |
| 103 | Electronics | 62000 |

## 4.10 HiveQL — ALTER and DROP

**Source:** `BDA_M5-1-115.pdf`, PDF page 60.

### Add a column

```sql
ALTER TABLE sales_text
ADD COLUMNS (
    PaymentMode STRING
);
```

### Rename a table

```sql
ALTER TABLE sales_text
RENAME TO sales_transactions;
```

### Drop a table

```sql
DROP TABLE sales_transactions;
```

## 4.11 HiveQL — retail sales analysis worked query set

**Source:** `BDA_M5-1-115.pdf`, PDF pages 61–62.

The notes use a sales table with schema:

```text
(TransactionID, City, Product, Quantity, Amount)
```

### Question 1 — Total revenue

```sql
SELECT SUM(Amount) AS TotalRevenue
FROM sales;
```

Mathematical expression:

$$
R=\sum_{i=1}^{n} Amount_i
$$

### Question 2 — Total revenue by city

```sql
SELECT City,
       SUM(Amount) AS TotalRevenue
FROM sales
GROUP BY City;
```

For city $c$:

$$
R_c=\sum_{i:City_i=c} Amount_i
$$

### Question 3 — Cities with revenue above a threshold

```sql
SELECT City,
       SUM(Amount) AS TotalRevenue
FROM sales
GROUP BY City
HAVING SUM(Amount) > 100000;
```

### Question 4 — Five highest-revenue cities

```sql
SELECT City,
       SUM(Amount) AS TotalRevenue
FROM sales
GROUP BY City
ORDER BY TotalRevenue DESC
LIMIT 5;
```

### Question 5 — Revenue from high-value transactions

```sql
SELECT City,
       SUM(Amount) AS TotalRevenue
FROM sales
WHERE Amount > 50000
GROUP BY City;
```

## 4.12 Hive partition pruning — numerical example

**Source:** `BDA_M5-1-115.pdf`, PDF page 72.

**Given:**
- Total dataset $D=5\text{ TB}$
- 10 yearly partitions
- Data is uniformly distributed.

Data per partition:

$$
D_p=\frac{5}{10}=0.5\text{ TB}
$$

Without partition pruning:

$$
D_{scan}=5\text{ TB}
$$

With effective partition pruning, only one yearly partition is scanned:

$$
D_{scan}=0.5\text{ TB}
$$

Percentage reduction:

$$
\text{Reduction}=\frac{5-0.5}{5}\times100=90\%
$$

**Answer:** The amount of data scanned is reduced by approximately **90%** under the stated assumptions. This does not guarantee a 90% reduction in total execution time.

## 4.13 Hive bucketing — worked example

**Source:** `BDA_M5-1-115.pdf`, PDF page 75.

**Given:**
- Number of buckets $B=4$
- For illustration, hash function returns the integer key.

Formula:

$$
b=CustomerID\bmod4
$$

Calculations:

| CustomerID | Calculation | Bucket |
|---:|---|---:|
| 101 | $101\bmod4=1$ | 1 |
| 102 | $102\bmod4=2$ | 2 |
| 103 | $103\bmod4=3$ | 3 |
| 104 | $104\bmod4=0$ | 0 |

## 4.14 Hive optimized sales table — design example

**Source:** `BDA_M5-1-115.pdf`, PDF pages 83–85.

**Given:**
- Sales data size $D=12\text{ TB}$
- Time span = 6 years
- Common queries: yearly totals, monthly reports, customer-wise analysis and joins with customer information.

The notes suggest:
- Partition by `(Year, Month)` because queries frequently filter by year and month.
- Consider bucketing by `CustomerID` for customer-wise operations and joins.
- Choose a suitable storage format for the workload.

There are:

$$
6\times12=72
$$

monthly partitions.

Average data per monthly partition:

$$
D_p=\frac{12}{72}\text{ TB}\approx0.167\text{ TB}
$$

Approximately:

$$
0.167\text{ TB}\approx167\text{ GB}
$$

Potential scan reduction when one monthly partition is selected:

$$
\left(1-\frac{1}{72}\right)\times100\approx98.61\%
$$

**Answer:** Under the uniform-distribution assumption, a monthly query may scan approximately **0.167 TB instead of 12 TB**, a potential scan reduction of **98.61%**. This is not a guaranteed execution-time improvement.

## 4.15 Kafka consumer assignment — worked numerical example

**Source:** `BDA_M5-1-115.pdf`, PDF page 102.

**Given:** 12 topic partitions.

### Case 1 — 4 consumers

$$
\frac{12}{4}=3
$$

Each consumer receives **3 partitions** if evenly assigned.

### Case 2 — 6 consumers

$$
\frac{12}{6}=2
$$

Each consumer receives **2 partitions** if evenly assigned.

### Case 3 — 15 consumers

There are only 12 partitions, so at most **12 consumers** can be assigned partitions. At least **3 consumers** remain without partition assignments.

## 4.16 Kafka + Spark Structured Streaming — PySpark code

**Source:** `BDA_M5-1-115.pdf`, PDF pages 107–108.

The notes show the basic structure for reading Kafka records as a streaming DataFrame and writing them to the console.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaStreaming")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "temperature_events")
    .load()
)

events = df.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value"
)

query = (
    events.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()
```

**Assumptions stated in the notes:**
- Kafka is running.
- The topic exists.
- The appropriate Spark Kafka connector is available.
- The specified bootstrap server is reachable.

This is an ingestion example, not a complete analytical application.

## 4.17 Event time and processing time example

**Source:** `BDA_M5-1-115.pdf`, PDF page 110.

The notes give:

```text
Event time      = 10:00:05
Processing time = 10:00:12
```

Delay:

$$
10{:}00{:}12-10{:}00{:}05=7\text{ seconds}
$$

**Answer:** The event was processed **7 seconds** after it occurred.

## 4.18 Watermark example

**Source:** `BDA_M5-1-115.pdf`, PDF page 111.

The simplified watermark model is:

$$
W=t_{max}-L
$$

Given:
- Maximum event time $t_{max}=10{:}15$
- Delay threshold $L=2$ minutes

$$
W=10{:}15-2\text{ minutes}=10{:}13
$$

**Answer:** Watermark = **10:13**.

## 4.19 IoT temperature monitoring — numerical example

**Source:** `BDA_M5-1-115.pdf`, PDF pages 111–112.

**Given:**
- 100 temperature sensors.
- Each sensor generates one reading per second.

### Events per second

$$
R=100\times1=100\text{ events/second}
$$

### Events per minute

$$
N_{1m}=100\times60=6000
$$

### Events per hour

$$
N_{1h}=100\times3600=360000
$$

### Raw event volume per hour

Each serialized event occupies approximately 200 bytes:

$$
V=360000\times200=72000000\text{ bytes}
$$

$$
V\approx72\text{ MB}
$$

**Answer:** 360,000 events and approximately **72 MB per hour**, using decimal units. The estimate excludes replication, protocol overhead, indexes, compression effects and other storage costs.

The notes give a possible alert condition:

```text
Ti > 80°C
```

## 4.20 Network intrusion monitoring — worked expression

**Source:** `BDA_M5-1-115.pdf`, PDF page 113.

An event may contain:

$$
e_i=(t_i,IP_{src},IP_{dst},Port,Bytes)
$$

The number of connections from a source IP during a time window is expressed as:

$$
N(IP,W)=\sum_i 1[IP_{src,i}=IP\land t_i\in W]
$$

If:

$$
N(IP,W)>\tau
$$

the system may flag the source for further investigation, where $\tau$ is a selected threshold.

The notes identify this as a simplified streaming-monitoring example, not a complete intrusion-detection algorithm.

---

# 5. Quick index — calculations to practise

| No. | Topic | Main calculation/result |
|---:|---|---|
| 1 | HDFS replication | $1\text{ TB}\times3=3\text{ TB}$ |
| 2 | Distributed processing time | $10\text{ TB}/100\text{ MB/s}\approx27.8\text{ hours}$ |
| 3 | MapReduce Reduce | $50000+60000+45000=155000$ |
| 4 | MapReduce Word Count | `big data big` → `big: 2, data: 1` |
| 5 | Combiner and mean | Local-mean average $57.5$ differs from global mean $43.33$ |
| 6 | CPU utilization | $4/20\times100=20\%$ |
| 7 | Cloud availability | $719/720\times100\approx99.86\%$ |
| 8 | Hive aggregate | Sum $=175000$, average $=43750$ |
| 9 | Hive GROUP BY | Chennai $=83000$, Delhi $=92000$ |
| 10 | Hive partition pruning | $90\%$ scan reduction |
| 11 | Hive bucketing | CustomerID modulo 4 |
| 12 | Hive monthly partitioning | 72 partitions; approximately $98.61\%$ potential scan reduction |
| 13 | Kafka consumer assignment | 12 partitions / 4 consumers = 3 each |
| 14 | IoT event volume | 360,000 events/hour; approximately 72 MB/hour |
| 15 | Spark task aggregation | $2500+3000+2200+2300=10000$ |
| 16 | Spark RDD transformations | Filter even numbers, then square |
| 17 | Spark Word Count | `big: 3, data: 2, spark: 1` |
| 18 | Spark city-wise aggregation | Chennai 45000, Delhi 43000, Mumbai 12000 |
| 19 | Spark accumulator | $5+3+7=15$ |
| 20 | Event-time delay | 7 seconds |
| 21 | Watermark | 10:13 |

---

## Scope note

This is a source-based extraction of the solved numerical examples, worked query examples, pseudocode and code examples found in the uploaded notes. The lecture PDFs also contain review questions and discussion prompts that are not presented with worked solutions; those have not been converted into invented answers here.
