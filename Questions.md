1. What is Apache Hive and why was it developed?
2. Why can HiveQL be more convenient than writing a MapReduce program for common analytical operations?
3. Explain the difference between declarative querying and lower-level procedural distributed processing.
4. Why is Hive described as a data-warehouse system?
5. Distinguish between operational transaction processing and analytical processing.
6. Explain the relationship between Hive and HDFS.
7. Does Hive replace HDFS? Justify your answer.
8. What is metadata? Give examples of metadata associated with a Hive table.
9. What is the Hive Metastore?
10. Explain the concept of schema-on-read.
11. Distinguish between schema-on-read and schema-on-write.
12. Explain the basic Hive data model.
13. What is a managed table in Hive?
14. What is an external table in Hive?
15. Distinguish between managed and external tables.
16. Why is Hive not simply another traditional RDBMS?
17. Why is Hive generally unsuitable as a replacement for a conventional OLTP database?
18. Compare Hive and MapReduce from the perspective of the programmer.
19. Compare the roles of Hive and Spark SQL.
20. Give three practical applications for which Hive would be appropriate.
21. A company stores five years of transaction records in HDFS. It wants monthly, city-wise and product-wise revenue reports. Explain why Hive would be suitable for this requirement.
22. Compare the primary roles of HDFS, MapReduce, Spark and Hive in a Big Data ecosystem.
23. Draw and explain the architecture of Apache Hive.
24. What are the major components of Hive architecture?
25. What is the role of a Hive client?
26. What is Beeline?
27. What is the purpose of HiveServer2?
28. Explain the role of the Hive Driver.
29. What is the role of the Compiler in Hive?
30. Distinguish between syntax analysis and semantic analysis.
31. Why does the Compiler need information from the Hive Metastore?
32. What information is stored in the Hive Metastore?
33. Distinguish between the Hive Metastore and HDFS.
34. What is the role of the Hive Optimizer?
35. Why is query optimization particularly important for very large datasets?
36. Explain the role of the Hive Execution Engine.
37. Explain the historical relationship between Hive and MapReduce.
38. Why is it incorrect to state that every modern Hive query must execute through MapReduce?
39. What is the role of Apache Tez in a modern Hive environment?
40. What is the role of YARN during Hive query execution?
41. Does YARN interpret HiveQL? Explain.
42. Explain the role of HDFS in the Hive architecture.
43. What is the difference between metadata and actual table data?
44. Trace the complete execution of the following query:
SELECT City, SUM(Amount)
FROM sales
GROUP BY City;
45. Explain the interaction among the Compiler, Metastore and Optimizer.
46. Distinguish between the Driver, Compiler, Optimizer and Execution Engine.
47. Explain how a high-level HiveQL query is ultimately converted into distributed computation.
48. Compare the high-level query-processing flow of Hive with that of Spark SQL.
49. What is HiveQL? Why is it described as a declarative query language?
50. Distinguish between DDL, DML, and DQL statements.
51. Write HiveQL commands to create a database named university db and select it.
52. Write a HiveQL statement to create a table containing StudentID, Name, Department, and CGPA.
53. Explain the role of data types in a Hive table schema.
54. What is the purpose of the ROW FORMAT DELIMITED clause?
55. Explain the difference between LOAD DATA LOCAL INPATH and LOAD DATA INPATH.
56. Distinguish between INSERT INTO and INSERT OVERWRITE.
57. Explain the purpose of the SELECT and WHERE clauses.
58. Distinguish between WHERE and HAVING with examples.
59. Write a HiveQL query to calculate total sales for each product category.
60. Explain how GROUP BY performs aggregation.
61. Distinguish between ORDER BY and SORT BY.
62. Why does LIMIT alone not guarantee that the highest values will be returned?
63. Explain the purpose of an INNER JOIN.
64. Distinguish between INNER JOIN and LEFT OUTER JOIN.
65. Write a HiveQL query to find the five cities having the highest total sales.
66. Explain how HiveQL queries are processed using the Hive architecture.
67. A company maintains several terabytes of transaction records. Write suitable HiveQL queries to:
(a) calculate total revenue for each city;
(b) identify cities with revenue exceeding a specified threshold.
68. Explain why HiveQL is more convenient than low-level MapReduce programming for many data-warehouse applications.
69. Why is query optimization important in Hive?
70. What is a full-table scan?
71. Define partitioning in Hive.
72. Explain the physical organization of a partitioned Hive table.
73. Write a HiveQL statement to create a sales table partitioned by year.
74. Distinguish between static and dynamic partitioning.
75. What is partition pruning?
76. Explain how partition pruning can reduce unnecessary data scanning.
77. A table contains 8 TB of data uniformly distributed across 16 partitions. A query accesses only one partition. Calculate the approximate reduction in data scanned.
78. What factors should be considered when selecting partition columns?
79. Why can excessive partitioning reduce performance?
80. Define bucketing in Hive.
81. Explain the role of hashing in bucket assignment.
82. A table uses 8 buckets. Under a simplified modulo-based bucket function, determine the buckets for integer keys 17, 22, 35 and 40.
83. Write HiveQL to create a table bucketed by CustomerID into 8 buckets.
84. Distinguish between partitioning and bucketing.
85. Explain how partitioning and bucketing can be combined.
86. Does bucketing always eliminate shuffle operations? Justify your answer.
87. What is column pruning?
88. Explain predicate pushdown.
89. Why are ORC and Parquet useful for analytical queries?
90. Explain the concept of join optimization.
91. What is cost-based optimization?
92. Why are table statistics useful?
93. Explain the small-file problem.
94. Design an appropriate partitioning strategy for a retail data warehouse containing ten years of transaction records, where most queries filter by year and month.
95. Compare Hive optimization with the Catalyst optimizer in Spark SQL.
96. Distinguish between batch processing and streaming processing.
97. Define streaming analytics. Give three practical applications.
98. What is an event stream?
99. Explain why direct point-to-point connections between producers and consumers can become difficult to manage.
100. Define Apache Kafka.
101. Explain the roles of Kafka producers and consumers.
102. What is a Kafka topic?
103. Explain the purpose of Kafka partitions.
104. Why does Kafka guarantee ordering within partitions rather than across an entire multi-partition topic?
105. Define a Kafka offset.
