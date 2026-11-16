# BDA Review questions

## unit -4 
1. What is Apache Spark? 
2. Why was Spark developed when Hadoop MapReduce already existed? 
3. Why can repeated disk I/O make MapReduce inefficient for iterative algorithms? 
4. Does Spark perform all processing entirely in memory? Explain. 
5. Compare Apache Spark and Hadoop MapReduce. 
6. Does Spark replace HDFS? Explain. 
7. How can Spark work together with HDFS and YARN? 
8. What are the major components of the Spark ecosystem? 
9. What is the role of Spark Core? 
10. What types of processing are supported by Spark? 
11. What are the Driver and Executors in a Spark application? 
12. Name the common cluster managers with which Spark can operate. 
13. Why may Spark be unnecessary for a small dataset 
14. Distinguish between local execution and cluster execution in Apache Spark.
15. What are the three major components involved in the execution of a Spark appli
cation?
16. Explain the major responsibilities of the Spark Driver.
17. What is a Spark Executor? How is an Executor different from a worker node?
18. What is the role of a Cluster Manager?
19. Name three cluster managers with which Spark can operate.
20. How can Spark make use of Hadoop YARN?
21. What is SparkSession?
22. Distinguish between SparkSession and SparkContext.
23. Explain the hierarchy: Application → Job → Stage → Task.
24. What is a data partition in Spark?
25. Explain the relationship between partitions, tasks, and parallel processing.
26. Why may the number of tasks be larger than the number of tasks that can execute
simultaneously?
27. What is meant by lazy evaluation in Spark?
28. What causes a Spark job to begin actual execution?
29. Why may a Spark Job contain more than one Stage?
30. What happens conceptually if a Spark task fails during execution?
31. Explain the importance of data locality in Spark.
32.  What is an RDD? Explain the significance of the words Resilient, Distributed, and
Dataset.
33. Explain the major properties of an RDD.
34. What is an RDD partition? Why are partitions important for parallel processing?
35. Explain the relationship among an RDD partition, a Task, and an Executor.
36. What is meant by RDD immutability? Why is immutability useful in distributed
computing?
37. Explain two ways of creating an RDD.
38. What is RDD lineage?
39. Explain how lineage contributes to fault tolerance.
40. Distinguish between HDFS replication and RDD lineage-based recovery.
41. What is a transformation in Spark? Give examples.
42. Explain the map(), filter(), and flatMap() transformations with examples.
43. Distinguish between map() and flatMap().
44. What is an action in Spark? Give examples.
45. Distinguish between transformations and actions.
46. Why should collect() be used carefully on large RDDs?
47. Explain lazy evaluation in Spark.
48. What is the advantage of lazy evaluation?
49. Distinguish between narrow and wide transformations.
50. What is a shuffle? Why can shuffle operations be expensive?
51. Explain how a wide transformation can create a Stage boundary.
52. Using the concepts of transformations and actions, explain how Word Count can
be implemented in Spark.
53. Consider the following sequence:  
RDD1 filter −−−−→ RDD2 map −−→ RDD3 key-based grouping−−−−−−−−−−→ RDD4 count−−−→ Result.  
Identify the transformations and action. Which operation is most likely to introduce a shuffle? Explain.
54. Briefly distinguish between an RDD and a DataFrame.
55. What is a Spark DataFrame? Why is it called a structured distributed-data abstraction?
56. Explain why a DataFrame should not be interpreted as a table residing entirely on one machine.
57. What is a schema? Why is schema information important in structured-data processing?
58. Explain how a DataFrame can be created from a local collection.
59. How can Spark create a DataFrame from a CSV file?
60. Explain the purpose of the `select()`, `filter()`, and `withColumn()` operations.
61. Explain grouping and aggregation in a DataFrame using an example.
62. Why can a `groupBy()` operation require a shuffle?
63. Distinguish between RDDs and DataFrames.
64. Why does the availability of a schema provide greater opportunities for automatic optimization?
65. What is Spark SQL?
66. What is a temporary view? Does creating a temporary view necessarily create a separate physical copy of the DataFrame?
67. Show how city-wise total sales can be calculated using both the DataFrame API and Spark SQL.
68. What is meant by declarative processing in SQL?
69. What is a logical plan?
70. What is the role of the Catalyst Optimizer?
71. Explain the simplified stages through which Catalyst processes a structured query.
72. What are column pruning and predicate pushdown?
73. What is the purpose of the `explain()` operation?
74. What is the role of Tungsten in Spark?
75. Distinguish between Catalyst and Tungsten.
76. Explain how a high-level Spark SQL query eventually results in Tasks being executed by Executors.
77. Consider the following code:
    ```python
    resultDF = salesDF \
        .filter("Amount > 25000") \
        .groupBy("City") \
        .sum("Amount")
    resultDF.show()
    ```
    Identify the transformations and action. Which operation is most likely to require a shuffle? Explain why.
78. Why can repeated use of the same intermediate dataset lead to unnecessary computation in Spark?
79. What is caching in Spark? Explain its purpose.
80. Does calling `cache()` immediately execute all the transformations used to create a dataset? Explain.
81. What is persistence?
82. Distinguish between `cache()` and `persist()`.
83. What is the purpose of `unpersist()`?
84. Under what circumstances is caching particularly useful?
85. Why is it not desirable to cache every intermediate dataset?
86. What is a broadcast variable?
87. Explain a practical situation in which a broadcast variable would be useful.
88. What is an accumulator? Give an example of its use.
89. Distinguish between a broadcast variable and an accumulator.
90. How does the number of partitions affect parallelism in Spark?
91. What problems may occur if a dataset contains too few partitions?
92. What problems may occur if a dataset contains a very large number of tiny partitions?
93. What does `repartition()` do?
94. Distinguish between `repartition()` and `coalesce()`.
95. What is data skew? Explain how it can affect Spark performance.
96. Why are shuffle operations often expensive?
97. Explain four important performance considerations for a Spark application.
98. Distinguish between batch processing and stream processing.
99. What is streaming data? Give three real-world examples.
100. Explain the micro-batch processing model.
101. What is a DStream?
102. Explain the relationship between a DStream and RDDs.
103. What is Structured Streaming?
104. Explain the unbounded-table model of Structured Streaming.
105. What are sources and sinks in a streaming application?
106. Why are window operations important in stream processing?
107. Distinguish between the original Spark Streaming API and Structured Streaming.
108. Consider an online retail system receiving transactions continuously. Explain conceptually how Spark could calculate city-wise sales totals over successive five-minute windows.
109. What is Spark MLlib? Why is it useful in a distributed computing environment?
110. Why may a single-machine machine-learning solution become unsuitable for very large datasets?
111. Distinguish between the historical RDD-based MLlib API and the DataFrame-based Spark ML API.
112. What are features and labels in a supervised machine-learning problem?
113. Why is a feature-vector column commonly required by Spark ML algorithms?
114. What is the purpose of `VectorAssembler`?
115. Define a Transformer in Spark ML.
116. Define an Estimator in Spark ML.
117. Explain the relationship: `Estimator → Model → Transformer`.
118. Distinguish between `fit()` and `transform()`.
119. Explain how a dataset may be divided into training and test subsets.
120. Describe the main steps involved in a Spark ML classification workflow.
121. Name four classification algorithms available through Spark ML.
122. Distinguish between classification and regression.
123. What is clustering? How does it differ from supervised learning?
124. State and explain the objective function of the k-means clustering algorithm.
125. Why can caching be particularly useful for iterative machine-learning algorithms?
126. Why might numerical features require scaling before applying certain machine-learning algorithms?
127. How can categorical features be handled in a machine-learning workflow?
128. What is a Spark ML Pipeline?
129. What kinds of stages can occur within a Pipeline?
130. Explain what happens when `fit()` is applied to a Pipeline containing both Transformers and Estimators.
131. What is a `PipelineModel`?
132. Why are Pipelines useful for maintaining consistency between training and test-data processing?
133. Why is model evaluation necessary?
134. Explain the basic idea of k-fold cross-validation.
135. Why does distributed machine learning not necessarily outperform single-machine machine learning for small datasets?
136. Explain how MLlib ultimately uses Spark partitions, Tasks and Executors.
137. Explain how RDDs, DataFrames, Spark SQL, Structured Streaming and MLlib fit into the overall Spark ecosystem.
138. Distinguish between the roles of HDFS and Apache Spark in a Big Data system.
139. Explain the major differences between Hadoop MapReduce and Apache Spark.
