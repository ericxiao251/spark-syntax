# Spark-Syntax

This is a public repo documenting all of the "best practices" of writing PySpark code from what I have learnt from working with `PySpark` for 3 years. This will mainly focus on the `Spark DataFrames and SQL` library.

you can also visit ericxiao251.github.io/spark-syntax/ for a online book version.

# Contributing/Topic Requests

If you notice an improvements in terms of typos, spellings, grammar, etc. feel free to create a PR and I'll review it 😁, you'll most likely be right.

If you have any topics that I could potentially go over, please create an **issue** and describe the topic. I'll try my best to address it 😁.

# Acknowledgement

Huge thanks to Levon for turning everything into a gitbook. You can follow his github at https://github.com/tumregels.

# Table of Contexts:

## Chapter 1 - Getting Started with Spark:
* #### 1.1 - [Useful Material](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%201%20-%20Basics/Section%201%20-%20Useful%20Material.md)
* #### 1.2 - [Creating your First DataFrame](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%201%20-%20Basics/Section%202%20-%20Creating%20your%20First%20Data%20Object.ipynb)
* #### 1.3 - [Reading your First Dataset](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%201%20-%20Basics/Section%203%20-%20Reading%20your%20First%20Dataset.ipynb)
* #### 1.4 - [More Comfortable with SQL?](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%201%20-%20Basics/Section%204%20-%20More%20Comfortable%20with%20SQL%3F.ipynb)

## Chapter 2 - Exploring the Spark APIs:
* #### 2.1 - Non-Trivial Data Structures in Spark
    * ##### 2.1.1 - [Struct Types](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%201.1%20-%20Struct%20Types.ipynb) (`StructType`)
    * ##### 2.1.2 - [Arrays and Lists](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%201.2%20-%20Arrays%20and%20Lists.ipynb) (`ArrayType`)
    * ##### 2.1.3 - [Maps and Dictionaries](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%201.3%20-%20Maps%20and%20Dictionaries.ipynb) (`MapType`)
    * ##### 2.1.4 - [Decimals and Why did my Decimals overflow :(](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%201.4%20-%20Decimals%20and%20Why%20did%20my%20Decimals%20Overflow.ipynb) (`DecimalType`)
* #### 2.2 - [Performing your First Transformations](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202%20-%20Performing%20your%20First%20Transformations.ipynb)
    * ##### 2.2.1  - [Looking at Your Data](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.1%20-%20Looking%20at%20Your%20Data.ipynb) (`collect`/`head`/`take`/`first`/`toPandas`/`show`)
    * ##### 2.2.2  - [Selecting a Subset of Columns](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.2%20-%20Selecting%20a%20Subset%20of%20Columns.ipynb) (`drop`/`select`)
    * ##### 2.2.3  - [Creating New Columns and Transforming Data](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.3%20-%20Creating%20New%20Columns%20and%20Transforming%20Data.ipynb) (`withColumn`/`withColumnRenamed`)
    * ##### 2.2.4  - [Constant Values and Column Expressions](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.4%20-%20Constant%20Values%20and%20Column%20Expressions.ipynb) (`lit`/`col`)
    * ##### 2.2.5  - [Casting Columns to a Different Type](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.5%20-%20Casting%20Columns%20to%20Different%20Type.ipynb) (`cast`)
    * ##### 2.2.6  - [Filtering Data](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.6%20-%20Filtering%20Data.ipynb) (`where`/`filter`/`isin`)
    * ##### 2.2.7  - [Equality Statements in Spark and Comparisons with Nulls](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.7%20-%20Equality%20Statements%20in%20Spark%20and%20Comparison%20with%20Nulls.ipynb) (`isNotNull()`/`isNull()`)
    * ##### 2.2.8  - [Case Statements](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.8%20-%20Case%20Statements.ipynb) (`when`/`otherwise`)
    * ##### 2.2.9  - [Filling in Null Values](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.9%20-%20Filling%20in%20Null%20Values.ipynb) (`fillna`/`coalesce`)
    * ##### 2.2.10  - [Spark Functions aren't Enough, I Need my Own!](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.10%20-%20Spark%20Functions%20aren't%20Enough%2C%20I%20Need%20my%20Own!.ipynb) (`udf`/`pandas_udf`)
    * ##### 2.2.11  - [Unionizing Multiple Dataframes](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.11%20%20-%20Unionizing%20Multiple%20Dataframes.ipynb) (`union`)
    * ##### 2.2.12 - [Performing Joins (clean one)](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202.12%20-%20Performing%20Joins%20(clean%20one).ipynb) (`join`)
* #### 2.3 More Complex Transformations
    * ##### 2.3.1 - [One to Many Rows](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%203.1%20-%20One%20to%20Many%20Rows.ipynb) (`explode`)
    * ##### 2.3.2 - [Range Join Conditions (WIP)](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%203.2%20-%20Range%20Join%20Conditions%20(WIP).ipynb) (`join`)
* #### 2.4 Potential Performance Boosting Functions
    * ##### 2.4.1 - (`repartition`)
    * ##### 2.4.2 - (`coalesce`)
    * ##### 2.4.2 - (`cache`)
    * ##### 2.4.2 - (`broadcast`)

## Chapter 3 - Aggregates:
* #### 3.1 - [Clean Aggregations](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%203%20-%20Aggregates/Section%201%20-%20Clean%20Aggregations.ipynb)
* #### 3.2 - [Non Deterministic Behaviours](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%203%20-%20Aggregates/Section%202%20-%20Non%20Deterministic%20Ordering%20for%20GroupBys.ipynb)

## Chapter 4 - Window Objects:
* #### 4.1 - [Default Ordering on a Window Object](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%205%20-%20Window%20Objects/Section%201%20-%20Default%20Behaviour%20of%20a%20Window%20Object.ipynb)
* #### 4.2 - [Ordering High Frequency Data with a Window Object](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%205%20-%20Window%20Objects/Section%202%20-%20Ordering%20High%20Frequency%20Data%20with%20a%20Window%20Object.ipynb)

## Chapter 5 - Error Logs:

## Chapter 6 - Understanding Spark Performance:
* #### 6.1 - Primer to Understanding Your Spark Application
    * #### 6.1.1 - [Understanding how Spark Works](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%206%20-%20Tuning%20%26%20Spark%20Parameters/Section%201.1%20-%20Understanding%20how%20Spark%20Works.md)
    * #### 6.1.2 - Understanding the SparkUI
    * #### 6.1.3 - Understanding how the DAG is Created
    * #### 6.1.4 - Understanding how Memory is Allocated
* #### 6.2 - Analyzing your Spark Application
    * #### 6.1 - Looking for Skew in a Stage
    * #### 6.2 - Looking for Skew in the DAG
    * #### 6.3 - How to Determine the Number of Partitions to Use
* #### 6.3 - How to Analyze the Skew of Your Data

## Chapter 7 - High Performance Code:
* #### 7.0 - The Types of Join Strategies in Spark
  * ##### 7.0.1 - You got a Small Table? (`Broadcast Join`)
  * ##### 7.0.2 - The Ideal Strategy (`BroadcastHashJoin`)
  * ##### 7.0.3 - The Default Strategy (`SortMergeJoin`)
* #### 7.1 - Improving Joins
    * ##### 7.1.1 - [Filter Pushdown](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%207%20-%20High%20Performance%20Code/Section%201.1%20-%20Filter%20Pushdown.ipynb)
    * ##### 7.1.2 - [Joining on Skewed Data (Null Keys)](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%207%20-%20High%20Performance%20Code/Section%201.2%20-%20Joins%20on%20Skewed%20Data%20(Null%20Keys).ipynb)
    * ##### 7.1.3 - [Joining on Skewed Data (High Frequency Keys I)](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%207%20-%20High%20Performance%20Code/Section%201.3%20-%20Joins%20on%20Skewed%20Data%20(High%20Frequency%20Keys%20I).ipynb)
    * ##### 7.1.4 - Joining on Skewed Data (High Frequency Keys II)
    * ##### 7.1.5 - Join Ordering
* #### 7.2 - Repeated Work on a Single Dataset (`caching`)
    * ##### 7.2.1 - caching layers
* #### 7.3 - Spark Parameters
  * ##### 7.3.1 - Running Multiple Spark Applications at Scale (`dynamic allocation`)
  * ##### 7.3.2 - The magical number `2001` (`partitions`)
  * ##### 7.3.3 - Using a lot of `UDF`s? (`python memory`)
* #### 7. - Bloom Filters :o?
