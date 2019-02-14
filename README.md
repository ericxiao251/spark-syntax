# Spark-Syntax

This is a public repo documenting all of the "best practices" of writing PySpark code from what I have learnt from working with `PySpark` for 3 years. This will mainly focus on the `Spark DataFrames and SQL` library.

# Table of Contexts:

## Chapter 1 - Getting Started with Spark:
* #### 1.1 - [Useful Material](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%201%20-%20Basics/Section%201%20-%20Useful%20Material.md)
* #### 1.2 - [Creating your First DataFrame](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%201%20-%20Basics/Section%202%20-%20Creating%20your%20First%20Data%20Object.ipynb)
* #### 1.3 - [Reading your First Dataset](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%201%20-%20Basics/Section%203%20-%20Reading%20your%20First%20Dataset.ipynb)
* #### 1.4 - [More Comfortable with SQL?](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%201%20-%20Basics/Section%204%20-%20More%20Comfortable%20with%20SQL%3F.ipynb)

## Chapter 2 - Exploring the Spark APIs:
* #### 2.1 - [Performing your First Transformations](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%201%20-%20Basics/Section%203%20-%20Performing%20your%20First%20Transformations.ipynb)
    * ##### 2.1.1 - Looking at your data (`collect`/`head`/`toPandas`)
    * ##### 2.1.2 - Selecting a Subset of Columns (`drop`/`select`)
    * ##### 2.1.3 - Filtering Data (`where`/`filter`/`isin`)
    * ##### 2.1.4 - Case Statements (`when`/`otherwise`)
    * ##### 2.1.4 - [Constant Values](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%201.4%20-%20Constant%20Values.ipynb) (`lit`)
    * ##### 2.1.5 - Filling in Null Values (`fill`/`fillna`/`colasce`)
    * ##### 2.1.6 - Spark Functions aren't Enough, I Need my Own Functions! (`udf`)
    * ##### 2.1.7 - Unioning Multiple Dataframes (`union`)
    * ##### 2.1.7 - One to Many Rows (`explode`)
    * ##### 2.1.8 - [Performing Joins (clean one)](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%201.8%20-%20Performing%20Joins%20(clean%20one).ipynb) (`join`)
    * ##### 2.1.8 - [Range Join Conditions (WIP)](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%201.9%20-%20Range%20Join%20Conditions%20(WIP).ipynb) (`join`)
* #### 2.2 - Decimal Types
    * ##### 2.1 - Working with Decimal Types
    * ##### 2.2 - Why did my Decimals overflow :(
* #### 2.3 - Dates and Datetime (Timestamps) Types
    * ##### 3.1 - Working with Date and Datetime Types
    * ##### 3.2 - Timezone aware data
* #### 2.2 - [Equalities with Null Values](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%202%20-%20Exploring%20the%20Spark%20APIs/Section%202%20-%20Equalities%20with%20Null%20Values.ipynb)

## Chapter 3 - Aggregates:
* #### 4.1 - [Clean Aggregations](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%203%20-%20Aggregates/Section%201%20-%20Clean%20Aggregations.ipynb)
* #### 4.2 - [Non Deterministic Behaviours](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%203%20-%20Aggregates/Section%202%20-%20Non%20Deterministic%20Ordering%20for%20GroupBys.ipynb)

## Chapter 4 - Window Objects:
* #### 5.1 - [Default Ordering on a Window Object](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%205%20-%20Window%20Objects/Section%201%20-%20Default%20Behaviour%20of%20a%20Window%20Object.ipynb)
* #### 5.2 - [Ordering High Frequency Data with a Window Object](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%205%20-%20Window%20Objects/Section%202%20-%20Ordering%20High%20Frequency%20Data%20with%20a%20Window%20Object.ipynb)

## Chapter 5 - Error Logs:

## Chapter 6 - Tuning & Spark Parameters:

## Chapter 7 - High Performance Code:
* #### 7.1 - Improving Joins
    * ##### 7.1.1 - [Filter Pushdown](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%207%20-%20High%20Performance%20Code/Section%201.1%20-%20Filter%20Pushdown.ipynb)
    * ##### 7.1.2 - [Joining on Skewed Data (Null Keys)](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%207%20-%20High%20Performance%20Code/Section%201.2%20-%20Joins%20on%20Skewed%20Data%20(Null%20Keys).ipynb)
    * ##### 7.1.3 - [Joining on Skewed Data (High Frequency Keys I)](https://github.com/ericxiao251/spark-syntax/blob/master/src/Chapter%207%20-%20High%20Performance%20Code/Section%201.3%20-%20Joins%20on%20Skewed%20Data%20(High%20Frequency%20Keys%20I).ipynb)
    * ##### 7.1.4 - Joining on Skewed Data (High Frequency Keys II) (WIP)
