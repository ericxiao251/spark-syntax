
A `skewed dataset` is defined by a dataset that has a class imbalance, this leads to poor or failing spark jobs that often get a `OOM` (out of memory) error.

When performing a `join` onto a `skewed dataset` it's usually the case where there is an imbalance on the `key`(s) on which the join is performed on. This results in a majority of the data falls onto a single partition, which will take longer to complete than the other partitions.

Some hints to detect skewness is:
1. The `key`(s) consist mainly of `null` values which fall onto a single partition.
2. There is a subset of values for the `key`(s) that makeup the high percentage of the total keys which fall onto a single partition.

We go through both these cases and see how we can combat it.

### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
```

### Template


```python
spark = (
    SparkSession.builder
    .master("local")
    .appName("Exploring Joins")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

sc = spark.sparkContext
```

### Situation 2: High Frequency Keys

Inital Datasets


```python
customers = spark.createDataFrame([
    (1, "John"), 
    (2, "Bob"),
], ["customer_id", "first_name"])

customers.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>first_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>John</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Bob</td>
    </tr>
  </tbody>
</table>
</div>




```python
orders = spark.createDataFrame([
    (i, 1 if i < 95 else 2, "order #{}".format(i)) for i in range(100) 
], ["id", "customer_id", "order_name"])

orders.toPandas().tail(6)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>customer_id</th>
      <th>order_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>94</th>
      <td>94</td>
      <td>1</td>
      <td>order #94</td>
    </tr>
    <tr>
      <th>95</th>
      <td>95</td>
      <td>2</td>
      <td>order #95</td>
    </tr>
    <tr>
      <th>96</th>
      <td>96</td>
      <td>2</td>
      <td>order #96</td>
    </tr>
    <tr>
      <th>97</th>
      <td>97</td>
      <td>2</td>
      <td>order #97</td>
    </tr>
    <tr>
      <th>98</th>
      <td>98</td>
      <td>2</td>
      <td>order #98</td>
    </tr>
    <tr>
      <th>99</th>
      <td>99</td>
      <td>2</td>
      <td>order #99</td>
    </tr>
  </tbody>
</table>
</div>



### Option 1: Inner Join


```python
df = customers.join(orders, "customer_id")

df.toPandas().tail(10)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>first_name</th>
      <th>id</th>
      <th>order_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>90</th>
      <td>1</td>
      <td>John</td>
      <td>90</td>
      <td>order #90</td>
    </tr>
    <tr>
      <th>91</th>
      <td>1</td>
      <td>John</td>
      <td>91</td>
      <td>order #91</td>
    </tr>
    <tr>
      <th>92</th>
      <td>1</td>
      <td>John</td>
      <td>92</td>
      <td>order #92</td>
    </tr>
    <tr>
      <th>93</th>
      <td>1</td>
      <td>John</td>
      <td>93</td>
      <td>order #93</td>
    </tr>
    <tr>
      <th>94</th>
      <td>1</td>
      <td>John</td>
      <td>94</td>
      <td>order #94</td>
    </tr>
    <tr>
      <th>95</th>
      <td>2</td>
      <td>Bob</td>
      <td>95</td>
      <td>order #95</td>
    </tr>
    <tr>
      <th>96</th>
      <td>2</td>
      <td>Bob</td>
      <td>96</td>
      <td>order #96</td>
    </tr>
    <tr>
      <th>97</th>
      <td>2</td>
      <td>Bob</td>
      <td>97</td>
      <td>order #97</td>
    </tr>
    <tr>
      <th>98</th>
      <td>2</td>
      <td>Bob</td>
      <td>98</td>
      <td>order #98</td>
    </tr>
    <tr>
      <th>99</th>
      <td>2</td>
      <td>Bob</td>
      <td>99</td>
      <td>order #99</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.explain()
```

    == Physical Plan ==
    *(5) Project [customer_id#122L, first_name#123, id#126L, order_name#128]
    +- *(5) SortMergeJoin [customer_id#122L], [customer_id#127L], Inner
       :- *(2) Sort [customer_id#122L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(customer_id#122L, 200)
       :     +- *(1) Filter isnotnull(customer_id#122L)
       :        +- Scan ExistingRDD[customer_id#122L,first_name#123]
       +- *(4) Sort [customer_id#127L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(customer_id#127L, 200)
             +- *(3) Filter isnotnull(customer_id#127L)
                +- Scan ExistingRDD[id#126L,customer_id#127L,order_name#128]


**What Happened**:
* We want to find what `order`s each `customer` made, so we will be `join`ing the `customer`s table to the `order`s table.
* When performing the join, we perform a `hashpartitioning` on `customer_id`.
* From our data creation, this means 95% of the data landed onto a single partition. 

**Results**:
* Similar to the `Null Skew` case, this means that single task/partition will take a lot longer than the others, and most likely erroring out.

### Option 2: Salt the key, then Join

**Helper Function**


```python
def data_skew_helper(left, right, key, number_of_partitions, how="inner"):
    salt_value = F.lit(F.rand() * number_of_partitions % number_of_partitions).cast('int')
    left = left.withColumn("salt", salt_value)
    
    salt_col = F.explode(F.array([F.lit(i) for i in range(number_of_partitions)])).alias("salt")
    right = right.select("*",  salt_col)

    return left.join(right, [key, "salt"], how).drop("salt")
```

**Example**


```python
num_of_partitions = 5
```


```python
left = customers

salt_value = F.lit(F.rand() * num_of_partitions % num_of_partitions).cast('int')    
left = left.withColumn("salt", salt_value)

left.toPandas().head(5)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>first_name</th>
      <th>salt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>John</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Bob</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>




```python
right = orders

salt_col = F.explode(F.array([F.lit(i) for i in range(num_of_partitions)])).alias("salt")
right = right.select("*",  salt_col)

right.toPandas().head(10)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>customer_id</th>
      <th>order_name</th>
      <th>salt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>1</td>
      <td>order #0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0</td>
      <td>1</td>
      <td>order #0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0</td>
      <td>1</td>
      <td>order #0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0</td>
      <td>1</td>
      <td>order #0</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0</td>
      <td>1</td>
      <td>order #0</td>
      <td>4</td>
    </tr>
    <tr>
      <th>5</th>
      <td>1</td>
      <td>1</td>
      <td>order #1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1</td>
      <td>1</td>
      <td>order #1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>1</td>
      <td>1</td>
      <td>order #1</td>
      <td>2</td>
    </tr>
    <tr>
      <th>8</th>
      <td>1</td>
      <td>1</td>
      <td>order #1</td>
      <td>3</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1</td>
      <td>1</td>
      <td>order #1</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>




```python
df = left.join(right, ["customer_id", "salt"])

df.orderBy('id').toPandas().tail(10)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>salt</th>
      <th>first_name</th>
      <th>id</th>
      <th>order_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>90</th>
      <td>1</td>
      <td>4</td>
      <td>John</td>
      <td>90</td>
      <td>order #90</td>
    </tr>
    <tr>
      <th>91</th>
      <td>1</td>
      <td>4</td>
      <td>John</td>
      <td>91</td>
      <td>order #91</td>
    </tr>
    <tr>
      <th>92</th>
      <td>1</td>
      <td>4</td>
      <td>John</td>
      <td>92</td>
      <td>order #92</td>
    </tr>
    <tr>
      <th>93</th>
      <td>1</td>
      <td>4</td>
      <td>John</td>
      <td>93</td>
      <td>order #93</td>
    </tr>
    <tr>
      <th>94</th>
      <td>1</td>
      <td>4</td>
      <td>John</td>
      <td>94</td>
      <td>order #94</td>
    </tr>
    <tr>
      <th>95</th>
      <td>2</td>
      <td>3</td>
      <td>Bob</td>
      <td>95</td>
      <td>order #95</td>
    </tr>
    <tr>
      <th>96</th>
      <td>2</td>
      <td>3</td>
      <td>Bob</td>
      <td>96</td>
      <td>order #96</td>
    </tr>
    <tr>
      <th>97</th>
      <td>2</td>
      <td>3</td>
      <td>Bob</td>
      <td>97</td>
      <td>order #97</td>
    </tr>
    <tr>
      <th>98</th>
      <td>2</td>
      <td>3</td>
      <td>Bob</td>
      <td>98</td>
      <td>order #98</td>
    </tr>
    <tr>
      <th>99</th>
      <td>2</td>
      <td>3</td>
      <td>Bob</td>
      <td>99</td>
      <td>order #99</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.explain()
```

    == Physical Plan ==
    *(5) Project [customer_id#122L, salt#136, first_name#123, id#126L, order_name#128]
    +- *(5) SortMergeJoin [customer_id#122L, salt#136], [customer_id#127L, salt#141], Inner
       :- *(2) Sort [customer_id#122L ASC NULLS FIRST, salt#136 ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(customer_id#122L, salt#136, 200)
       :     +- *(1) Filter (isnotnull(salt#136) && isnotnull(customer_id#122L))
       :        +- *(1) Project [customer_id#122L, first_name#123, cast(((rand(-8040129551223767613) * 5.0) % 5.0) as int) AS salt#136]
       :           +- Scan ExistingRDD[customer_id#122L,first_name#123]
       +- *(4) Sort [customer_id#127L ASC NULLS FIRST, salt#141 ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(customer_id#127L, salt#141, 200)
             +- Generate explode([0,1,2,3,4]), [id#126L, customer_id#127L, order_name#128], false, [salt#141]
                +- *(3) Filter isnotnull(customer_id#127L)
                   +- Scan ExistingRDD[id#126L,customer_id#127L,order_name#128]


**What Happened**:
* We created a new `salt` column for both datasets.
* On one of the dataset we duplicate the data so we have a row for each `salt` value.
* When performing the join, we perform a `hashpartitioning` on `[customer_id, salt]`.

**Results**:
* When we produce a row per `salt` value, we have essentially duplicated `(num_partitions - 1) * N` rows.
* This created more data, but allowed us to spread the data across more partitions as you can see from `hashpartitioning(customer_id, salt)`.

### Summary

All to say:
* By `salt`ing our keys, the `skewed` dataset gets divided into smaller partitions. Thus removing the skew.
* Again we will sacrifice more resources in order to get a performance gain or a successful run.
* We produced more data by creating `(num_partitions - 1) * N` more data for the right side.

