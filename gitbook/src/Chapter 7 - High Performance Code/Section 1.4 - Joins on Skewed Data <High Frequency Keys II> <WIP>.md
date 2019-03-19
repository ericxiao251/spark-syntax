
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

### Option 2: Split the DataFrame in 2 Sections, High Frequency and Non-High Frequency values
