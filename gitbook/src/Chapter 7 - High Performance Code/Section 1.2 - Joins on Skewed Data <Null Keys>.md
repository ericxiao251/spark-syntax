
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

### Situation 1: Null Keys

Inital Datasets


```python
customers = spark.createDataFrame([
    (1, None), 
    (2, None), 
    (3, 1),
], ["id", "card_id"])

customers.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>card_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
cards = spark.createDataFrame([
    (1, "john", "doe", 21), 
    (2, "rick", "roll", 10), 
    (3, "bob", "brown", 2)
], ["card_id", "first_name", "last_name", "age"])

cards.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>card_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>age</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>john</td>
      <td>doe</td>
      <td>21</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>rick</td>
      <td>roll</td>
      <td>10</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>bob</td>
      <td>brown</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



### Option #1: Join Regularly


```python
df = customers.join(cards, "card_id", "left")

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>card_id</th>
      <th>id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>age</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>1</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NaN</td>
      <td>2</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1.0</td>
      <td>3</td>
      <td>john</td>
      <td>doe</td>
      <td>21.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
df = customers.join(cards, "card_id")

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>card_id</th>
      <th>id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>age</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>3</td>
      <td>john</td>
      <td>doe</td>
      <td>21</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened**:
* Rows that didn't join up were brought to the join.

* For a `left join`, they will get `Null` values for the right side columns, what's the point of being them in?
* For a `inner join`, they rows will get dropped, so again what's the point of being them in?

**Results**:
* We brought more rows to the join than we had to. These rows get normally get put onto a single partition. 
* If the data is large enough and the percentage of keys that are null is high. The program could OOM out.

### Option #2: Filter Null Keys First, then Join, then Union


```python
def null_skew_helper(left, right, key):
    """
    Steps:
        1. Filter out the null rows.
        2. Create the columns you would get from the join.
        3. Join the tables.
        4. Union the null rows to joined table.
    """
    df1 = left.where(F.col(key).isNull())
    for f in right.schema.fields:
            df1 = df1.withColumn(f.name, F.lit(None).cast(f.dataType))
    
    df2 = left.where(F.col(key).isNotNull())
    df2 = df2.join(right, key, "left")
    
    return df1.union(df2.select(df1.columns))
    
    
df = null_skew_helper(customers, cards, "card_id")

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>card_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>age</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1.0</td>
      <td>john</td>
      <td>doe</td>
      <td>21.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.explain()
```

    == Physical Plan ==
    Union
    :- *(1) Project [id#0L, null AS card_id#23L, null AS first_name#26, null AS last_name#30, null AS age#35L]
    :  +- *(1) Filter isnull(card_id#1L)
    :     +- Scan ExistingRDD[id#0L,card_id#1L]
    +- *(5) Project [id#0L, card_id#1L, first_name#5, last_name#6, age#7L]
       +- SortMergeJoin [card_id#1L], [card_id#4L], LeftOuter
          :- *(3) Sort [card_id#1L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(card_id#1L, 200)
          :     +- *(2) Filter isnotnull(card_id#1L)
          :        +- Scan ExistingRDD[id#0L,card_id#1L]
          +- *(4) Sort [card_id#4L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(card_id#4L, 200)
                +- Scan ExistingRDD[card_id#4L,first_name#5,last_name#6,age#7L]


**What Happened**:
* We seperated the data into 2 sets:
  * one where the `key`s are not `null`.
  * one where the `key`s are `null`.
* We perform the join on the set where the keys are not null, then union it back with the set where the keys are null. (This step is not necessary when doing an inner join).

**Results**:
* We brought less data to the join.
* We read the data twice; more time was spent on reading data from disk.

### Option #3: Cache the Table, Filter Null Keys First, then Join, then Union

**Helper Function**


```python
def null_skew_helper(left, right, key):
    """
    Steps:
        1. Cache table.
        2. Filter out the null rows.
        3. Create the columns you would get from the join.
        4. Join the tables.
        5. Union the null rows to joined table.
    """
    left = left.cache()
    
    df1 = left.where(F.col(key).isNull())
    for f in right.schema.fields:
            df1 = df1.withColumn(f.name, F.lit(None).cast(f.dataType))
    
    df2 = left.where(F.col(key).isNotNull())
    df2 = df2.join(right, key, "left")
    
    return df1.union(df2.select(df1.columns))
```


```python
df = null_skew_helper(customers, cards, "card_id")

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>card_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>age</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1.0</td>
      <td>john</td>
      <td>doe</td>
      <td>21.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.explain()
```

    == Physical Plan ==
    Union
    :- *(1) Project [id#0L, null AS card_id#68L, null AS first_name#71, null AS last_name#75, null AS age#80L]
    :  +- *(1) Filter isnull(card_id#1L)
    :     +- *(1) InMemoryTableScan [card_id#1L, id#0L], [isnull(card_id#1L)]
    :           +- InMemoryRelation [id#0L, card_id#1L], true, 10000, StorageLevel(disk, memory, deserialized, 1 replicas)
    :                 +- Scan ExistingRDD[id#0L,card_id#1L]
    +- *(5) Project [id#0L, card_id#1L, first_name#5, last_name#6, age#7L]
       +- SortMergeJoin [card_id#1L], [card_id#4L], LeftOuter
          :- *(3) Sort [card_id#1L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(card_id#1L, 200)
          :     +- *(2) Filter isnotnull(card_id#1L)
          :        +- *(2) InMemoryTableScan [id#0L, card_id#1L], [isnotnull(card_id#1L)]
          :              +- InMemoryRelation [id#0L, card_id#1L], true, 10000, StorageLevel(disk, memory, deserialized, 1 replicas)
          :                    +- Scan ExistingRDD[id#0L,card_id#1L]
          +- *(4) Sort [card_id#4L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(card_id#4L, 200)
                +- Scan ExistingRDD[card_id#4L,first_name#5,last_name#6,age#7L]


**What Happened**:
* Similar to option #2, but we did a `InMemoryTableScan` instead of two reads of the data.

**Results**:
* We brought less data to the join.
* We did 1 less read, but we used more memory.

### Summary

All to say:
* It's definitely better to bring less data to a join, so performing a filter for `null keys` before the join is definitely suggested.
* For `left join`s:
    * By doing a union, this will result in an extra read of data or memory usage.
    * Decide what you can afford; the extra read vs memory usage and `cache` the table before the `filter`.

Always check the spread the values for the `join key`, to detect if there's any skew and pre filters that can be performed.
