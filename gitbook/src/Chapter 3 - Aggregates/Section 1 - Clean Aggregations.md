
### Library Imports


```python
from datetime import datetime

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

### Initial Datasets


```python
pets = spark.createDataFrame(
    [
        (1, 1, 'Bear', 5),
        (2, 1, 'Chewie', 10),
        (3, 2, 'Roger', 15),
    ], ['id', 'breed_id', 'nickname', 'age']
)

pets.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>nickname</th>
      <th>age</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>Chewie</td>
      <td>10</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2</td>
      <td>Roger</td>
      <td>15</td>
    </tr>
  </tbody>
</table>
</div>




```python
groupby_columns = ['breed_id']
```

### Option 1: Using a Dictionary


```python
df_1 = (
    pets
    .groupby(groupby_columns)
    .agg({
        "*": "count",
        "age": "sum",
    })
)

df_1.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>count(1)</th>
      <th>sum(age)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2</td>
      <td>15</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>15</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened:**
* Very similar to `pandas` `agg` function.
* The resultant column names are a bit awkward to use after the fact.

### Option 2: Using List of Columns


```python
df_2 = (
    pets
    .groupby(groupby_columns)
    .agg(
        F.count("*"),
        F.sum("age"),
    )
)

df_2.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>count(1)</th>
      <th>sum(age)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2</td>
      <td>15</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>15</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened:**
* Here we use the Spark `agg` functions.
* Again, the resultant column names are a bit awkward to use after the fact.

### Option 3: Using List of Columns, with Aliases


```python
df_3 = (
    pets 
    .groupby(groupby_columns)
    .agg(
        F.count("*").alias("count_of_breeds"),
        F.sum("age").alias("total_age_of_breeds"),
    )
)

df_3.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>count_of_breeds</th>
      <th>total_age_of_breeds</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2</td>
      <td>15</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>15</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened:**
* Here we use the Spark `agg` functions and `alias`ed the resultant columns to new names.
* This provides cleaner column names that we can use later on.

### Summary

**I encourage using option #3.**

This creates more elegant and meaning names for the new aggregate columns.

A `withColumnRenamed` can be performed after the aggregates, but why not do it with an `alias`? It's easier as well.
