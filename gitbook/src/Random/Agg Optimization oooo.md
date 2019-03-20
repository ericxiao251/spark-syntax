
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
```

Create a `SparkSession`. No need to create `SparkContext` as you automatically get it as part of the `SparkSession`.


```python
spark = SparkSession.builder \
    .master("local") \
    .appName("Exploring Joins") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext
```


```python
test_df = spark.createDataFrame(
    [
        (1, 1, 1),
        (1, 2, 1),
        (2, 1, 1),
        (2, 1, 1),
    ], ['id','salt_key', 'amount']
)

test_df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>salt_key</th>
      <th>amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>1</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



first aggregate


```python
group_by_cols = ['id']

agg_df = test_df \
    .groupby(group_by_cols + ['salt_key']) \
    .agg(F.sum('amount').alias('amount'))

agg_df.toPandas()

```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>salt_key</th>
      <th>amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



second aggregate


```python
agg_df \
    .groupby(group_by_cols) \
    .agg(F.sum('amount').alias('amount')) \
    .toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>


