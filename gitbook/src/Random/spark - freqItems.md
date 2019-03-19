
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
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
df = spark.createDataFrame(
    [
        (1, 'facebook.com'),
        (1, 'facebook.com'),
        (2, 'snapchat.com'),
        (2, None),
        (None, 'twitter.com'),
    ], ['shop_id', 'shop_domain']
)

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>shop_id</th>
      <th>shop_domain</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1.0</td>
      <td>facebook.com</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1.0</td>
      <td>facebook.com</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2.0</td>
      <td>snapchat.com</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>NaN</td>
      <td>twitter.com</td>
    </tr>
  </tbody>
</table>
</div>




```python
join_keys = ['shop_id', 'shop_domain']

df = df.withColumn('natural_key', F.struct(*(F.col(col) for col in join_keys)))
df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>shop_id</th>
      <th>shop_domain</th>
      <th>join_key</th>
      <th>natural_key</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1.0</td>
      <td>facebook.com</td>
      <td>(1, facebook.com)</td>
      <td>(1, facebook.com)</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1.0</td>
      <td>facebook.com</td>
      <td>(1, facebook.com)</td>
      <td>(1, facebook.com)</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2.0</td>
      <td>snapchat.com</td>
      <td>(2, snapchat.com)</td>
      <td>(2, snapchat.com)</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2.0</td>
      <td>None</td>
      <td>(2, None)</td>
      <td>(2, None)</td>
    </tr>
    <tr>
      <th>4</th>
      <td>NaN</td>
      <td>twitter.com</td>
      <td>(None, twitter.com)</td>
      <td>(None, twitter.com)</td>
    </tr>
  </tbody>
</table>
</div>




```python
freq_df = df.freqItems(
    ['join_key'], 
    support=0.6
).withColumn('temp', F.explode('join_key_freqItems'))
freq_df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>join_key_freqItems</th>
      <th>temp</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>[(None, twitter.com)]</td>
      <td>(None, twitter.com)</td>
    </tr>
  </tbody>
</table>
</div>




```python

```
