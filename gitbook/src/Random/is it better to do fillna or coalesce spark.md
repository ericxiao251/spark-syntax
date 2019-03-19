
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
        (1, "Not bot", "", ""), 
        (2, "Bot", "", ""), 
        (3, None, "", ""), 
    ], ['id', 'ua_form_factor', "user_agent", "ua_details"]
)

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>ua_form_factor</th>
      <th>user_agent</th>
      <th>ua_details</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Not bot</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Bot</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>None</td>
      <td></td>
      <td></td>
    </tr>
  </tbody>
</table>
</div>




```python
result = df.where(
    (F.col("ua_form_factor").isNull()) |
    (F.col("ua_form_factor") != 'Bot')
  ) \
  .drop("user_agent", "ua_details")

result.explain()
result.toPandas()
```

    == Physical Plan ==
    *(1) Project [id#166L, ua_form_factor#167]
    +- *(1) Filter (isnull(ua_form_factor#167) || NOT (ua_form_factor#167 = Bot))
       +- Scan ExistingRDD[id#166L,ua_form_factor#167,user_agent#168,ua_details#169]





<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>ua_form_factor</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Not bot</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




```python
result = df.where(F.coalesce(F.col("ua_form_factor"), F.lit("")) != 'Bot') \
  .drop("user_agent", "ua_details")

result.explain()
result.toPandas()
```

    == Physical Plan ==
    *(1) Project [id#166L, ua_form_factor#167]
    +- *(1) Filter NOT (coalesce(ua_form_factor#167, ) = Bot)
       +- Scan ExistingRDD[id#166L,ua_form_factor#167,user_agent#168,ua_details#169]





<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>ua_form_factor</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Not bot</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>


