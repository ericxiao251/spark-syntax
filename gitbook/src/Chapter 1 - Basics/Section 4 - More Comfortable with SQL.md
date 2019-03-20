
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import types as T
```

### Template


```python
spark = (
    SparkSession.builder
    .master("local")
    .appName("Section 4 - More Comfortable with SQL?")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

sc = spark.sparkContext

import os

data_path = "/data/pets.csv"
base_path = os.path.dirname(os.getcwd())
path = base_path + data_path

df = spark.read.csv(path, header=True)
df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>species_id</th>
      <th>name</th>
      <th>birthday</th>
      <th>color</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>brown</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



### Register DataFrame as a SQL Table


```python
df.createOrReplaceTempView("pets")
```

### What Happened?
The first step in making a `df` queryable with `SQL` is to **register** the table as a sql table.

This particular function will **replace** any previously registered **local** table named `pets` as a result. There are other functions that will register a dataframe with slightly different behavior. You can check the reference docs if this isn't the desired behavior: [docs](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.createGlobalTempView)

### Let Write a SQL Query!


```python
df_2 = spark.sql("""
SELECT 
    *
FROM pets
WHERE name = 'Argus'
""")

df_2.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>species_id</th>
      <th>name</th>
      <th>birthday</th>
      <th>color</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



### What Happened?
Once your `df` is registered, call the spark `sc` function on your `spark session` object. It takes a `sql string` as an input and outputs a new `df`.

### Conclusion?
If you're more comfortable with writing `sql` than python/spark code, then you can do so with a spark `df`! We do this by:
1. Register the `df` with `df.createOrReplaceTempView('table')`.
2. Call the `sql` function on your `spark session` with a `sql string` as an input.
3. You're done!
