
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
    .appName("Exploring Joins")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

sc = spark.sparkContext
```

### Create a DataFrame


```python
schema = T.StructType([
    T.StructField("pet_id", T.IntegerType(), False),
    T.StructField("name", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True),
])

data = [
    (1, "Bear", 13), 
    (2, "Chewie", 12), 
    (2, "Roger", 1), 
]

pet_df = spark.createDataFrame(
    data=data,
    schema=schema
)

pet_df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pet_id</th>
      <th>name</th>
      <th>age</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Bear</td>
      <td>13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Chewie</td>
      <td>12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Roger</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



### Background
There are 3 datatypes in spark `RDD`, `DataFrame` and `Dataset`. As mentioned before, we will focus on the `DataFrame` datatype. 

* This is most performant and commonly used datatype. 
* `RDD`s are a thing of the past and you should refrain from using them unless you can't do the transformation in `DataFrame`s.
* `Dataset`s are a thing in `Spark scala`.

If you have used a `DataFrame` in Pandas, this is the same thing. If you haven't, a dataframe is similar to a `csv` or `excel` file. There are columns and rows that you can perform transformations on. You can search online for better descriptions of what a `DataFrame` is.

### What Happened?
For any `DataFrame (df)` that you work with in Spark you should provide it with 2 things:
1. a `schema` for the data. Providing a `schema` explicitly makes it clearer to the reader and sometimes even more performant, if we can know that a column is `nullable`. This means providing 3 things:
    * the `name` of the column
    * the `datatype` of the column
    * the `nullability` of the column
2. the data. Normally you would read data stored in `gcs`, `aws` etc and store it in a `df`, but there will be the off-times that you will need to create one. 
