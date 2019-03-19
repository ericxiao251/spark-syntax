
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import types as T
```

The above also shows you the "best practices" for importing these components into your program.

*some of the above imports will be explained later, just know this is how you should import these functions into your Spark application.

These are the essential `imports` that you will need for any `PySpark` program. 

**`SparkSession`**  
The `SparkSession` is how you begin a Spark application. This is where you provide some configuration for your Spark program.

**`pyspark.sql.functions`**  
You will find that all your data wrangling/analysis will mostly be done by chaining together multiple `functions`. If you find that you get your desired transformations with the base functions, you should:
1. Look through the API docs again.
2. Ask Google.
3. Write a `user defined function` (`udf`).

**`pyspark.sql.types`**  
When working with spark, you will need to define the type of data for each column you are working with. 

The possible types that Spark accepts are listed here: [Spark types](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)

### Hello World


```python
spark = (
    SparkSession.builder
    .master("local")
    .appName("Section 3 - Reading your First Dataset")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

sc = spark.sparkContext
```

Create a `SparkSession`. No need to create `SparkContext` as you automatically get it as part of the `SparkSession`.

### Read in Data (CSV)


```python
# define the structure of your data inside the CSV file
def get_csv_schema(*args):
    return T.StructType([
        T.StructField(*arg)
        for arg in args
    ])

# read in your csv file with enforcing a schema
def read_csv(fname, schema):
    return spark.read.csv(
        path=fname,
        header=True,
        schema=get_csv_schema(*schema)
    )
```


```python
import os

data_path = "/data"
pets_path = "/pets.csv"
base_path = os.path.dirname(os.getcwd())

path = base_path + data_path + pets_path
df = read_csv(
    fname=path,
    schema=[
        ("id", T.LongType(), False),
        ("breed_id", T.LongType(), True),
        ("name", T.StringType(), True),
        ("birthday", T.TimestampType(), True),
        ("color", T.StringType(), True)
    ]
)
```


```python
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



### What Happened?
Here we read in a `csv` file and put it into a `DataFrame (DF)`: this is one of the three datasets that Spark allows you to use. The other two are `Resilient Distributed Dataset (RDD)` and `Dataset`. `DF`s have replaced `RDD`s as more features have been brought out in version `2.x` of Spark. You should be able to perform anything with `DataFrames` now, if not you will have to work with `RDD`s, which I will not cover.

Spark gives you the option to automatically infer the schema and types of columns in your dataset. But you should always specify a `schema` for the data that you're reading in. For each column in the `csv` file we specified:
* the `name` of the column
* the `data type` of the column
* if `null` values can appear in the column

### Conclusion
Congratulations! You've read in your first dataset in Spark. Next we'll look at how you can perform transformations on this dataset :).
