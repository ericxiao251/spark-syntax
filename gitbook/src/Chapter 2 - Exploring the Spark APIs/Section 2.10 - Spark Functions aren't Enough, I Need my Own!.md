
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from pyspark.sql import functions as F

from datetime import datetime
from decimal import Decimal
```

### Template


```python
spark = (
    SparkSession.builder
    .master("local")
    .appName("Section 2.10 - Spark Functions aren't Enough, I Need my Own!")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

sc = spark.sparkContext

import os

data_path = "/data/pets.csv"
base_path = os.path.dirname(os.getcwd())
path = base_path + data_path
```


```python
pets = spark.read.csv(path, header=True)
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
      <th>birthday</th>
      <th>age</th>
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
      <td>5</td>
      <td>brown</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>2</td>
      <td>Maple</td>
      <td>2018-11-22 10:05:10</td>
      <td>17</td>
      <td>white</td>
    </tr>
  </tbody>
</table>
</div>



### Spark Functions aren't Enough, I Need my Own!

What happens when you can't find functions that can perform what you want? Try looking again 🤪. But if this is really the case, your last resort can be to implement an `udf` short for `user defined function`.

These are functions written in python code that take a subset of columns as the input and returns a new column back. There are multiple steps in creating a `udf`, we'll walk through one and decompose it step by step. 

### Option 1: Steps


```python
# Step 1: Create your own function
def uppercase_words(word, cuttoff_length=2):
    return word.upper()[:cuttoff_length] if word else None

s = 'Hello World!'
print(s)
print(uppercase_words(s, 20))

# Step 2: Register the udf as a spark udf
uppercase_words_udf = F.udf(uppercase_words, T.StringType())

# Step 3: Use it!
(
    pets
    .withColumn('nickname_uppercase', uppercase_words_udf(F.col('nickname')))
    .withColumn('color_uppercase', uppercase_words_udf(F.col('color')))
    .withColumn('color_uppercase_trimmed', uppercase_words_udf(F.col('color'), F.lit(3)))
    .toPandas()
)
```

    Hello World!
    HELLO WORLD!





<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>nickname</th>
      <th>birthday</th>
      <th>age</th>
      <th>color</th>
      <th>nickname_uppercase</th>
      <th>color_uppercase</th>
      <th>color_uppercase_trimmed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>KI</td>
      <td>BR</td>
      <td>BRO</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>None</td>
      <td>AR</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
      <td>CH</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>2</td>
      <td>Maple</td>
      <td>2018-11-22 10:05:10</td>
      <td>17</td>
      <td>white</td>
      <td>MA</td>
      <td>WH</td>
      <td>WHI</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

Although the upper function is defined in the spark `fuctions` library it still serves as a good example. Let's breakdown the steps involved:
1. Create the function that you want (`uppercase_words`), remembering that only `spark columnar objects` are accepted as input arguments to the function. This means if you want to use other values, you will need to cast it to a column object using `F.lit()` from the previous sections.
2. Register the python function as a spark function, and specify the spark return type. The format is like so `F.udf(python_function, spark_return_type)`.
3. Now you can use the function!

### Option 2: 1 Less Step


```python
from pyspark.sql.functions import udf

# Step 1: Create and register your own function
@udf('string', 'int')
def uppercase_words(word, cuttoff_length=2):
    return word.upper()[:cuttoff_length] if word else None

# Step 2: Use it!
(
    pets
    .withColumn('color_uppercase', uppercase_words_udf(F.col('color')))
    .toPandas()
)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>breed_id</th>
      <th>nickname</th>
      <th>birthday</th>
      <th>age</th>
      <th>color</th>
      <th>color_uppercase</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>King</td>
      <td>2014-11-22 12:30:31</td>
      <td>5</td>
      <td>brown</td>
      <td>BR</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
      <td>Argus</td>
      <td>2016-11-22 10:05:10</td>
      <td>10</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Chewie</td>
      <td>2016-11-22 10:05:10</td>
      <td>15</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>2</td>
      <td>Maple</td>
      <td>2018-11-22 10:05:10</td>
      <td>17</td>
      <td>white</td>
      <td>WH</td>
    </tr>
  </tbody>
</table>
</div>



**What Happened?**

The `udf` function can also be used as a decorator to register your python functions as spark functions. 

Where the inputs are the types of the arguments to the `udf`.

### The Ugly Part of `udf`s

**TL;DR** `Spark function`s are executed on the JVM, while `Python UDF`s are executed in Python. This will require extra python memory for your spark application (will explain in Chapter 6) and more passing of data between the JVM and Python.

If your function can be performed with the spark `functions`, you should alway use the spark `functions`. `udf`s perform very poorly compared to the spark `functions`. This is a greate response that encapsulates the reason as to why:

> The main reasons are already enumerated above and can be reduced to a simple fact that Spark DataFrame is natively a JVM structure and standard access methods are implemented by simple calls to Java API. UDF from the other hand are implemented in Python and require moving data back and forth.
>
> While PySpark in general requires data movements between JVM and Python, in case of low level RDD API it typically doesn't require expensive serde activity. Spark SQL adds additional cost of serialization and serialization as well cost of moving data from and to unsafe representation on JVM. The later one is specific to all UDFs (Python, Scala and Java) but the former one is specific to non-native languages.
>
> Unlike UDFs, Spark SQL functions operate directly on JVM and typically are well integrated with both Catalyst and Tungsten. It means these can be optimized in the execution plan and most of the time can benefit from codgen and other Tungsten optimizations. Moreover these can operate on data in its "native" representation.
>
> So in a sense the problem here is that Python UDF has to bring data to the code while SQL expressions go the other way around.

source: https://stackoverflow.com/questions/38296609/spark-functions-vs-udf-performance

### Option 3: Pandas Vectorized `UDF`s


```python
# TODO: https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html
```

### Summary

* We learnt how to use a python function within spark, called `udf`s.
* We learnt how to pass non-column objects into the function by using knowledge gained from previous chapters.
* We learnt about the bad parts of `udf`s and their performance issues.
