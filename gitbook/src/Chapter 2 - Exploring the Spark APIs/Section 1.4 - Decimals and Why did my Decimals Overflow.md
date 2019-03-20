
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
    .appName("Section 1.4 - Decimals and Why did my Decimals overflow")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

sc = spark.sparkContext

def get_csv_schema(*args):
    return T.StructType([
        T.StructField(*arg)
        for arg in args
    ])

def read_csv(fname, schema):
    return spark.read.csv(
        path=fname,
        header=True,
        schema=get_csv_schema(*schema)
    )

import os

data_path = "/data/pets.csv"
base_path = os.path.dirname(os.getcwd())
path = base_path + data_path
```

### Decimals and Why did my Decimals overflow

Some cases where you would deal with `Decimal` types are if you are talking about money, height, weight, etc. Working with `Decimal` types may appear simple at first but there are some nuances that will sneak up behind you. We will go through some ways to get around these as they are hard to debug.

Here is some simple jargon that we will use in the following examples:
* `Integer`: The set of numbers including all the whole numbers and their opposites (the positive whole numbers, the negative whole numbers, and zero). ie. -1, 0, 1, 2, etc.
* `Irrational Number`: The set including all numbers that are non- terminating, non- repeating decimals. ie. 2.1, 10.5, etc.
* `Precision`: the maximum total number of digits.
* `Scale`: the number of digits on the right of dot.

Source:
* [link](https://www.sparknotes.com/math/prealgebra/integersandrationals/terms/)
* [link](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.types.DecimalType)

### Case 1: Working With `Decimal`s in Python


```python
print("Example 1 - {}".format(Decimal(20)))
print("Example 2 - {}".format(Decimal("20.2")))
print("Example 3 - {}".format(Decimal(20.5)))
print("Example 4 - {}".format(Decimal(20.2)))
```

    Example 1 - 20
    Example 2 - 20.2
    Example 3 - 20.5
    Example 4 - 20.199999999999999289457264239899814128875732421875


**What Happened?**

Let's break down the examples above.

Example 1:

Here we provided a whole number, so nothing special.

Example 2:

Here we provided a string representing an irrational number. The `precision` and `scale` were preserved.

Example 3:

Here we provided an irrational number. The `precision` and `scale` were preserved.

Example 4:

Here we provided an irrational number, but this isn't what we expected? Well this is because it's impossible to provide an exact representation of `20.2` on the computer! If you want to know more about this you can look up "IEEE floating point representation". We will keep this in mind for later on.

### IEEE Floating Point Represenatation (OUT OF SCOPE OF HANDBOOK)

When you were little have you ever tried to divide 10 by 3, did the result ever terminate? No. You were left with 3.33333... This is a similar case with trying to represent some `irrational numbers` on a computer.

> "There's only 10 types of people in the world: those who get binary and those who don't."

Let's think about how a computer works, if we go down to the lowest level, everything is stored as a binary value either a 0 or 1. Every whole number is easily representable, `11` is 3, `101` is 5, etc. But when it comes to `irrational numbers` there has been extensive work and established standards to representing these numbers most correctly. One of these standards are called the `IEEE-754 32-bit Single-Precision Floating-Point Numbers`.

**IEEE-754 32-bit Single-Precision Floating-Point Numbers**

![](https://github.com/ericxiao251/spark-syntax/blob/master/src/images/ieee-floating-point-representation.png)

From the picture above we can see that to represent a single irrational or floating point number, you only have 32 bits (or 64 in other cases) and a set number of bits for each portion of the number. Only 1 bit is ever needed for the `signed` bit, then the number of bits will vary for the whole (exponent) number and then decimal fractional) values.

I won't show you how the whole number and floating point values are computed, you can refer to other references for this. For example, this is a very good example: [link](https://www.youtube.com/watch?v=8afbTaA-gOQ).

Let's look at our examples above:

|example|signed|exponent  |fraction               |
|:------|-----:|---------:|-----------------------|
|20.2   |0     |010000011 |01000011001100110011010|
|20.5   |0     |010000011 |01001000000000000000000|

If you did the calculations for `20.2` you will notice that the fraction portion never actually divides nicely. Like I mentioned with the simply example above with trying to divide 10 by 3 nicely, it's impoosible you will always have some leftover/remainder values. But with `20.5` we can see that it divides nicely.

### Case 2: Reading in Decimals in Spark (Incorrectly)


```python
pets = read_csv(
    fname=path,
    schema=[
        ("id", T.LongType(), False),
        ("breed_id", T.LongType(), True),
        ("nickname", T.StringType(), True),
        ("birthday", T.TimestampType(), True),
        ("age", T.LongType(), True),
        ("color", T.StringType(), True),
        ("weight", T.DecimalType(), True),
    ]
)
pets.show()
```

    +---+--------+--------+-------------------+---+-----+------+
    | id|breed_id|nickname|           birthday|age|color|weight|
    +---+--------+--------+-------------------+---+-----+------+
    |  1|       1|    King|2014-11-22 12:30:31|  5|brown|    10|
    |  2|       3|   Argus|2016-11-22 10:05:10| 10| null|     6|
    |  3|       1|  Chewie|2016-11-22 10:05:10| 15| null|    12|
    |  3|       2|   Maple|2018-11-22 10:05:10| 17|white|     3|
    |  4|       2|    null|2019-01-01 10:05:10| 13| null|    10|
    +---+--------+--------+-------------------+---+-----+------+
    


**What Happened?**

What happened to our `scalar` values, they weren't read in? This is because the default arguments to the `T.Decimal()` function are `DecimalType(precision=10, scale=0)`. So to read in the data correctly we need to override these default arguments.

### Case 2: Reading in Decimals in Spark (Correctly)


```python
pets = read_csv(
    fname=path,
    schema=[
        ("id", T.LongType(), False),
        ("breed_id", T.LongType(), True),
        ("nickname", T.StringType(), True),
        ("birthday", T.TimestampType(), True),
        ("age", T.LongType(), True),
        ("color", T.StringType(), True),
        ("weight", T.DecimalType(10,2), True),
    ]
)
pets.show()
```

    +---+--------+--------+-------------------+---+-----+------+
    | id|breed_id|nickname|           birthday|age|color|weight|
    +---+--------+--------+-------------------+---+-----+------+
    |  1|       1|    King|2014-11-22 12:30:31|  5|brown| 10.00|
    |  2|       3|   Argus|2016-11-22 10:05:10| 10| null|  5.50|
    |  3|       1|  Chewie|2016-11-22 10:05:10| 15| null| 12.00|
    |  3|       2|   Maple|2018-11-22 10:05:10| 17|white|  3.40|
    |  4|       2|    null|2019-01-01 10:05:10| 13| null| 10.00|
    +---+--------+--------+-------------------+---+-----+------+
    


### Case 3: Reading in Large Decimals in Spark


```python
spark.createDataFrame(
    data=[
        (100,),
        (2 ** 63,)
    ],
    schema=['data']
).show()
```

    +----+
    |data|
    +----+
    | 100|
    |null|
    +----+
    


**What Happened?**

Why is the second value null? The second value overflows the max value of a decimal and never makes it to Spark (Scala). 

If you see this error then you will need to check your input data as there might be something wrong there.

### Case 3: Setting Values in a DataFrame (Incorrectly)


```python
(
    pets
    .withColumn('decimal_column', F.lit(Decimal(20.2)))
    .show()
)
```


    ---------------------------------------------------------------------------

    AnalysisException                         Traceback (most recent call last)

    <ipython-input-7-14e51ddcba87> in <module>()
          1 (
          2     pets
    ----> 3     .withColumn('decimal_column', F.lit(Decimal(20.2)))
          4     .show()
          5 )


    /usr/local/lib/python2.7/site-packages/pyspark/sql/functions.pyc in _(col)
         40     def _(col):
         41         sc = SparkContext._active_spark_context
    ---> 42         jc = getattr(sc._jvm.functions, name)(col._jc if isinstance(col, Column) else col)
         43         return Column(jc)
         44     _.__name__ = name


    /usr/local/lib/python2.7/site-packages/py4j/java_gateway.pyc in __call__(self, *args)
       1255         answer = self.gateway_client.send_command(command)
       1256         return_value = get_return_value(
    -> 1257             answer, self.gateway_client, self.target_id, self.name)
       1258 
       1259         for temp_arg in temp_args:


    /usr/local/lib/python2.7/site-packages/pyspark/sql/utils.pyc in deco(*a, **kw)
         67                                              e.java_exception.getStackTrace()))
         68             if s.startswith('org.apache.spark.sql.AnalysisException: '):
    ---> 69                 raise AnalysisException(s.split(': ', 1)[1], stackTrace)
         70             if s.startswith('org.apache.spark.sql.catalyst.analysis'):
         71                 raise AnalysisException(s.split(': ', 1)[1], stackTrace)


    AnalysisException: u'DecimalType can only support precision up to 38;'


**What Happened?**

Remember our python examples above? Well because the `precision` of the Spark `T.DecimalType` is 38 digits, the value went over the maximum value of the Spark type.

### Case 3: Setting Values in a DataFrame (Correctly)


```python
(
    pets
    .withColumn('decimal_column', F.lit(Decimal("20.2")))
    .show()
)
```

    +---+--------+--------+-------------------+---+-----+------+--------------+
    | id|breed_id|nickname|           birthday|age|color|weight|decimal_column|
    +---+--------+--------+-------------------+---+-----+------+--------------+
    |  1|       1|    King|2014-11-22 12:30:31|  5|brown| 10.00|          20.2|
    |  2|       3|   Argus|2016-11-22 10:05:10| 10| null|  5.50|          20.2|
    |  3|       1|  Chewie|2016-11-22 10:05:10| 15| null| 12.00|          20.2|
    |  3|       2|   Maple|2018-11-22 10:05:10| 17|white|  3.40|          20.2|
    |  4|       2|    null|2019-01-01 10:05:10| 13| null| 10.00|          20.2|
    +---+--------+--------+-------------------+---+-----+------+--------------+
    


**What Happened?**

If we provide the irrational number as a string, this solves the problem.

### Case 4: Performing Arthimetrics with `DecimalType`s (Incorrectly)


```python
pets = spark.createDataFrame(
    data=[
        (Decimal('113.790000000000000000'), Decimal('2.54')),
        (Decimal('113.790000000000000000'), Decimal('2.54')),
    ],
    schema=['weight_in_kgs','conversion_to_lbs']
)

pets.show()
```

    +--------------------+--------------------+
    |       weight_in_kgs|   conversion_to_lbs|
    +--------------------+--------------------+
    |113.7900000000000...|2.540000000000000000|
    |113.7900000000000...|2.540000000000000000|
    +--------------------+--------------------+
    



```python
(
    pets
    .withColumn('weight_in_lbs', F.col('weight_in_kgs') * F.col('conversion_to_lbs'))
    .show()
)
```

    +--------------------+--------------------+-------------+
    |       weight_in_kgs|   conversion_to_lbs|weight_in_lbs|
    +--------------------+--------------------+-------------+
    |113.7900000000000...|2.540000000000000000|   289.026600|
    |113.7900000000000...|2.540000000000000000|   289.026600|
    +--------------------+--------------------+-------------+
    


**What Happened?**

This used to overflow... Guess they updated it 😅.

### Case 4: Performing Arthimetrics Operations with DecimalTypes (Correctly)


```python
(
    pets
    .withColumn('weight_in_kgs', F.col('weight_in_kgs').cast('Decimal(20,2)'))
    .withColumn('conversion_to_lbs', F.col('conversion_to_lbs').cast('Decimal(20,2)'))
    .withColumn('weight_in_lbs', F.col('weight_in_kgs') * F.col('conversion_to_lbs'))
    .show()
)
```

    +-------------+-----------------+-------------+
    |weight_in_kgs|conversion_to_lbs|weight_in_lbs|
    +-------------+-----------------+-------------+
    |       113.79|             2.54|     289.0266|
    |       113.79|             2.54|     289.0266|
    +-------------+-----------------+-------------+
    


**What Happened?**

Before doing the calculations, we truncated (with the help of the `cast` function, which we will learn about in the later chapters) all of the values to be only 2 `scalar` digits at most. This is how you should perform your arithmetic operations with `Decimal Types`. Ideally you should know the minimum number of `scalar` digits needed for each datatype.

### Summary

* We learned that you should always initial `Decimal` types using string represented numbers, if they are an Irrational Number.
* When reading in `Decimal` types, you should explicitly override the default arguments of the Spark type and make sure that the underlying data is correct.
* When performing arithmetic operations with decimal types you should always truncate the scalar digits to the lowest number of digits as possible, if you haven't already.
