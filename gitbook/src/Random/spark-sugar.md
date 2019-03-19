
**DataTypes in PySpark:**  

RDD:
* Records

DataFrames:
* Rows
* Columns

Groupby:
* Groups

DataSets:


### df.withColumn in PySpark:
When you are do a `df.`**`withColumn()`** you cannot use the output of that expression in the same chained transform.

```python
>>> df = sc.sql.createDataFrame([('Alice', 2), ('Bob', 5)], ['name','age'])
```
```python
# what not to do
>>> df.withColumn('age', df['age']*2)\
...   .withColumn('new_age', df['age'] + 1)\
...   .collect()
[
    Row(struct=Row(age=4, new_age=3, name=u'Alice')), 
    Row(struct=Row(age=10, new_age=6, name=u'Bob'))
]     
# what to do
>>> df = df.withColumn('age', df['age']*2)
... df.withColumn('new_age', df['age'] + 1)\
...   .collect()
[
    Row(struct=Row(age=4, new_age=5, name=u'Alice')), 
    Row(struct=Row(age=10, new_age=11, name=u'Bob'))
]     
```

# Joins/Unions in PySpark

### Joins:

```python
>>> df = sc.sql.createDataFrame([('Alice', 2), ('Bob', 5)], ['name','age'])
>>> df_2 = sc.sql.createDataFrame([('Alice', 'female'), ('Bob', 'male')], ['name','gender'])
>>> df_3 = sc.sql.createDataFrame([('female', 'pink'), ('male', 'blue')], ['gender','color'])
```

*correctly joining DFs*  
When not done correctly you will get duplicated columns.
```python
# what not to do
>>> df_1.join(df_2, df_1['name'] == df_2['name'])\
...     .collect()
[
    Row(struct=Row(name=u'Alice', age=2, name=u'Alice', gender=u'female')), 
    Row(struct=Row(name=u'Bob', age=5, name=u'Bob', gender=u'male'))
]
# what to do
>>> df_1.join(df_2, 'name')\
...     .collect()
[
    Row(struct=Row(name=u'Alice', age=2, gender=u'female')), 
    Row(struct=Row(name=u'Bob', age=5, gender=u'male'))
]
```

*multiple joins*  
You can join multiple dataframes in one chained function.

```python
>>> df_1.join(df_2, 'name')\
...     .join(df_3, 'gender')
...     .collect()
[
    Row(struct=Row(name=u'Alice', gender=u'female', age=2)), 
    Row(struct=Row(name=u'Bob', gender=u'male', age=5))
]
```

### Unions:
When you union 2 DFs you need to union them so that the order of the columns match.

```python
>>> df_1 = sc.sql.createDataFrame([('Alice', 2), ('Bob', 5)], ['name','age'])
>>> df_2 = sc.sql.createDataFrame([(4, 'Eric'), (7, 'Steve')], ['age', 'name'])
```

```python
>>> df = df_1.union(df_2.select(df_1.columns))
...          .collect()
[
    Row(struct=Row(name=u'Alice', age=2)), 
    Row(struct=Row(name=u'Bob', age=5))
]
```

# Data-structures in PySpark

### F.lit in PySpark:
`pyspark.sql.functions.`**`lit`**`(col)`

Creates a **Column** of literal value.
**Parameters: col** – a literal value.

```python
>>> df = sc.sql.createDataFrame([('Alice', 2), ('Bob', 5)], ['name','age'])
>>> df.withColumn('age', F.lit(100))\
...   .collect()
[
    Row(struct=Row(age=100, name=u'Alice')), 
    Row(struct=Row(age=100, name=u'Bob'))
]
>>> df.withColumn('age', df['age'] > F.lit(3))\
...   .collect()
[
    Row(struct=Row(age=5, name=u'Bob'))
]
```
Use cases:
1. When you are trying to compare cols to literal values.
2. When you are trying to assign literal values to columns.


**Note**: When you are inside a `F.when(...).otherwise(...)` `F.lit()` is not needed.

### F.col in PySpark:
`pyspark.sql.functions.`**`col`**`(col)`

Returns a Column based on the given column name.
**Parameters: col** – a `column` name.

```python
>>> df = sc.sql.createDataFrame([('Alice', 2), ('Bob', 5)], ['name','age'])
>>> age = df['age']
>>> age = F.col('age')
```
Use cases:
1. It returns a column expression that binds to whatever DF it's part of when you run it

### F.struct in PySpark:
`pyspark.sql.functions.`**`struct`**`(*cols)`

Creates a new struct column.  
**Parameters:**	**cols** – list of column names (string) or list of **Column** expressions.

Example:
```python
>>> df = sc.sql.createDataFrame([('Alice', 2), ('Bob', 5)], ['name','age'])

>>> df.select(struct('age', 'name').alias("struct")).collect()
[
    Row(struct=Row(age=2, name=u'Alice')), 
    Row(struct=Row(age=5, name=u'Bob'))
]

>>> df.select(struct([df.age, df.name]).alias("struct")).collect()
[
    Row(struct=Row(age=2, name=u'Alice')), 
    Row(struct=Row(age=5, name=u'Bob'))
]
```

Use cases:
1. When you use `groupBy`, it drop any columns you're not grouping by or aggregating on.

# Windows in PySpark

add stuff

# SQL in PySpark

### F.coalesce in PySpark:
`pyspark.sql.functions.`**`coalesce`**`(*cols)`

Returns the first column that is not `null`.

Example:
```python
>>> cDf = sc.sql.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
>>> cDf.show()
+----+----+
|   a|   b|
+----+----+
|null|null|
|   1|null|
|null|   2|
+----+----+
```

```python
>>> cDf.select(coalesce(cDf["a"], cDf["b"])).show()
+--------------+
|coalesce(a, b)|
+--------------+
|          null|
|             1|
|             2|
+--------------+
```

```python
>>> cDf.select('*', coalesce(cDf["a"], lit(0.0))).show()
+----+----+----------------+
|   a|   b|coalesce(a, 0.0)|
+----+----+----------------+
|null|null|             0.0|
|   1|null|             1.0|
|null|   2|             0.0|
+----+----+----------------+
```
returns the last column that is not `null` and uses a default value is there is none.

Example:

```python
>>> ....
>>> "comment_present": F.when(F.col("comment_present").isNull(), False).otherwise(F.col("comment_present")),
>>> ...
```
can be
```python
>>> ...
>>> F.coalesce(F.col("comment_present"), F.lit(False))
>>> ...
```

A use case for `F.coalesce`.

### F.explode in PySpark:
`pyspark.sql.functions.explode(col)`

Returns a new row for each element in the given array or map.

**Multi-dimensional arrays collapsing in DataFrames:**  
Calling `F.explode(col)` on a 2-D array will flatten all 2-D arrays in `col`.

```python
>>> df = sc.sql.createDataFrame([(['a'],'a'),
...                             (['a', 'b'],'a'),
...                             (['c'],'b'),
...                             (['d', 'e'],'b')],
...                             ['arrays', 'group'])
>>> df = df.withColumn('arrays', F.explode('arrays'))
>>> df.groupBy('group').agg(F.collect_list('arrays').alias('arrays')).collect()
[Row(group=u'b', arrays=[u'c', u'd', u'e']), Row(group=u'a', arrays=[u'a', u'a', u'b'])]
```

**Creating Multiple rows from an array:**  
Calling `F.explode(col)` on an array will create multiple rows for each element in the list in `col`.
```python
>>> df = sc.sql.createDataFrame([(1, ["sample","support","zendesk"],)], 
...                             ['col1', 'tags'])
+----+--------------------+
|col1|                tags|
+----+--------------------+
|   1|[sample, support,...|
+----+--------------------+
>>> df.withColumn('tags', F.explode('tags')).show(truncate=False)
+----+-------+
|col1|tags   |
+----+-------+
|1   |sample |
|1   |support|
|1   |zendesk|
+----+-------+
```

### F.json_object() in PySpark:

```python
>>> df = sc.sql.createDataFrame([('[{"a": 1},{"a": 2}]',)], ['j'])
>>> df.select(F.get_json_object('j', '$')).show()
+---------------------+
|get_json_object(j, $)|
+---------------------+
|    [{"a":1},{"a":2}]|
+---------------------+

>>> df.select(F.get_json_object('j', '$[0]')).show()
+------------------------+
|get_json_object(j, $[0])|
+------------------------+
|                 {"a":1}|
+------------------------+
```

### Ternary Operators (F.when & F.otherwise) in PySpark:
`pyspark.sql.functions.`**`when`**`(condition, value)`  
Evaluates a list of conditions and returns one of multiple possible result expressions. If **Column.otherwise()** is not invoked, None is returned for unmatched conditions.

**Parameters:	
condition** – a boolean Column expression.  
**value** – a literal value, or a Column expression.

`pyspark.sql.functions.`**`otherwise`**`(value)`  
Evaluates a list of conditions and returns one of multiple possible result expressions. If Column.otherwise() is not invoked, None is returned for unmatched conditions.

**Parameters: value** – a literal value, or a **Column** expression.

```python
>>> df = sc.sql.createDataFrame([('Alice', 2), ('Bob', 5)], ['name','age'])
```

```python
>>> df.select(df.name, F.when(df.age > 3, 1).otherwise(0)).show()
+-----+-------------------------------------+
| name|CASE WHEN (age > 3) THEN 1 ELSE 0 END|
+-----+-------------------------------------+
|Alice|                                    0|
|  Bob|                                    1|
+-----+-------------------------------------+
```

**Example**:
```python
>>> F.when(F.col("campaign_medium").rlike("^(cpc|ppc|paidsearch)$"), F.lit("Paid Search"))
... .when(F.col("campaign_medium").rlike("^(cpv|cpa|cpp|content-text)$"), F.lit("Other Advertising"))
... .when(F.col("campaign_medium").rlike("^(display|cpm|banner)$"), F.lit("Display"))
... .when(F.col("campaign_medium") == F.lit("email"), F.lit("Email"))
... .when(F.col("campaign_medium") == F.lit("organic"), F.lit("Organic Search"))
... .when(F.col("campaign_medium") == F.lit("affiliate"), F.lit("Affiliates"))
... .when(F.col("campaign_medium") == F.lit("referral"), F.lit("Referral"))
```

You can chain multiple multiple `.when` statements like a switch/case operator.

**Python Functions**
* Functions are just variables in python. By writing it like this you save space and complexity as opposed to doing it in-line.

```python
>>> def derived_session_token_udf():
>>>    return F.concat(
...        F.col("shop_id").cast("string"), F.lit(":"),
...        F.col("user_token"), F.lit(":"),
...        F.col("session_token"), F.lit(":"),
...        F.year(F.col(timestamp_key)), F.lit(":"),
...        F.dayofyear(F.col(timestamp_key))
...    )

>>> new_df = df.withColumn("derived_session_token", derived_session_token_udf())
```

**`.to_dicts()` vs `.to_dataframe()`**

```python
>>> dt = DataTemplate(sc, Contract({'id': {'type': int}, 'foo': {'type': unicode, 'nullable': True}}), 
                                   {'id': 1})
>>> dt.to_dicts([{}])
... [{'id': 1}]

>>> dt.to_dataframe([{}]).collect()
... [Row(foo=None, id=1)]
```

### getItem(key)
An expression that gets an item at position ordinal out of a list, or gets an item by key out of a dict.

```python
>>> df = sc.parallelize([([1, 2], {"key": "value"})]).toDF(["l", "d"])
>>> df.select(df.l.getItem(0), df.d.getItem("key")).show()
+----+------+
|l[0]|d[key]|
+----+------+
|   1| value|
+----+------+
```

```python
>>> df.select(df.l[0], df.d["key"]).show()
+----+------+
|l[0]|d[key]|
+----+------+
|   1| value|
+----+------+
```

### When multiplying 2 Decimal Types
When you multiple 2 decimal types, you can come across values that are `None` or `0`. When the result is `None`, then this usually indicates a **casting** / **overflow** type error. The maximum precision of a **decimal value is 38**.

```python
>>> df = sc.sql.createDataFrame([(Decimal('113.790000000000000000'), Decimal('1.000000000000000000')),
...                              (Decimal('113.790000000000000000'), Decimal('2.000000000000000000')),
...                             ['total_price','local_to_usd'])

>>> df = df.withColumn('total_price_fx', F.col("total_price") * F.col("total_price"))\
...        .withColumn('total_price_fx2', F.col("total_price") * Decimal('2.50'))\
...        .withColumn('total_price_fx3', F.col("total_price").cast('Decimal(20, 2)') * F.col("local_to_usd"))
    
>>> df.collect()
[Row(total_price=Decimal('113.790000000000000000'), local_to_usd=Decimal('1.000000000000000000'), total_price_fx=None, total_price_fx2=Decimal('284.47500000000000000000'), total_price_fx3=Decimal('113.79000000000000000000')),
 Row(total_price=Decimal('113.790000000000000000'), local_to_usd=Decimal('2.000000000000000000'), total_price_fx=None, total_price_fx2=Decimal('284.47500000000000000000'), total_price_fx3=Decimal('227.58000000000000000000')),
 Row(total_price=Decimal('113.790000000000000000'), local_to_usd=Decimal('2.500000000000000000'), total_price_fx=None, total_price_fx2=Decimal('284.47500000000000000000'), total_price_fx3=Decimal('284.47500000000000000000')),
 Row(total_price=Decimal('113.790000000000006253'), local_to_usd=Decimal('2.500000000000000000'), total_price_fx=None, total_price_fx2=Decimal('284.47500000000001563250'), total_price_fx3=Decimal('284.47500000000000000000'))]
```

### `.take(...)` vs `.count()`

`.take` could be much slower if it is actually empty.

it assumes that it’s easy to satisfy the request, so it only processes a single partition from the final stage, and then gives you the first `(n)` results

if it can’t find `(n)` results, it’ll process 2 more partitions

and then 4

and then 8
