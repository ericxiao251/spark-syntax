
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from datetime import datetime
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
        (1, 1, datetime(2018, 1, 1, 1 ,1, 1), 'Bear', 5),
        (2, 1, datetime(2015, 1, 1, 1 ,1, 1), 'Chewie', 10),
        (3, 1, datetime(2015, 1, 1, 1 ,1, 1), 'Roger', 15),
    ], ['id', 'breed_id', 'birthday', 'nickname', 'age']
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
      <td>1</td>
      <td>Roger</td>
      <td>15</td>
    </tr>
  </tbody>
</table>
</div>



### Scenario #1

No `orderBy` specified for `window` object.


```python
window_1 = Window.partitionBy('breed_id')

df_1 = pets.withColumn('foo', (F.sum(F.col('age')).over(window_1)))

df_1.toPandas()
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
      <th>foo</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
      <td>5</td>
      <td>30</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>Chewie</td>
      <td>10</td>
      <td>30</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Roger</td>
      <td>15</td>
      <td>30</td>
    </tr>
  </tbody>
</table>
</div>



### Scenario #2

`orderBy` with no `rowsBetween` specified for `window` object.


```python
window_2 = (
    Window
    .partitionBy('breed_id')
    .orderBy(F.col('id'))
)    

df_2 = pets.withColumn('foo', (F.sum(F.col('age')).over(window_2)))

df_2.toPandas()
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
      <th>foo</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
      <td>5</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>Chewie</td>
      <td>10</td>
      <td>15</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Roger</td>
      <td>15</td>
      <td>30</td>
    </tr>
  </tbody>
</table>
</div>



### Scenario #3

`orderBy` with a `rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)` specified for `window` object.


```python
window_3 = (
    Window
    .partitionBy('breed_id')
    .orderBy(F.col('id'))
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

df_3 = pets.withColumn('foo', (F.sum(F.col('age')).over(window_3)))

df_3.toPandas()
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
      <th>foo</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
      <td>5</td>
      <td>30</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>Chewie</td>
      <td>10</td>
      <td>30</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>1</td>
      <td>Roger</td>
      <td>15</td>
      <td>30</td>
    </tr>
  </tbody>
</table>
</div>



## Why is This?


```python
df_1.explain()
```

    == Physical Plan ==
    Window [sum(age#3L) windowspecdefinition(breed_id#1L, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS foo#9L], [breed_id#1L]
    +- *(1) Sort [breed_id#1L ASC NULLS FIRST], false, 0
       +- Exchange hashpartitioning(breed_id#1L, 200)
          +- Scan ExistingRDD[id#0L,breed_id#1L,nickname#2,age#3L]



```python
df_2.explain()
```

    == Physical Plan ==
    Window [sum(age#3L) windowspecdefinition(breed_id#1L, id#0L ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS foo#216L], [breed_id#1L], [id#0L ASC NULLS FIRST]
    +- *(1) Sort [breed_id#1L ASC NULLS FIRST, id#0L ASC NULLS FIRST], false, 0
       +- Exchange hashpartitioning(breed_id#1L, 200)
          +- Scan ExistingRDD[id#0L,breed_id#1L,nickname#2,age#3L]



```python
df_3.explain()
```

    == Physical Plan ==
    Window [sum(age#3L) windowspecdefinition(breed_id#1L, id#0L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS foo#423L], [breed_id#1L], [id#0L ASC NULLS FIRST]
    +- *(1) Sort [breed_id#1L ASC NULLS FIRST, id#0L ASC NULLS FIRST], false, 0
       +- Exchange hashpartitioning(breed_id#1L, 200)
          +- Scan ExistingRDD[id#0L,breed_id#1L,nickname#2,age#3L]


### TL;DR

By looking at the **Physical Plan**, the default behaviour for `Window.partitionBy('col_1').orderBy('col_2')` without a `.rowsBetween()` is to do `.rowsBetween(Window.unboundedPreceding, Window.currentRow)`.

Looking at the scala code we can see that this is indeed the default and intended behavior, https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/expressions/Window.scala#L36-L38.

```scala
 * @note When ordering is not defined, an unbounded window frame (rowFrame, unboundedPreceding,
 *       unboundedFollowing) is used by default. When ordering is defined, a growing window frame
 *       (rangeFrame, unboundedPreceding, currentRow) is used by default.
```

**Problem:**
This will cause problems if you're care about all the rows in the partitions.

