
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
        (1, 1, 'Bear'),
        (2, 1, 'Chewie'),
        (3, 2, 'Roger'),
    ], ['id', 'breed_id', 'nickname']
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
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>Chewie</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2</td>
      <td>Roger</td>
    </tr>
  </tbody>
</table>
</div>




```python
breeds = spark.createDataFrame(
    [
        (1, 'Pitbull', 10), 
        (2, 'Corgie', 20), 
    ], ['breed_id', 'name', 'average_height']
)

breeds.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>name</th>
      <th>average_height</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Corgie</td>
      <td>20</td>
    </tr>
  </tbody>
</table>
</div>



### Filter Pushdown

> `Filter pushdown` improves performance by reducing the amount of data shuffled during any dataframes transformations.

Depending on your filter logic and where you place your filter code. Your Spark code will behave differently.

### Case #1: Filtering on Only One Side of the Join


```python
df = (
    pets
    .join(breeds, 'breed_id', 'left_outer')
    .filter(F.col('nickname') == 'Chewie')
)

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>id</th>
      <th>nickname</th>
      <th>name</th>
      <th>average_height</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2</td>
      <td>Chewie</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.explain()
```

    == Physical Plan ==
    *(4) Project [breed_id#1L, id#0L, nickname#2, name#7, average_height#8L]
    +- SortMergeJoin [breed_id#1L], [breed_id#6L], LeftOuter
       :- *(2) Sort [breed_id#1L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(breed_id#1L, 200)
       :     +- *(1) Filter (isnotnull(nickname#2) && (nickname#2 = Chewie))
       :        +- Scan ExistingRDD[id#0L,breed_id#1L,nickname#2]
       +- *(3) Sort [breed_id#6L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(breed_id#6L, 200)
             +- Scan ExistingRDD[breed_id#6L,name#7,average_height#8L]


**What Happened:**

Because the column `nickname` is only present in the `left` side of the join, only the `left` side of the join was `filtered` before the join.

### Case #2: Filter on Both Sides of the Join


```python
df = (
    pets
    .join(breeds, 'breed_id', 'left_outer')
    .filter(F.col('breed_id') == 1)
)

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>id</th>
      <th>nickname</th>
      <th>name</th>
      <th>average_height</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2</td>
      <td>Chewie</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.explain()
```

    == Physical Plan ==
    *(4) Project [breed_id#1L, id#0L, nickname#2, name#7, average_height#8L]
    +- SortMergeJoin [breed_id#1L], [breed_id#6L], LeftOuter
       :- *(2) Sort [breed_id#1L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(breed_id#1L, 200)
       :     +- *(1) Filter (isnotnull(breed_id#1L) && (breed_id#1L = 1))
       :        +- Scan ExistingRDD[id#0L,breed_id#1L,nickname#2]
       +- *(3) Sort [breed_id#6L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(breed_id#6L, 200)
             +- Scan ExistingRDD[breed_id#6L,name#7,average_height#8L]


**What Happened:**

The column `breed_id` is present in `both` sides of the join, but only the `left` side was `filtered` before the join.

### Case #3: Filter on Both Sides of the Join #2


```python
df = (
    pets
    .join(breeds, 'breed_id')
    .filter(F.col('breed_id') == 1)
)

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>id</th>
      <th>nickname</th>
      <th>name</th>
      <th>average_height</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2</td>
      <td>Chewie</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.explain()
```

    == Physical Plan ==
    *(5) Project [breed_id#1L, id#0L, nickname#2, name#7, average_height#8L]
    +- *(5) SortMergeJoin [breed_id#1L], [breed_id#6L], Inner
       :- *(2) Sort [breed_id#1L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(breed_id#1L, 200)
       :     +- *(1) Filter (isnotnull(breed_id#1L) && (breed_id#1L = 1))
       :        +- Scan ExistingRDD[id#0L,breed_id#1L,nickname#2]
       +- *(4) Sort [breed_id#6L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(breed_id#6L, 200)
             +- *(3) Filter (isnotnull(breed_id#6L) && (breed_id#6L = 1))
                +- Scan ExistingRDD[breed_id#6L,name#7,average_height#8L]


**What Happened:**

The column `breed_id` is present in `both` sides of the join, and spark was able to figure out that it should perform a `filter` on both sides before the join.

### Case #4: Filter on Both Sides of the Join, Filter Beforehand


```python
df = (
    pets
    .join(
        breeds.filter(F.col('breed_id') == 1), 
        'breed_id', 
        'left_outer'
    )
    .filter(F.col('breed_id') == 1)
)

df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>breed_id</th>
      <th>id</th>
      <th>nickname</th>
      <th>name</th>
      <th>average_height</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Bear</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2</td>
      <td>Chewie</td>
      <td>Pitbull</td>
      <td>10</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.explain()
```

    == Physical Plan ==
    *(5) Project [breed_id#1L, id#0L, nickname#2, name#7, average_height#8L]
    +- SortMergeJoin [breed_id#1L], [breed_id#6L], LeftOuter
       :- *(2) Sort [breed_id#1L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(breed_id#1L, 200)
       :     +- *(1) Filter (isnotnull(breed_id#1L) && (breed_id#1L = 1))
       :        +- Scan ExistingRDD[id#0L,breed_id#1L,nickname#2]
       +- *(4) Sort [breed_id#6L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(breed_id#6L, 200)
             +- *(3) Filter (isnotnull(breed_id#6L) && (breed_id#6L = 1))
                +- Scan ExistingRDD[breed_id#6L,name#7,average_height#8L]


**What Happened:**

The column `breed_id` is present in `both` sides of the join, and both sides were `filtered` before the join.

### Summary
* To improve join performance, we should always try to push the `filter` before the joins.
* Spark might be smart enough to figure that the `filter` can be performed on both sides, but not always.
* You should alway check to see if your Spark DAG is performant during a join and if any `filter`s can be pushed before the joins.
