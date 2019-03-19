
### Library Imports


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from decimal import Decimal
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
sales_df = spark.createDataFrame(
    [
        (1, 1, 1, "order", Decimal('1')),
        (2, 1, 2, "refund", Decimal('1')),
        (3, 2, None, "shipping", Decimal('1')),
    ], ['id', 'shop_id', "order_id", "type", "amount"]
)

sales_df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>shop_id</th>
      <th>order_id</th>
      <th>type</th>
      <th>amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>1.0</td>
      <td>order</td>
      <td>1.000000000000000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>2.0</td>
      <td>refund</td>
      <td>1.000000000000000000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2</td>
      <td>NaN</td>
      <td>shipping</td>
      <td>1.000000000000000000</td>
    </tr>
  </tbody>
</table>
</div>




```python
orders_df = spark.createDataFrame(
    [
        (1, 1, Decimal("1.00"), Decimal("1.13")), 
        (2, 1, Decimal("2.00"), Decimal("1.13")), 
        (3, 2, Decimal("3.00"), Decimal("1.13")), 
    ], ['order_id', 'shop_id', "price", "tax"]
)

orders_df.toPandas()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>shop_id</th>
      <th>price</th>
      <th>tax</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>1.000000000000000000</td>
      <td>1.130000000000000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>1</td>
      <td>2.000000000000000000</td>
      <td>1.130000000000000000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2</td>
      <td>3.000000000000000000</td>
      <td>1.130000000000000000</td>
    </tr>
  </tbody>
</table>
</div>




```python
sales_df \
    .join(orders_df, 'order_id') \
    .withColumn('total_price', F.col("amount") * F.col("tax")) \
    .select('order_id', 'total_price', 'amount', 'tax') \
    .explain()
```

    == Physical Plan ==
    *(5) Project [order_id#26L, CheckOverflow((promote_precision(amount#28) * promote_precision(tax#37)), DecimalType(38,6)) AS total_price#68, amount#28, tax#37]
    +- *(5) SortMergeJoin [order_id#26L], [order_id#34L], Inner
       :- *(2) Sort [order_id#26L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(order_id#26L, 200)
       :     +- *(1) Project [order_id#26L, amount#28]
       :        +- *(1) Filter isnotnull(order_id#26L)
       :           +- Scan ExistingRDD[id#24L,shop_id#25L,order_id#26L,type#27,amount#28]
       +- *(4) Sort [order_id#34L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(order_id#34L, 200)
             +- *(3) Project [order_id#34L, tax#37]
                +- *(3) Filter isnotnull(order_id#34L)
                   +- Scan ExistingRDD[order_id#34L,shop_id#35L,price#36,tax#37]

