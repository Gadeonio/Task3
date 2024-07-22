from pyspark.sql import SparkSession, Row
from datetime import datetime, date
import pandas as pd

spark = SparkSession.builder.getOrCreate()
products = spark.createDataFrame([
    Row(prodId=0, name='soup'),
    Row(prodId=1, name='rope'),
])

categories = spark.createDataFrame([
    Row(categId=0, name='food'),
    Row(categId=1, name='gym'),
])

product_category = spark.createDataFrame([
    Row(prodId=0, categId=0)
])

product_category.join(products, 'prodId', 'right').join(categories, 'categId', 'left').select(products.name, categories.name).show()
