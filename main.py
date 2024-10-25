from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName("ProductsAndCategories").getOrCreate()

products = [
    (1, "Product A"),
    (2, "Product B"),
    (3, "Product C")
]

categories = [
    (1, "Category1"),
    (2, "Category2"),
]

product_category = [
    (1, 1),
    (1, 2),
    (2, 1)
]

products_df = spark.createDataFrame(products, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories, ["category_id", "category_name"])
product_category_df = spark.createDataFrame(product_category, ["product_id", "category_id"])

products_df.show()
categories_df.show()

result = products_df.join(product_category_df, "product_id", "left") \
                                    .join(categories_df, "category_id", "left") \
                                    .select("product_name", "category_name")

result.show()



