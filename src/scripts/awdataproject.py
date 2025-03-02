# Databricks notebook source
#  %md
#  ## Silver Layer script


from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import sum, datediff, col, when,year, month, weekofyear, dayofweek,





#Allow us to acess the data from data lake. Using below script used spark.
spark.conf.set("fs.azure.account.auth.type.<storageaccount>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storageaccount>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storageaccount>.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storageaccount>.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storageaccount>.dfs.core.windows.net", "https://login.microsoftonline.com/oauth2/token")


#  %md
#  #### Data Loading


#  %md
#  ##### Read calender data
# 


cal_df = spark.read.format("csv").\
    option("header", "true").\
        option("inferSchema", "true").\
            load('abfss://bronze@<storageaccount>.dfs.core.windows.net/AdventureWorks_Calendar')


customer_df = spark.read.format("csv").\
    option("header", "true").\
        option("inferSchema", "true").\
            load('abfss://bronze@<storageaccount>.dfs.core.windows.net/AdventureWorks_Customers')




product_cat_df = spark.read.format("csv").\
    option("header", "true").\
        option("inferSchema", "true").\
            load('abfss://bronze@<storageaccount>.dfs.core.windows.net/AdventureWorks_Product_Categories')


products_cat_df = spark.read.format("csv").\
    option("header", "true").\
        option("inferSchema", "true").\
            load('abfss://bronze@<storageaccount>.dfs.core.windows.net/AdventureWorks_Products')


returns_df = spark.read.format("csv").\
    option("header", "true").\
        option("inferSchema", "true").\
            load('abfss://bronze@<storageaccount>.dfs.core.windows.net/AdventureWorks_Returns')



sales_df = spark.read.format("csv").\
    option("header", "true").\
        option("inferSchema", "true").\
            load('abfss://bronze@<storageaccount>.dfs.core.windows.net/AdventureWorks_Sales*')


sales_df = spark.read.format("csv").\
    option("header", "true").\
        option("inferSchema", "true").\
            load('abfss://bronze@<storageaccount>.dfs.core.windows.net/AdventureWorks_Sales*')


territory_df = spark.read.format("csv").\
    option("header", "true").\
        option("inferSchema", "true").\
            load('abfss://bronze@<storageaccount>.dfs.core.windows.net/AdventureWorks_Territories')


product_sub_cat_df = spark.read.format("csv").\
    option("header", "true").\
        option("inferSchema", "true").\
            load('abfss://bronze@<storageaccount>.dfs.core.windows.net/Product_Subcategories')



products_df = spark.read.format("csv").\
    option("header", "true").\
        option("inferSchema", "true").\
            load('abfss://bronze@<storageaccount>.dfs.core.windows.net/products')


#  %md
#  #### Data Transformation


#Transforming calender data
#Here we creating two seperate colums Month,Year, Day, Week
transformed_df_cal = cal_df.withColumn("Month", month(col("Date")))\
    .withColumn("Year", year(col("Date")))\
        .withColumn('Day', dayofmonth(col("Date")))\
            .withColumn('Week', weekofyear(col("Date")))


cal_df.write.format('parquet')\
    .mode('append')\
        .option('path', 'abfss://silver@<storageaccount>.dfs.core.windows.net/AdventureWorks_Calendar')\
            .save()


## Customers data transfroamtion
customer_df.display()


customer_df = customer_df.withColumn('FullName', concat(col('Prefix'), lit(' '), col('FirstName'), lit(' '), col('LastName')))\
    .withColumn("Year", year(col("BirthDate")))\
        .withColumn("Month", month(col("BirthDate")))\
            .withColumn("Day", dayofmonth(col("BirthDate")))\
                .withColumn("Week", weekofyear(col("BirthDate")))\
    .withColumn("weekday", weekday(col("BirthDate")))



customer_df.display()


customer_df.write.format('parquet')\
    .mode('append')\
        .option('path', 'abfss://silver@<storageaccount>.dfs.core.windows.net/AdventureWorks_Customers')\
            .save()


product_cat_df.write.format('parquet')\
    .mode('append')\
        .option('path', 'abfss://silver@<storageaccount>.dfs.core.windows.net/AdventureWorks_Product_Categories')\
            .save()


returns_df.write.format('parquet')\
    .mode('append')\
        .option('path', 'abfss://silver@<storageaccount>.dfs.core.windows.net/AdventureWorks_Returns')\
            .save()


product_sub_cat_df.write.format('parquet')\
    .mode('append')\
        .option('path', 'abfss://silver@<storageaccount>.dfs.core.windows.net/Product_Subcategories')\
            .save()


products_df.display()


products_df = products_df.withColumn("ProductSKU", split(col("ProductSKU"), "-")[0])\
    .withColumn('ProductName', split(col("ProductName"), "-")[0])\
        .withColumn('ModelName', trim(col("ModelName")))



products_df.display()


products_df.write.format('parquet')\
    .mode('append')\
        .option('path', 'abfss://silver@<storageaccount>.dfs.core.windows.net/products')\
            .save()


territory_df.display()


territory_df.write.format('parquet')\
    .mode('append')\
        .option('path', 'abfss://silver@<storageaccount>.dfs.core.windows.net/AdventureWorks_Territories')\
            .save()


sales_df.display()



# Adding a new column 'DaysBetween' which is the difference between OrderDate and StockDate
df_with_date_diff = sales_df.withColumn(
    'DaysBetween', 
    datediff(col('OrderDate'), col('StockDate'))
)

# Show the result



from pyspark.sql.functions import sum

# Aggregate by OrderNumber to get the total OrderQuantity for each order
aggregated_df = sales_df.groupBy('OrderNumber').agg(
    sum('OrderQuantity').alias('TotalOrderQuantity')
)



aggregated_df.write.format('parquet')\
    .mode('append')\
        .option('path', 'abfss://silver@<storageaccount>.dfs.core.windows.net/AdventureWorks_Sales')\
            .save()


aggregated_df.display()


from pyspark.sql.functions import when

# Categorize OrderQuantity into High, Medium, or Low categories
df_with_category = sales_df.withColumn(
    'OrderCategory',
    when(sales_df['OrderQuantity'] > 2, 'High')
    .when(sales_df['OrderQuantity'] == 2, 'Medium')
    .otherwise('Low')
)



df_with_category.write.format('parquet')\
    .mode('append')\
        .option('path', 'abfss://silver@<storageaccount>.dfs.core.windows.net/AdventureWorks_Sales')\
            .save()


df_with_category.display()


from pyspark.sql.functions import year, month

# Extract year and month from OrderDate
df_with_year_month = sales_df.withColumn(
    'Year', year(col('OrderDate'))
).withColumn(
    'Month', month(col('OrderDate'))
)



from pyspark.sql.functions import to_date

# Convert string to date type if needed
df = sales_df.withColumn('OrderDate', to_date(col('OrderDate'), 'yyyy-MM-dd'))

# Filter for orders after a certain date
filtered_date_df = sales_df.filter(sales_df['OrderDate'] > '2016-01-01')



from pyspark.sql.functions import year, month, weekofyear, dayofweek

# Extract Year, Month, Week, and Day
df_with_time = sales_df.withColumn('Year', year(sales_df['OrderDate'])) \
    .withColumn('Month', month(sales_df['OrderDate'])) \
    .withColumn('Week', weekofyear(sales_df['OrderDate'])) \
    .withColumn('Day', dayofweek(sales_df['OrderDate']))  # Returns 1 (Sunday) to 7 (Saturday)



# Group by Year and Month and count the number of orders
orders_per_month = df_with_time.groupBy('Year', 'Month').count().orderBy('Year', 'Month')

orders_per_month.display()



# Group by Year and Week and count the number of orders
orders_per_week = df_with_time.groupBy('Year', 'Week').count().orderBy('Year', 'Week')

orders_per_week.display()



# Group by Year and Day and count the number of orders
orders_per_day = df_with_time.groupBy('Year', 'Month', 'Day').count().orderBy('Year', 'Month', 'Day')

orders_per_day.display()
