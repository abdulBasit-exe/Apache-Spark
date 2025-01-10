# Databricks notebook source
df=spark.read.format("csv").load("dbfs:/FileStore/tables/billing_dataset.csv", header="true", inferSchema="true")
df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("billing_dataset")

# COMMAND ----------

df  = spark.sql("Select * from billing_dataset limit 20")

# COMMAND ----------

df.show()

# COMMAND ----------

df.count()

# COMMAND ----------

df_sql = spark.sql("""Select country c, category ct, sum(billedamount) as ba
                    from billing_dataset
                    group by c, ct
                    order by c;
                   """)

# COMMAND ----------

df_sql.show()

# COMMAND ----------

df_sql.count()

# COMMAND ----------

df.usa = spark.sql("select * \
    from billing_dataset\
        where country = 'Azerbaijan'")
df.usa.show()

# COMMAND ----------

