# Databricks notebook source
# MAGIC %md
# MAGIC # Workato Lookup Table作成
# MAGIC - DP案件の83対象テーブルのLookupテーブルをcsvファイルとして作成する
# MAGIC <table>
# MAGIC   <tr><th>作者</th><th>タイン</th></tr>
# MAGIC   <tr><td>開始日付</td><td>2023/04/25</td></tr>
# MAGIC   <tr><td>終了日付</td><td>2023/04/</td></tr>
# MAGIC   <tr><td>cluster</td><td>dx-dev-small</td></tr>
# MAGIC </table>

# COMMAND ----------

s3_path = "s3://adhoc-dev-lake-434402754152/thanh/rebbon_dp/Workato - raw lookup table.csv"
raw_lookup_table_sdf = (
    spark
    .read
    .format("csv")
    .option("inferschema",True)
    .option("header",True)
    .load(s3_path)
)
display(raw_lookup_table_sdf)
raw_lookup_table_sdf.count()

# COMMAND ----------

from pyspark.sql.functions import when, col

raw_lookup_table_sdf = raw_lookup_table_sdf.na.fill("DUMMY", subset=["DATE COLUMN NAME"])

display(raw_lookup_table_sdf)

# COMMAND ----------

lookup_table_sdf = raw_lookup_table_sdf.withColumn("FOLDER NAME", when(col("DATE COLUMN NAME") == "DEDT_YMDTIM", "diff_merge").otherwise("all_merge"))
display(lookup_table_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC # END
