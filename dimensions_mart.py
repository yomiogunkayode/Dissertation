from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import col, when, regexp_replace
spark = SparkSession.builder.master("yarn-client").enableHiveSupport() \
                    .appName('Dissertation-Covid-Mart-Dimensions') \
                    .getOrCreate()

'''Set loglevel to WARN to avoid logs flooding on the console'''
spark.sparkContext.setLogLevel("WARN")


'''User defined function to populate date dimension with 'date' as unique column'''
def create_date_table(start='2010-01-01', end='2050-12-31'):
   df = pd.DataFrame({"date": pd.date_range(start, end)})
   df["day"] = df.date.dt.day
   df["month"] = df.date.dt.month
   df["quarter"] = df.date.dt.quarter
   df["year"] = df.date.dt.year
   return df



dim_date_temp = spark.createDataFrame(create_date_table())
dim_date_temp.registerTempTable("dim_date_temp")
dim_date = spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY date) as DIM_DATE_ID, date as dt, day, month, quarter, year from dim_date_temp")


'''User defined list to populate age group classification'''
dim_age_list = [(1, "0_to_4"),(2, "5_to_9"),(3, "10_to_14"),(4, "15_to_19"),(5, "20_to_24"),(6, "25_to_29"),(7, "30_to_34"),(8, "35_to_39"),(9, "40_to_44"),(10, "45_to_49"),(11, "50_to_54"),(12, "55_to_59"), (13, "60_to_64"), (14, "65_to_69"), (15, "70_to_74"), (16, "75_to_79"), (17, "80_to_84"), (18, "85_to_89"), (19, "90+")]
dim_age_schema = StructType([ \
    StructField("DIM_AGEGRP_ID",IntegerType(),True), \
    StructField("age_group",StringType(),True)])
dim_age = spark.createDataFrame(data=dim_age_list, schema=dim_age_schema)


# infection_type_schema = StructType([ \
#     StructField("id",IntegerType(),True), \
#     StructField("type",StringType(),True),\
#     StructField("name",StringType(),True) ])
# dim_infectiontype = spark.createDataFrame(data=infection_type_list, schema=infection_type_schema)

# dim_reg_education = spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY Region) as id, Region from staging.education")
# dim_reg_education.registerTempTable("dim_reg_education")

# gender_list = [(1, "Male"), (2, "Female")]
# gender_schema = StructType([ \
#     StructField("id",IntegerType(),True), \
#     StructField("name",StringType(),True) ])
# dim_gender = spark.createDataFrame(data=gender_list, schema=gender_schema)

# dist_eng_codes = spark.sql("SELECT DISTINCT UTLACD, UTLANM, RGNCD, RGNNM FROM staging.england_codes")
# dist_eng_codes.registerTempTable("dist_eng_codes")

# edf1 = spark.sql("SELECT DISTINCT codes.RGNCD, codes.RGNNM FROM staging.ethnicity ethn JOIN dist_eng_codes codes on ethn.GEO_CODE = codes.UTLACD")
# edf1.registerTempTable("edf1")
# dim_reg_ethnicity = spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY RGNCD) as id, RGNCD, RGNNM from edf1")

loc_lvl1 = spark.sql("SELECT DISTINCT CTRYNM as NAME, 'Country' as TYPE, CTRYCD as CODE FROM staging.country_codes")
loc_lvl2 = spark.sql("SELECT DISTINCT codes.RGNNM as NAME, 'Region' as TYPE, codes.RGNCD as CODE  FROM staging.england_codes codes")
loc_lvl3 = spark.sql("SELECT DISTINCT UTLANM as NAME, 'Area' as TYPE, UTLACD as CODE FROM staging.yorkshire_codes")

'''Union all three different codes and types (Country, Region, Area)'''
loc_temp = loc_lvl1.union(loc_lvl2.union(loc_lvl3))
loc_temp.registerTempTable("loc_temp")
dim_location = spark.sql("SELECT ROW_NUMBER() OVER (ORDER BY CODE DESC) as DIM_LOCATION_ID, name, type, code from loc_temp")

'''Displaying the schema of dataframes to the console log for verification. However, since these are Hive tables - the metadata will be intact'''
print("Location Dimesion schema")
dim_location.printSchema()

print("Date Dimension schema")
dim_date.printSchema()

print("Age Group Dimension schema")
dim_age.printSchema()

dim_location.write.option("delimiter", "~").insertInto("mart.DIM_LOCATION", overwrite=True)
dim_date.write.insertInto("mart.DIM_DATE", overwrite=True)
dim_age.write.insertInto("mart.DIM_AGEGRP", overwrite=True)
# dim_infectiontype.write.insertInto("mart.DIM_INFECTIONTYPE", overwrite=True)
# dim_reg_education.write.insertInto("mart.DIM_REG_EDUCATION", overwrite=True)
# dim_gender.write.insertInto("mart.DIM_GENDER", overwrite=True)
# dim_reg_ethnicity.write.insertInto("mart.DIM_REG_ETHNICITY", overwrite=True)
