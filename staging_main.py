from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql.functions import col, when, lit, current_timestamp, count, isnull
spark = SparkSession.builder.master("yarn-client").enableHiveSupport() \
                    .appName('Dissertation-Covid-Staging') \
                    .getOrCreate()
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
'''Set loglevel to WARN to avoid logs flooding on the console'''
spark.sparkContext.setLogLevel("WARN")

'''Declaring schemas to read the source hdfs files, this makes the dataframes going forward to be more tight with the schema and proper structured one'''
ethnicity_schema = StructType([StructField("Measure", StringType(), True)\
                   ,StructField("Time", StringType(), True)\
                   ,StructField("Ethnicity", StringType(), True)\
                   ,StructField("Ethnicity_type", StringType(), True)\
                   ,StructField("Geography_name", StringType(), True)\
                   ,StructField("Geography_code", StringType(), True)\
                   ,StructField("Geography_type", StringType(), True)\
                   ,StructField("Value", FloatType(), True)\
                   ,StructField("Numerator", StringType(), True)\
                   ,StructField("Denominator", StringType(), True) ])

education_schema = StructType([StructField("Region", StringType(), True)\
                   ,StructField("No_Qual", FloatType(), True)\
                   ,StructField("NQF_Level2_Above", FloatType(), True)\
                   ,StructField("NQF_Level3_Above", FloatType(), True)\
                   ,StructField("NQF_Level4_Above", FloatType(), True) ])

income_schema = StructType([StructField("Year", StringType(), True)\
                   ,StructField("County", StringType(), True)\
                   ,StructField("Agg_Total_Wealth", StringType(), True) ])


cases_age_region_schema = StructType([StructField("Area_Code", StringType(), True)\
                   ,StructField("Area_Name", StringType(), True)\
                   ,StructField("Area_Type", StringType(), True)\
                   ,StructField("Date", DateType(), True)\
                   ,StructField("Age", StringType(), True)\
                   ,StructField("cases", IntegerType(), True)\
                   ,StructField("rollingSum", IntegerType(), True)\
                   ,StructField("rollingRate", StringType(), True) ])

vaccination_schema = StructType([StructField("Area_Code", StringType(), True)\
                   ,StructField("Area_Name", StringType(), True)\
                   ,StructField("Area_Type", StringType(), True)\
                   ,StructField("Date", DateType(), True)\
                   ,StructField("numberOfVaccines", IntegerType(), True) ])

cases_gender_schema = StructType([StructField("Area_Code", StringType(), True)\
                   ,StructField("Area_Name", StringType(), True)\
                   ,StructField("Area_Type", StringType(), True)\
                   ,StructField("Date", DateType(), True)\
                   ,StructField("Age", StringType(), True)\
                   ,StructField("Rate", StringType(), True)\
                   ,StructField("Value", IntegerType(), True) ])

ethnicity_stg = spark.read.csv("/dissertation_data/input/static/ethnicity", schema=ethnicity_schema)
education_stg = spark.read.csv("/dissertation_data/input/static/education", schema=education_schema)
income_stg = spark.read.csv("/dissertation_data/input/static/income", schema=income_schema)
lacode_stg = spark.read.csv("/dissertation_data/input/static/la_code", sep='~', header=True)
# cases_age_region_stg = spark.read.csv("/dissertation_data/input/dynamic/covid/infections/england/age", schema=cases_age_region_schema)
vaccin_first_stg = spark.read.csv("/dissertation_data/input/dynamic/covid/vaccinations/england/first_vac", schema=vaccination_schema)
vaccin_second_stg = spark.read.csv("/dissertation_data/input/dynamic/covid/vaccinations/england/second_vac", schema=vaccination_schema)
male_cases_stg = spark.read.csv("/dissertation_data/input/dynamic/covid/infections/england/male_case", schema=cases_gender_schema)
female_cases_stg = spark.read.csv("/dissertation_data/input/dynamic/covid/infections/england/female_case", schema=cases_gender_schema)

'''Cleaning, filtering, removal, manipulation and synthesis of columns from source datasets'''


'''Filtering only major ethnicities'''
major_ethnicities = ["Asian", "Black", "Mixed", "White", "Other"]
eth = ethnicity_stg.filter((col("Measure") == "% of local population in this ethnic group") & (col("Ethnicity_type") == "ONS 2011 5+1") & col("Ethnicity").isin(major_ethnicities)).select("Ethnicity","Geography_name","Geography_code","Value")

'''Manipulating Values of region column'''
edu = education_stg.withColumn("Region", when(col("Region").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("Region"))).select("Region", "No_Qual", "NQF_Level2_Above", "NQF_Level3_Above", "NQF_Level4_Above")

la_codes = lacode_stg.withColumn("RGNNM", when(col("RGNNM") == "East of England", "East")
                                 .when(col("RGNNM").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("RGNNM"))).select("UTLACD", "UTLANM", "RGNCD", "RGNNM", "CTRYCD", "CTRYNM")

# cases_by_age_region = cases_age_region_stg.withColumn("Area_Name", when(col("Area_Name").contains("Yorkshire"), "Yorkshire and Humber")
#                                  .otherwise(col("Area_Name"))).withColumn("Age", when(col("Age") == "00_04", "0_to_4").when(col("Age") == "05_09", "5_to_9")
#                                  .otherwise(regexp_replace(col("Age"), "_", "_to_"))).filter((col("Age") != "unassigned") & (col("Age") != "60+") & (col("Age") != "00_to_59")).select("Area_Code","Area_Name","Area_Type","Date","Age","cases")

'''Removing unwanted columns. Keeps only Area_Name, Area_Code, Area_Type, Date & numberofVaccines'''
vaccin_first = vaccin_first_stg.withColumn("Area_Name", when(col("Area_Name").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("Area_Name"))).select("Area_Code","Area_Name","Area_Type","Date","numberOfVaccines")

vaccin_second = vaccin_second_stg.withColumn("Area_Name", when(col("Area_Name").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("Area_Name"))).select("Area_Code","Area_Name","Area_Type","Date","numberOfVaccines")

'''Removing unwanted columns. Keeps only Area_Name, Area_Code, Area_Type, Date, Age & Value'''
male_cases = male_cases_stg.withColumn("Area_Name", when(col("Area_Name").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("Area_Name"))).select("Area_Code","Area_Name","Area_Type","Date","Age","Value")

female_cases = female_cases_stg.withColumn("Area_Name", when(col("Area_Name").contains("Yorkshire"), "Yorkshire and Humber")
                                 .otherwise(col("Area_Name"))).select("Area_Code","Area_Name","Area_Type","Date","Age","Value")                               

'''Joining to create unified dataframe'''
vaccin_both = vaccin_first.join(vaccin_second, (vaccin_first["Area_Code"] == vaccin_second["Area_Code"]) & (vaccin_first["Date"] == vaccin_second["Date"]), "inner").select(vaccin_first["Area_Code"], vaccin_first["Area_Name"], vaccin_first["Area_Type"], vaccin_first["Date"], vaccin_first["numberOfVaccines"].alias("firstDose"), vaccin_second["numberOfVaccines"].alias("secondDose"))

cases_both = male_cases.join(female_cases, (male_cases["Area_Code"] == female_cases["Area_Code"]) & (male_cases["Date"] == female_cases["Date"]) & (male_cases["Age"] == female_cases["Age"]), "inner").select(male_cases["Area_Code"], male_cases["Area_Name"], male_cases["Area_Type"], male_cases["Date"], male_cases["Age"], male_cases["Value"].alias("maleCases"), female_cases["Value"].alias("femaleCases"))
cases_both.registerTempTable("cases_both")

'''Synthesizing actual male and female cases from cumulative values'''
cases_actual_calc = spark.sql(" SELECT Area_Code, Area_Name, Area_Type, Date, Age, maleCases, femaleCases, CASE WHEN m_l is null then 0 else m_l end as m_l, CASE WHEN f_l is null then 0 else f_l end as f_l FROM \
    (SELECT *, lead(malecases) over (partition by Area_Code, Age order by Date desc) as m_l, lead(femalecases) over (partition by Area_Code, Age order by Date desc) as f_l from cases_both) BASE")
cases_actual_calc.registerTempTable("cases_actual_calc")
cases_both_actual = spark.sql("SELECT Area_Code, Area_Name, Area_Type, Date, case when Age='90' then '90+' else Age end as Age, malecases-m_l as actualmalecases, femalecases-f_l as actualfemalecases from cases_actual_calc")

'''Filtering only England country codes'''
la_codes.registerTempTable("la_codes")
country_codes = spark.sql("SELECT DISTINCT CTRYCD, CTRYNM FROM la_codes")

england_region_codes_all = la_codes.filter(col("CTRYNM") == "England")
england_region_codes_all.registerTempTable("england_codes_all")

yorkshire_region_code = spark.sql("SELECT DISTINCT UTLACD, UTLANM FROM england_codes_all WHERE RGNNM = 'Yorkshire and Humber'")

, deaths_utla_stg[

eth.write.insertInto("staging.ethnicity", overwrite=True)
edu.write.insertInto("staging.education", overwrite=True)
income_stg.write.insertInto("staging.income", overwrite=True)
cases_both_actual.write.insertInto("staging.cov_cases_reg_age_gen", overwrite=True)
vaccin_both.write.insertInto("staging.cov_vaccinations", overwrite=True)
country_codes.write.insertInto("staging.country_codes", overwrite=True)
england_region_codes_all.write.insertInto("staging.england_codes", overwrite=True)
yorkshire_region_code.write.insertInto("staging.yorkshire_codes", overwrite=True)

'''Displaying the schema of dataframes to the console log for verification'''
print("Ethnicity dataset schema")
eth.printSchema()

print("Education dataset schema")
edu.printSchema()

print("Income dataset schema")
income_stg.printSchema()

print("Covid Cases dataset schema")
cases_both_actual.printSchema()

print("Vaccination dataset schema")
vaccin_both.printSchema()

print("Country codes dataset schema")
country_codes.printSchema()

'''Logging Data Validity metrics to audit table'''

ethnicity_stg.registerTempTable("ethnicity_stg")
education_stg.registerTempTable("education_stg")
income_stg.registerTempTable("income_stg")
lacode_stg.registerTempTable("lacode_stg")
# cases_age_region_stg.registerTempTable("cases_age_region_stg")
vaccin_first_stg.registerTempTable("vaccin_first_stg")
vaccin_second_stg.registerTempTable("vaccin_second_stg")
male_cases_stg.registerTempTable("male_cases_stg")
female_cases_stg.registerTempTable("female_cases_stg")

source_counts = spark.sql(" SELECT 'SOURCING' AS LAYER, 'HDFS FILES' AS TYPE, 'Ethnicity' AS NAME, COUNT(*) as RECORDS_COUNT FROM ethnicity_stg GROUP BY 'SOURCING', 'HDFS FILES', 'Ethnicity' \
UNION ALL \
SELECT 'SOURCING' AS LAYER, 'HDFS FILES' AS TYPE, 'Education' AS NAME, COUNT(*) as cnt FROM education_stg GROUP BY 'SOURCING', 'HDFS FILES', 'Education' \
UNION ALL \
SELECT 'SOURCING' AS LAYER, 'HDFS FILES' AS TYPE, 'Income' AS NAME, COUNT(*) as cnt FROM income_stg GROUP BY 'SOURCING', 'HDFS FILES', 'Income' \
UNION ALL \
SELECT 'SOURCING' AS LAYER, 'HDFS FILES' AS TYPE, 'LA_Codes_All' AS NAME, COUNT(*) as cnt FROM lacode_stg GROUP BY 'SOURCING', 'HDFS FILES', 'LA_Codes_All' \
UNION ALL \
SELECT 'SOURCING' AS LAYER, 'HDFS FILES' AS TYPE, 'Both_Vaccines' AS NAME, COUNT(*) as cnt FROM vaccin_first_stg GROUP BY 'SOURCING', 'HDFS FILES', 'Both_Vaccines' \
UNION ALL \
SELECT 'SOURCING' AS LAYER, 'HDFS FILES' AS TYPE, 'Both_Cases' AS NAME, COUNT(*) as cnt FROM male_cases_stg GROUP BY 'SOURCING', 'HDFS FILES', 'Both_Cases'  ")

stg_counts = spark.sql(" SELECT 'STAGING' AS LAYER, 'HIVE TABLE' AS TYPE, 'Ethnicity' AS NAME, COUNT(*) as RECORDS_COUNT FROM staging.ethnicity GROUP BY 'STAGING', 'HIVE TABLE', 'Ethnicity' \
UNION ALL \
SELECT 'STAGING' AS LAYER, 'HIVE TABLE' AS TYPE, 'Education' AS NAME, COUNT(*) as cnt FROM staging.education GROUP BY 'STAGING', 'HIVE TABLE', 'Education' \
UNION ALL \
SELECT 'STAGING' AS LAYER, 'HIVE TABLE' AS TYPE, 'Income' AS NAME, COUNT(*) as cnt FROM staging.income GROUP BY 'STAGING', 'HIVE TABLE', 'Income' \
UNION ALL \
SELECT 'STAGING' AS LAYER, 'HIVE TABLE' AS TYPE, 'LA_Codes_England' AS NAME, COUNT(*) as cnt FROM staging.england_codes GROUP BY 'STAGING', 'HIVE TABLE', 'LA_Codes_England' \
UNION ALL \
SELECT 'STAGING' AS LAYER, 'HIVE TABLE' AS TYPE, 'Both_Vaccines' AS NAME, COUNT(*) as cnt FROM staging.cov_vaccinations GROUP BY 'STAGING', 'HIVE TABLE', 'Both_Vaccines' \
UNION ALL \
SELECT 'STAGING' AS LAYER, 'HIVE TABLE' AS TYPE, 'Both_Cases' AS NAME, COUNT(*) as cnt FROM staging.cov_cases_reg_age_gen GROUP BY 'STAGING', 'HIVE TABLE', 'Both_Cases' ")

total_counts = source_counts.union(stg_counts)
total_counts_inter = total_counts.withColumn("LOAD_DATETIME", lit(current_timestamp())).withColumn("BATCH_ID", lit(0))
total_counts_inter.registerTempTable("total_counts_inter")

total_counts_final = spark.sql("SELECT DISTINCT CASE WHEN BD.MAX_BATCH_ID > INTER.BATCH_ID THEN BD.MAX_BATCH_ID+1 ELSE 1 END AS BATCH_ID, INTER.LAYER, INTER.TYPE, INTER.NAME, INTER.RECORDS_COUNT, INTER.LOAD_DATETIME \
    FROM total_counts_inter INTER INNER JOIN (SELECT CASE WHEN MAX(BATCH_ID) IS NULL THEN 0 ELSE MAX(batch_id) END AS MAX_BATCH_ID, 0 AS DUMMY FROM staging.DATA_VALIDITY_AUDIT) BD ON INTER.BATCH_ID=BD.DUMMY ")
total_counts_final.write.insertInto("staging.DATA_VALIDITY_AUDIT", overwrite=False)

'''Data Quality Checks'''
'''Duplicates Check on all fact tables and log the results to Data Qulaity Audit table'''

vaccin_both_dup = vaccin_both.groupBy("Area_Code","Area_Name","Area_Type", "Date", "firstDose", "secondDose").count().filter("count > 1")
vaccin_both_dup.registerTempTable("vaccin_both_dup")
cases_both_actual_dup= cases_both_actual.groupBy("Area_Code","Area_Name","Area_Type", "Date", "Age", "actualmalecases", "actualfemalecases").count().filter("count > 1")
cases_both_actual_dup.registerTempTable("cases_both_actual_dup")

dq_duplicate = spark.sql("SELECT CASE WHEN COUNT=0 THEN 'Passed' ELSE 'Failed' END AS RESULT, 'cov_vaccinations' AS TABLE_NAME, 'staging' AS DATABASE_NAME, 'Duplicate Check' as DQ_TYPE FROM (SELECT COUNT(*) AS COUNT FROM vaccin_both_dup) VACCINES \
UNION ALL \
SELECT CASE WHEN COUNT=0 THEN 'Passed' ELSE 'Failed' END AS RESULT, 'cov_cases_reg_age_gen' AS TABLE_NAME, 'staging' AS DATABASE_NAME, 'Duplicate Check' as DQ_TYPE FROM (SELECT COUNT(*) AS COUNT FROM cases_both_actual_dup) CASES")
dq_duplicate_inter = dq_duplicate.withColumn("LOAD_DATETIME", lit(current_timestamp())).withColumn("BATCH_ID", lit(0))

dq_duplicate_inter.registerTempTable("dq_duplicate_inter")
dq_duplicate_final = spark.sql("SELECT DISTINCT CASE WHEN BD.MAX_BATCH_ID > INTER.BATCH_ID THEN BD.MAX_BATCH_ID+1 ELSE 1 END AS BATCH_ID, INTER.DATABASE_NAME, INTER.TABLE_NAME, INTER.DQ_TYPE, INTER.RESULT, INTER.LOAD_DATETIME \
    FROM dq_duplicate_inter INTER INNER JOIN (SELECT CASE WHEN MAX(BATCH_ID) IS NULL THEN 0 ELSE MAX(batch_id) END AS MAX_BATCH_ID, 0 AS DUMMY FROM staging.DATA_QUALITY_AUDIT) BD ON INTER.BATCH_ID=BD.DUMMY ")
dq_duplicate_final.write.insertInto("staging.DATA_QUALITY_AUDIT", overwrite=False)

'''Null Check on all fact tables and log the results to Data Qulaity Audit table'''
'''User defined function to calculate null values in all columns of a dataframe'''

def test(input, name):
    df_agg = reduce(lambda a, b: a.union(b), (input.agg(count(when(isnull(c), c)).alias('Count')).select(lit(name).alias("Table"), lit(c).alias("Column"), "Count") for c in input.columns))
    return df_agg

cov_cases_null = test(cases_both_actual, "cov_cases_reg_age_gen")
cov_cases_null.registerTempTable("cov_cases_null")
cov_cases_null_res = spark.sql("SELECT CASE WHEN TOTAL_NULLS=0 THEN 'Passed' ELSE 'Failed' END AS RESULT, 'cov_cases_reg_age_gen' AS TABLE_NAME, 'staging' AS DATABASE_NAME, 'Null Check' as DQ_TYPE \
    FROM (SELECT TABLE, SUM(COUNT) AS TOTAL_NULLS FROM cov_cases_null GROUP BY TABLE) CASES")

cov_vaccinations_null = test(vaccin_both, "cov_vaccinations")
cov_vaccinations_null.registerTempTable("cov_vaccinations_null")
cov_vaccinations_null_res = spark.sql("SELECT CASE WHEN TOTAL_NULLS=0 THEN 'Passed' ELSE 'Failed' END AS RESULT, 'cov_vaccinations' AS TABLE_NAME, 'staging' AS DATABASE_NAME, 'Null Check' as DQ_TYPE \
    FROM (SELECT TABLE, SUM(COUNT) AS TOTAL_NULLS FROM cov_vaccinations_null GROUP BY TABLE) CASES")

dq_null = cov_cases_null_res.union(cov_vaccinations_null_res)
dq_null_inter = dq_null.withColumn("LOAD_DATETIME", lit(current_timestamp())).withColumn("BATCH_ID", lit(0))

dq_null_inter.registerTempTable("dq_null_inter")
dq_null_final = spark.sql("SELECT DISTINCT CASE WHEN BD.MAX_BATCH_ID > INTER.BATCH_ID THEN BD.MAX_BATCH_ID+1 ELSE 1 END AS BATCH_ID, INTER.DATABASE_NAME, INTER.TABLE_NAME, INTER.DQ_TYPE, INTER.RESULT, INTER.LOAD_DATETIME \
    FROM dq_null_inter INTER INNER JOIN (SELECT CASE WHEN MAX(BATCH_ID) IS NULL THEN 0 ELSE MAX(batch_id) END AS MAX_BATCH_ID, 0 AS DUMMY FROM staging.DATA_QUALITY_AUDIT WHERE DQ_TYPE='Null Check') BD ON INTER.BATCH_ID=BD.DUMMY ")
dq_null_final.write.insertInto("staging.DATA_QUALITY_AUDIT", overwrite=False)
