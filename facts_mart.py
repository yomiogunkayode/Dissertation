from ctypes import Union
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql.functions import  when, lit, current_timestamp, count, isnull
spark = SparkSession.builder.master("yarn-client").enableHiveSupport() \
                    .appName('Dissertation-Covid-Mart-Facts') \
                    .getOrCreate()

spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
'''Set loglevel to WARN to avoid logs flooding on the console'''
spark.sparkContext.setLogLevel("WARN")


deaths_nation = spark.sql("select dt.DIM_DATE_ID, dl.DIM_LOCATION_ID, stg_deaths.newDeaths, 0 as firstDose, 0 as secondDose from mart.dim_date dt join staging.cov_deaths_nation stg_deaths on dt.dt = stg_deaths.reported_date join mart.DIM_LOCATION dl on dl.code = stg_deaths.areacode ")

deaths_yorkshire = spark.sql("select dt.DIM_DATE_ID, dl.DIM_LOCATION_ID, stg_deaths.newDeaths, 0 as firstDose, 0 as secondDose from mart.dim_date dt join staging.cov_deaths_yorkshire stg_deaths on dt.dt = stg_deaths.reported_date join mart.DIM_LOCATION dl on dl.code = stg_deaths.areacode ")

total_deaths = deaths_nation.union(deaths_yorkshire)
total_deaths.registerTempTable("total_deaths")
# fact_deaths= spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY dim_date_id, dim_location_id) as FACT_COVID_DEATHS_ID, dim_date_id, dim_location_id, newdeaths from total_deaths")
# fact_deaths.write.insertInto("mart.FACT_COVID_DEATHS", overwrite=True)

doses_nation = spark.sql("select dt.DIM_DATE_ID, dl.DIM_LOCATION_ID, 0 as newDeaths, stg_doses.firstDose, stg_doses.secondDose from mart.dim_date dt join staging.cov_vaccinations stg_doses on dt.dt = stg_doses.reported_date join mart.DIM_LOCATION dl on dl.code = stg_doses.areacode")
doses_nation.registerTempTable("doses_nation")
# fact_vaccines= spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY dim_date_id, dim_location_id) as FACT_COVID_VACCINES_ID, dim_date_id, dim_location_id, firstDose, secondDose from doses_nation")
# fact_vaccines.write.insertInto("mart.FACT_COVID_VACCINES", overwrite=True)

# deaths_and_vaccines = spark.sql("select deaths.dim_date_id, deaths.dim_location_id, deaths.newDeaths, doses.firstDose, doses.secondDose from total_deaths deaths join doses_nation doses on deaths.dim_date_id = doses.dim_date_id and deaths.dim_location_id = doses.dim_location_id")
deaths_and_vaccines = total_deaths.union(doses_nation)
deaths_and_vaccines.registerTempTable("deaths_and_vaccines")
fact_deaths_and_vaccines= spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY dim_date_id, dim_location_id) as FACT_COV_DEATHS_AND_VACCINES_ID, dim_date_id, dim_location_id, newDeaths, firstDose, secondDose from deaths_and_vaccines")
fact_deaths_and_vaccines.write.insertInto("mart.FACT_COV_DEATHS_AND_VACCINES", overwrite=True)

cases_nation = spark.sql("select dt.DIM_DATE_ID, dl.DIM_LOCATION_ID, da.DIM_AGEGRP_ID, stg_cases.malecases, stg_cases.femalecases from mart.dim_date dt join staging.cov_cases_reg_age_gen stg_cases on dt.dt = stg_cases.reported_date join mart.DIM_LOCATION dl on dl.code = stg_cases.areacode join mart.DIM_AGEGRP da on da.age_group = stg_cases.age ")
cases_nation.registerTempTable("cases_nation")
fact_cases= spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY dim_date_id, dim_location_id, dim_agegrp_id) as FACT_COV_CASES_ID, dim_date_id, dim_location_id, dim_agegrp_id, malecases, femalecases, (malecases+femalecases) as totalcases from cases_nation")
fact_cases.write.insertInto("mart.FACT_COV_CASES", overwrite=True)

edu_agg = spark.sql("SELECT codes.RGNCD, codes.RGNNM, ethn.ethnicity, AVG(ethn.VALUE) as AVG_VAL FROM staging.ethnicity ethn JOIN (SELECT DISTINCT UTLACD, UTLANM, RGNCD, RGNNM FROM staging.england_codes) as codes on ethn.GEO_CODE = codes.UTLACD GROUP BY codes.RGNCD, codes.RGNNM, ethn.ethnicity")
edu_agg.registerTempTable("edu_agg")

ethn_and_edu = spark.sql("select dl.DIM_LOCATION_ID, agg.ethnicity, agg.avg_val as ethnic_percent, edu.no_qual, edu.nqf4_above from mart.dim_location dl join staging.education edu on dl.name = edu.region join edu_agg agg on dl.code = agg.rgncd ")
ethn_and_edu.registerTempTable("ethn_and_edu")

fact_regn_misc = spark.sql("SELECT ROW_NUMBER() OVER(ORDER BY dim_location_id, ethnicity) as FACT_REGN_MISCELLANEOUS_ID, dim_location_id, ethnicity, ethnic_percent, no_qual, nqf4_above from ethn_and_edu")
fact_regn_misc.write.insertInto("mart.FACT_REGN_MISCELLANEOUS", overwrite=True)

'''Displaying the schema of dataframes to the console log for verification. However, since these are Hive tables - the metadata will be intact'''
print("Deaths and Vaccines Fact schema")
fact_deaths_and_vaccines.printSchema()

print("Covid cases Fact schema")
fact_cases.printSchema()

print("Miscellaneous Region Fact schema")
fact_regn_misc.printSchema()

'''Logging Data Validity metrics to audit table'''


stg_counts = spark.sql(" SELECT 'STAGING' AS LAYER, 'HIVE TABLE' AS TYPE, 'Deaths_Nation' AS NAME, COUNT(*) as RECORDS_COUNT FROM staging.cov_deaths_nation GROUP BY 'STAGING', 'HIVE TABLE', 'Deaths_Nation' \
UNION ALL \
SELECT 'STAGING' AS LAYER, 'HIVE TABLE' AS TYPE, 'Deaths_Region_Yorkshire' AS NAME, COUNT(*) as cnt FROM staging.cov_deaths_yorkshire GROUP BY 'STAGING', 'HIVE TABLE', 'Deaths_Region_Yorkshire' \
UNION ALL \
SELECT 'STAGING' AS LAYER, 'HIVE TABLE' AS TYPE, 'Both_Vaccines' AS NAME, COUNT(*) as cnt FROM staging.cov_vaccinations GROUP BY 'STAGING', 'HIVE TABLE', 'Both_Vaccines' \
UNION ALL \
SELECT 'STAGING' AS LAYER, 'HIVE TABLE' AS TYPE, 'Both_Cases' AS NAME, COUNT(*) as cnt FROM staging.cov_cases_reg_age_gen GROUP BY 'STAGING', 'HIVE TABLE', 'Both_Cases' ")

mart_counts = spark.sql(" SELECT 'MART' AS LAYER, 'HIVE TABLE' AS TYPE, 'Deaths_Nation' AS NAME, COUNT(mf.FACT_COV_DEATHS_AND_VACCINES_ID) as RECORDS_COUNT FROM mart.FACT_COV_DEATHS_AND_VACCINES MF JOIN MART.DIM_LOCATION DL ON MF.DIM_LOCATION_ID=DL.DIM_LOCATION_ID WHERE DL.TYPE='Country' GROUP BY 'MART', 'HIVE TABLE', 'Deaths_Nation' \
UNION ALL \
SELECT 'MART' AS LAYER, 'HIVE TABLE' AS TYPE, 'Deaths_Region_Yorkshire' AS NAME, COUNT(mf.FACT_COV_DEATHS_AND_VACCINES_ID) as RECORDS_COUNT FROM mart.FACT_COV_DEATHS_AND_VACCINES MF JOIN MART.DIM_LOCATION DL ON MF.DIM_LOCATION_ID=DL.DIM_LOCATION_ID WHERE DL.TYPE='Area' GROUP BY 'MART', 'HIVE TABLE', 'Deaths_Region_Yorkshire' \
UNION ALL \
SELECT 'MART' AS LAYER, 'HIVE TABLE' AS TYPE, 'Both_Vaccines' AS NAME, COUNT(mf.FACT_COV_DEATHS_AND_VACCINES_ID) as RECORDS_COUNT FROM mart.FACT_COV_DEATHS_AND_VACCINES MF JOIN MART.DIM_LOCATION DL ON MF.DIM_LOCATION_ID=DL.DIM_LOCATION_ID WHERE DL.TYPE='Region' GROUP BY 'MART', 'HIVE TABLE', 'Both_Vaccines' \
UNION ALL \
SELECT 'MART' AS LAYER, 'HIVE TABLE' AS TYPE, 'Both_Cases' AS NAME, COUNT(FACT_COV_CASES_ID) as cnt FROM mart.FACT_COV_CASES GROUP BY 'MART', 'HIVE TABLE', 'Both_Cases' ")

total_counts = mart_counts.union(stg_counts)
total_counts_inter = total_counts.withColumn("LOAD_DATETIME", lit(current_timestamp())).withColumn("BATCH_ID", lit(0))
total_counts_inter.registerTempTable("total_counts_inter")

total_counts_final = spark.sql("SELECT DISTINCT CASE WHEN BD.MAX_BATCH_ID > INTER.BATCH_ID THEN BD.MAX_BATCH_ID+1 ELSE 1 END AS BATCH_ID, INTER.LAYER, INTER.TYPE, INTER.NAME, INTER.RECORDS_COUNT, INTER.LOAD_DATETIME \
    FROM total_counts_inter INTER INNER JOIN (SELECT CASE WHEN MAX(BATCH_ID) IS NULL THEN 0 ELSE MAX(batch_id) END AS MAX_BATCH_ID, 0 AS DUMMY FROM mart.DATA_VALIDITY_AUDIT) BD ON INTER.BATCH_ID=BD.DUMMY ")
total_counts_final.write.insertInto("mart.DATA_VALIDITY_AUDIT", overwrite=False)


'''Data Quality Checks'''
'''Duplicates Check on all fact tables and log the results to Data Qulaity Audit table'''

fact_deaths_and_vaccines_dup = fact_deaths_and_vaccines.groupBy("FACT_COV_DEATHS_AND_VACCINES_ID", "dim_date_id", "dim_location_id", "newDeaths", "firstDose", "secondDose").count().filter("count > 1")
fact_deaths_and_vaccines_dup.registerTempTable("fact_deaths_and_vaccines_dup")
fact_cases_dup = fact_cases.groupBy("FACT_COV_CASES_ID", "dim_date_id", "dim_location_id", "dim_agegrp_id", "malecases", "femalecases", "totalcases").count().filter("count > 1")
fact_cases_dup.registerTempTable("fact_cases_dup")
fact_regn_misc_dup = fact_regn_misc.groupBy("FACT_REGN_MISCELLANEOUS_ID", "dim_location_id", "ethnicity", "ethnic_percent", "no_qual", "nqf4_above").count().filter("count > 1")
fact_regn_misc_dup.registerTempTable("fact_regn_misc_dup")

dq_duplicate = spark.sql("SELECT CASE WHEN COUNT=0 THEN 'Passed' ELSE 'Failed' END AS RESULT, 'fact_deaths_and_vaccines' AS TABLE_NAME, 'mart' AS DATABASE_NAME, 'Duplicate Check' as DQ_TYPE FROM (SELECT COUNT(*) AS COUNT FROM fact_deaths_and_vaccines_dup) DEATHS_NATION \
UNION ALL \
SELECT CASE WHEN COUNT=0 THEN 'Passed' ELSE 'Failed' END AS RESULT, 'fact_cases' AS TABLE_NAME, 'mart' AS DATABASE_NAME, 'Duplicate Check' as DQ_TYPE FROM (SELECT COUNT(*) AS COUNT FROM fact_cases_dup) DEATHS_REGION \
UNION ALL \
SELECT CASE WHEN COUNT=0 THEN 'Passed' ELSE 'Failed' END AS RESULT, 'fact_regn_miscellaneous' AS TABLE_NAME, 'mart' AS DATABASE_NAME, 'Duplicate Check' as DQ_TYPE FROM (SELECT COUNT(*) AS COUNT FROM fact_regn_misc_dup) VACCINES")
dq_duplicate_inter = dq_duplicate.withColumn("LOAD_DATETIME", lit(current_timestamp())).withColumn("BATCH_ID", lit(0))
dq_duplicate_inter.registerTempTable("dq_duplicate_inter")

dq_duplicate_final = spark.sql("SELECT DISTINCT CASE WHEN BD.MAX_BATCH_ID > INTER.BATCH_ID THEN BD.MAX_BATCH_ID+1 ELSE 1 END AS BATCH_ID, INTER.DATABASE_NAME, INTER.TABLE_NAME, INTER.DQ_TYPE, INTER.RESULT, INTER.LOAD_DATETIME \
    FROM dq_duplicate_inter INTER INNER JOIN (SELECT CASE WHEN MAX(BATCH_ID) IS NULL THEN 0 ELSE MAX(batch_id) END AS MAX_BATCH_ID, 0 AS DUMMY FROM mart.DATA_QUALITY_AUDIT) BD ON INTER.BATCH_ID=BD.DUMMY ")
dq_duplicate_final.write.insertInto("mart.DATA_QUALITY_AUDIT", overwrite=False)

'''Null Check on all fact tables and log the results to Data Qulaity Audit table'''
'''User defined function to calculate null values in all columns of a dataframe'''

def test(input, name):
    df_agg = reduce(lambda a, b: a.union(b), (input.agg(count(when(isnull(c), c)).alias('Count')).select(lit(name).alias("Table"), lit(c).alias("Column"), "Count") for c in input.columns))
    return df_agg

fact_deaths_and_vaccines_null = test(fact_deaths_and_vaccines, "fact_deaths_and_vaccines")
fact_deaths_and_vaccines_null.registerTempTable("fact_deaths_and_vaccines_null")
fact_deaths_and_vaccines_null_res = spark.sql("SELECT CASE WHEN TOTAL_NULLS=0 THEN 'Passed' ELSE 'Failed' END AS RESULT, 'fact_deaths_and_vaccines' AS TABLE_NAME, 'mart' AS DATABASE_NAME, 'Null Check' as DQ_TYPE \
    FROM (SELECT TABLE, SUM(COUNT) AS TOTAL_NULLS FROM fact_deaths_and_vaccines_null GROUP BY TABLE) CASES")

fact_cases_null = test(fact_cases, "fact_cases")
fact_cases_null.registerTempTable("fact_cases_null")
fact_cases_null_res = spark.sql("SELECT CASE WHEN TOTAL_NULLS=0 THEN 'Passed' ELSE 'Failed' END AS RESULT, 'fact_cases' AS TABLE_NAME, 'mart' AS DATABASE_NAME, 'Null Check' as DQ_TYPE \
    FROM (SELECT TABLE, SUM(COUNT) AS TOTAL_NULLS FROM fact_cases_null GROUP BY TABLE) CASES")

fact_regn_miscellaneous_null = test(fact_regn_misc, "fact_regn_miscellaneous")
fact_regn_miscellaneous_null.registerTempTable("fact_regn_miscellaneous_null")
fact_regn_miscellaneous_null_res = spark.sql("SELECT CASE WHEN TOTAL_NULLS=0 THEN 'Passed' ELSE 'Failed' END AS RESULT, 'fact_regn_miscellaneous' AS TABLE_NAME, 'mart' AS DATABASE_NAME, 'Null Check' as DQ_TYPE \
    FROM (SELECT TABLE, SUM(COUNT) AS TOTAL_NULLS FROM fact_regn_miscellaneous_null GROUP BY TABLE) CASES")

dq_null = fact_deaths_and_vaccines_null_res.union(fact_cases_null_res.union(fact_regn_miscellaneous_null_res))
dq_null_inter = dq_null.withColumn("LOAD_DATETIME", lit(current_timestamp())).withColumn("BATCH_ID", lit(0))
dq_null_inter.registerTempTable("dq_null_inter")

dq_null_final = spark.sql("SELECT DISTINCT CASE WHEN BD.MAX_BATCH_ID > INTER.BATCH_ID THEN BD.MAX_BATCH_ID+1 ELSE 1 END AS BATCH_ID, INTER.DATABASE_NAME, INTER.TABLE_NAME, INTER.DQ_TYPE, INTER.RESULT, INTER.LOAD_DATETIME \
    FROM dq_null_inter INTER INNER JOIN (SELECT CASE WHEN MAX(BATCH_ID) IS NULL THEN 0 ELSE MAX(batch_id) END AS MAX_BATCH_ID, 0 AS DUMMY FROM mart.DATA_QUALITY_AUDIT WHERE DQ_TYPE='Null Check') BD ON INTER.BATCH_ID=BD.DUMMY ")
dq_null_final.write.insertInto("mart.DATA_QUALITY_AUDIT", overwrite=False)


'''Integrity checks - Facts tables'''
fact_deaths_and_vaccines_intgrty = spark.sql("SELECT dim_date_id as non_id from mart.FACT_COV_DEATHS_AND_VACCINES where dim_date_id not in (select dim_date_id from mart.dim_date) \
UNION \
select dim_location_id as non_id from mart.FACT_COV_DEATHS_AND_VACCINES where dim_location_id not in (select dim_location_id from mart.dim_location)")
fact_deaths_and_vaccines_intgrty.registerTempTable("fact_deaths_and_vaccines_intgrty")

fact_cases_intgrty = spark.sql("SELECT dim_date_id as non_id from mart.FACT_COV_CASES where dim_date_id not in (select dim_date_id from mart.dim_date) \
UNION \
select dim_location_id as non_id from mart.FACT_COV_CASES where dim_location_id not in (select dim_location_id from mart.dim_location) \
UNION \
select dim_agegrp_id as non_id from mart.FACT_COV_CASES where dim_location_id not in (select dim_agegrp_id from mart.dim_agegrp) ")
fact_cases_intgrty.registerTempTable("fact_cases_intgrty")

fact_regn_miscellaneous_intgrty = spark.sql("select dim_location_id as non_id from mart.FACT_REGN_MISCELLANEOUS where dim_location_id not in (select dim_location_id from mart.dim_location)")
fact_regn_miscellaneous_intgrty.registerTempTable("fact_regn_miscellaneous_intgrty")

fact_deaths_and_vaccines_intgrty_res = spark.sql("SELECT CASE WHEN TOTAL_ID>0 THEN 'Failed' ELSE 'Passed' END AS RESULT, 'fact_deaths_and_vaccines' AS TABLE_NAME, 'mart' AS DATABASE_NAME, 'Integrity Check' as DQ_TYPE \
    FROM (SELECT COUNT(non_id) AS TOTAL_ID FROM fact_deaths_and_vaccines_intgrty) CASES")

fact_cases_intgrty_res = spark.sql("SELECT CASE WHEN TOTAL_ID>0 THEN 'Failed' ELSE 'Passed' END AS RESULT, 'fact_cases' AS TABLE_NAME, 'mart' AS DATABASE_NAME, 'Integrity Check' as DQ_TYPE \
    FROM (SELECT COUNT(non_id) AS TOTAL_ID FROM fact_cases_intgrty) CASES")

fact_regn_miscellaneous_intgrty_res = spark.sql("SELECT CASE WHEN TOTAL_ID>0 THEN 'Failed' ELSE 'Passed' END AS RESULT, 'fact_regn_miscellaneous' AS TABLE_NAME, 'mart' AS DATABASE_NAME, 'Integrity Check' as DQ_TYPE \
    FROM (SELECT COUNT(non_id) AS TOTAL_ID FROM fact_regn_miscellaneous_intgrty) CASES")

dq_integrty = fact_deaths_and_vaccines_intgrty_res.union(fact_cases_intgrty_res.union(fact_regn_miscellaneous_intgrty_res))
dq_integrty_inter = dq_integrty.withColumn("LOAD_DATETIME", lit(current_timestamp())).withColumn("BATCH_ID", lit(0))
dq_integrty_inter.registerTempTable("dq_integrty_inter")

dq_integrty_final = spark.sql("SELECT DISTINCT CASE WHEN BD.MAX_BATCH_ID > INTER.BATCH_ID THEN BD.MAX_BATCH_ID+1 ELSE 1 END AS BATCH_ID, INTER.DATABASE_NAME, INTER.TABLE_NAME, INTER.DQ_TYPE, INTER.RESULT, INTER.LOAD_DATETIME \
    FROM dq_integrty_inter INTER INNER JOIN (SELECT CASE WHEN MAX(BATCH_ID) IS NULL THEN 0 ELSE MAX(batch_id) END AS MAX_BATCH_ID, 0 AS DUMMY FROM mart.DATA_QUALITY_AUDIT WHERE DQ_TYPE='Integrity Check') BD ON INTER.BATCH_ID=BD.DUMMY ")
dq_integrty_final.write.insertInto("mart.DATA_QUALITY_AUDIT", overwrite=False)
