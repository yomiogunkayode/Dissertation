from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
spark = SparkSession.builder.master("yarn-client") \
                    .appName('Dissertation-Covid-Sourcing') \
                    .getOrCreate()

'''Set loglevel to WARN to avoid logs flooding on the console'''
spark.sparkContext.setLogLevel("WARN")


'''Schema Declaration for Lower Authority (LA) codes lookup file'''
la_schema = StructType([StructField("LADCD", StringType(), True)\
                   ,StructField("LADNM", StringType(), True)\
                   ,StructField("LADCD_ACTIVE", StringType(), True)\
                   ,StructField("LADNM_ACTIVE", StringType(), True)\
                   ,StructField("UTLACD", StringType(), True)\
                   ,StructField("UTLANM", StringType(), True)\
                   ,StructField("CAUTHCD", StringType(), True)\
                   ,StructField("CAUTHNM", StringType(), True)\
                   ,StructField("RGNCD", StringType(), True)\
                   ,StructField("RGNNM", StringType(), True)\
                   ,StructField("CTRYCD", StringType(), True)\
                   ,StructField("CTRYNM", StringType(), True)\
                   ,StructField("EWCD", StringType(), True)\
                   ,StructField("EWNM", StringType(), True)\
                   ,StructField("GBCD", StringType(), True)\
                   ,StructField("GBNM", StringType(), True)\
                   ,StructField("UKCD", StringType(), True)\
                   ,StructField("UKNM", StringType(), True)\
                   ,StructField("Current", StringType(), True)\
                   ,StructField("LAD20CD", StringType(), True)\
                   ,StructField("LAD20NM", StringType(), True) ])

ethnicity_src = spark.createDataFrame(pd.read_csv("https://raw.githubusercontent.com/mohasin-ibrahim/admp/main/suppl_files/ethnicity_uk.csv"))
ethnicity_src.write.mode("overwrite").csv("/data/input/static/ethnicity")

education_src = spark.createDataFrame(pd.read_csv("https://raw.githubusercontent.com/mohasin-ibrahim/admp/main/suppl_files/England_Education_Levels.csv"))
education_src.write.format("csv").mode('overwrite').save("/data/input/static/education")

income_wealth_src = spark.createDataFrame(pd.read_csv("https://raw.githubusercontent.com/mohasin-ibrahim/admp/main/suppl_files/Income_Wealth_UK.csv"))
income_wealth_src.write.format("csv").mode('overwrite').save("/data/input/static/income")

'''Saving the dataframe as ~ delimited file to avoid clashing due to in-column values'''
lacode_src = spark.createDataFrame(pd.read_csv("https://raw.githubusercontent.com/drkane/geo-lookups/master/la_all_codes.csv", header=None), schema=la_schema)
lacode_src.write.format("csv").mode("overwrite").option("quote", "").option("delimiter", "~").save("/data/input/static/lacode")

cases_age_region_src = spark.createDataFrame(pd.read_csv("https://api.coronavirus.data.gov.uk/v2/data?areaType=region&metric=newCasesBySpecimenDateAgeDemographics&format=csv"))
cases_age_region_src.write.format("csv").mode('overwrite').save("/data/input/dynamic/covid/infections/england/age")

vaccin_first_src = spark.createDataFrame(pd.read_csv("https://api.coronavirus.data.gov.uk/v2/data?areaType=region&metric=newPeopleVaccinatedFirstDoseByVaccinationDate&format=csv"))
vaccin_first_src.write.format("csv").mode('overwrite').save("/data/input/dynamic/covid/vaccinations/england/first")

vaccin_second_src = spark.createDataFrame(pd.read_csv("https://api.coronavirus.data.gov.uk/v2/data?areaType=region&metric=newPeopleVaccinatedSecondDoseByVaccinationDate&format=csv"))
vaccin_second_src.write.format("csv").mode('overwrite').save("/data/input/dynamic/covid/vaccinations/england/second")

male_cases_src = spark.createDataFrame(pd.read_csv("https://api.coronavirus.data.gov.uk/v2/data?areaType=region&metric=maleCases&format=csv"))
male_cases_src.write.format("csv").mode('overwrite').save("/data/input/dynamic/covid/infections/england/male")

female_cases_src = spark.createDataFrame(pd.read_csv("https://api.coronavirus.data.gov.uk/v2/data?areaType=region&metric=femaleCases&format=csv"))
female_cases_src.write.format("csv").mode('overwrite').save("/data/input/dynamic/covid/infections/england/female")
