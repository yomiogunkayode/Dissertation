drop table if exists staging.country_codes;
drop table if exists staging.cov_cases_reg_age_gen;
drop table if exists staging.cov_vaccinations;
drop table if exists staging.education;
drop table if exists staging.england_codes;
drop table if exists staging.ethnicity;
drop table if exists staging.income;
drop table if exists staging.yorkshire_codes;
drop table if exists staging.data_quality_audit;
drop table if exists staging.data_validity_audit;
drop database if exists staging;

drop table if exists mart.dim_agegrp;
drop table if exists mart.dim_date;
drop table if exists mart.dim_location;
drop table if exists mart.fact_cov_cases;
drop table if exists mart.fact_regn_miscellaneous;
drop table if exists mart.data_quality_audit;
drop table if exists mart.data_validity_audit;
drop database if exists mart;