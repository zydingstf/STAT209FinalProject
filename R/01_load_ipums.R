# R/01_load_ipums.R

library(ipumsr)
library(dplyr)
library(arrow)

# 1. Read IPUMS metadata (DDI)
ddi <- read_ipums_ddi("Desktop/STAT209FinalProject/data/usa_00001.xml") # Might need to change path

# 2. Read microdata
ipums_raw <- read_ipums_micro(ddi)

# 3. Keep only the variables from extract
ipums_sel <- ipums_raw %>%
  select(
    YEAR,          # Census year
    SAMPLE,        # IPUMS sample identifier
    SERIAL,        # Household serial number
    CBSERIAL,      # Original Census Bureau household serial
    HHWT,          # Household weight
    CLUSTER,       # Household cluster for variance estimation
    STATEFIP,      # State (FIPS)
    STRATA,        # Household strata for variance estimation
    GQ,            # Group quarters status
    
    PERNUM,        # Person number in household
    PERWT,         # Person weight
    
    SEX,
    AGE,
    MARST,
    RACE,
    RACED,
    BPL,
    BPLD,
    CITIZEN,
    YRIMMIG,
    LANGUAGE,
    LANGUAGED,
    SPEAKENG,
    EDUC,
    EDUCD,
    
    EMPSTAT,
    EMPSTATD,
    LABFORCE,
    CLASSWKR,
    CLASSWKRD,
    OCC,
    IND,
    WKSWORK2,
    UHRSWORK,
    
    FTOTINC,
    INCWAGE
  )

# 4. Light cleaning / helpers

ipums_clean <- ipums_sel %>%
  filter(AGE >= 18, AGE <= 65)

# 5. Save to Parquet for use in both R and Python
write_parquet(ipums_clean, "Desktop/STAT209FinalProject/data/usa_00001_clean.parquet")

print(ipums_clean %>% select(YEAR, CITIZEN, INCWAGE) %>% head())
