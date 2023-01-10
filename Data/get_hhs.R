#HHS Data
# Socrata API token for this project (do not share)
token <- "llBMOEMWQQKKIg7XEAwuNVri4" 

##python version: https://lvngd.com/blog/accessing-nyc-open-data-with-python-and-the-socrata-open-data-api/


hhs <- read.socrata("https://healthdata.gov/resource/g62h-syeh.csv",
                    app_token = token)


fips <- read_csv("data/fips-codes.csv")
uspop <- read_csv("data/uspop.csv")

colnames(fips) <- c("fips", "state_full", "state", "alphacount")
colnames(uspop) <- c("state_full", "pop")

# Add in FIPS and population data, and calculate per-capita admissions
hhs <- hhs %>% 
  mutate(date = date(date)) %>%
  left_join(fips) %>% 
  left_join(uspop) %>%
  filter(!is.na(state_full)) %>% # For now, only 50 states + DC
  # Create more convenient names for admissions
  mutate(flu = previous_day_admission_influenza_confirmed,
         covid = previous_day_admission_adult_covid_confirmed) %>%
  # Per-capita
  mutate(flu_per_cap = flu / pop * 1e5) %>% 
  mutate(covid_per_cap = covid / pop * 1e5) %>% 
  # 7-day average for admissions
  arrange(state, date) %>% 
  group_by(state) %>%
  mutate(flu_av7 = av7fun(flu, date),
         covid_av7 = av7fun(covid, date),
         flu_per_cap_av7 = av7fun(flu_per_cap, date),
         covid_per_cap_av7 = av7fun(covid_per_cap, date)) %>% 
  mutate(ratio_log = log(flu_per_cap_av7) - log(covid_per_cap_av7)) %>%
  ungroup()
