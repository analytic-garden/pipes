covid_hospitalizations <- function(lag = 7) {
  require(tidyverse)
  require(zoo)
  
  url <- 'https://health.data.ny.gov/api/views/jw46-jpb7/rows.csv?accessType=DOWNLOAD'
  hospitals <-c('ALBANY MEDICAL CENTER HOSPITAL', 'ST PETERS HOSPITAL', 'COLUMBIA MEMORIAL HOSPITAL',
                'SAMARITAN HOSPITAL', 'SARATOGA HOSPITAL', 'ELLIS HOSPITAL')
  
  df <- read.csv(url) %>%
    select(As.of.Date, Facility.Name, Patients.Newly.Admitted, Patients.Positive.After.Admission) %>%
    filter(Facility.Name %in% {{ hospitals }}) %>%
    mutate(As.of.Date = as.Date(As.of.Date, format = '%m/%d/%Y')) %>%
    group_by(As.of.Date) %>%
    summarise(Patients.Newly.Admitted = sum(Patients.Newly.Admitted),
              Patients.Positive.After.Admission = sum(Patients.Positive.After.Admission)) %>%
    mutate(New.Covid.Cases = Patients.Newly.Admitted + Patients.Positive.After.Admission) %>%
    mutate(Moving.Avg = rollmean(New.Covid.Cases, lag, align = 'right', na.pad = TRUE)) %>%
    select(As.of.Date, New.Covid.Cases, Moving.Avg) %>%
    map_df(rev)
      
  return(df)
}