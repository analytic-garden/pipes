#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
covid_hospitalizations.py - downlaod and extract COVID hospitalization data
author: Bill Thompson
license: GPL 3
copyright: 2022-04-15
"""

import pandas as pd

def select_hospitals(df, hospitals):
 return df.loc[df['Facility Name'].isin(hospitals)]

def main():
    url = 'https://health.data.ny.gov/api/views/jw46-jpb7/rows.csv?accessType=DOWNLOAD'
    out_file = 'hospitalizations_7day_ma_2.csv'
    hospitals = ('ALBANY MEDICAL CENTER HOSPITAL', 'ST PETERS HOSPITAL', 'COLUMBIA MEMORIAL HOSPITAL',
                 'SAMARITAN HOSPITAL', 'SARATOGA HOSPITAL', 'ELLIS HOSPITAL')
    cols = ['As of Date', 'Facility Name',
            'Patients Newly Admitted', 'Patients Positive After Admission']
    lag = 7

    df = (pd.read_csv(url)
        .filter(cols)
        .assign(As_of_Date = lambda x: pd.to_datetime(x['As of Date'], format = '%m/%d/%Y'))
        .pipe(select_hospitals, hospitals)  # I couldn't use a lambda here
        .groupby(['As_of_Date'])[['Patients Newly Admitted', 'Patients Positive After Admission']]
        .sum()
        .reset_index()
        .assign(New_Covid_Cases = lambda x: x['Patients Newly Admitted'] + x['Patients Positive After Admission'])
        .assign(Moving_Average = lambda x: x['New_Covid_Cases'].rolling(window = lag).mean())
        .dropna()
        .filter(['As_of_Date', 'New_Covid_Cases', 'Moving_Average'])
        .sort_values(['As_of_Date'], ascending = 0)
    )
    
    df.to_csv(out_file, index = False)
    
if __name__ == "__main__":
    main()
