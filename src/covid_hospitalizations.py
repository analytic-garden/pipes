#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
covid_hospitalizations.py - downlaod and extract COVID hospitalization data
author: Bill Thompson
license: GPL 3
copyright: 2022-04-15
"""

import pandas as pd

def main():
    url = 'https://health.data.ny.gov/api/views/jw46-jpb7/rows.csv?accessType=DOWNLOAD'
    out_file = 'hospitalizations_7day_ma.csv'
    hospitals = ('ALBANY MEDICAL CENTER HOSPITAL', 'ST PETERS HOSPITAL', 'COLUMBIA MEMORIAL HOSPITAL',
                 'SAMARITAN HOSPITAL', 'SARATOGA HOSPITAL', 'ELLIS HOSPITAL')
    cols = ['As of Date', 'Facility Name',
            'Patients Newly Admitted', 'Patients Positive After Admission']
    lag = 7

    df = pd.read_csv(url)
    df = df[cols]
    df = df.loc[df['Facility Name'].isin(hospitals)]
    df['As of Date'] = pd.to_datetime(df['As of Date'], format = '%m/%d/%Y')
    df = df.groupby(['As of Date'])[['Patients Newly Admitted', 'Patients Positive After Admission']].sum()
    df['New Covid Cases'] = df['Patients Newly Admitted'] + df['Patients Positive After Admission']
    df['Moving Average'] = df['New Covid Cases'].rolling(window = lag).mean()
    df = df.reset_index()
    df['Moving Average'].dropna(inplace = True)
    df = df[['As of Date', 'New Covid Cases', 'Moving Average']]
    df = df.sort_values(['As of Date'], ascending = 0)
    
    df.to_csv(out_file, index = False)
    
if __name__ == "__main__":
    main()
