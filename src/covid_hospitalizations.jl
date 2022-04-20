#!/usr/bin/env julia
# -*- coding: utf-8 -*-

#=
covid_hospitalizations.jl - downlaod and extract COVID hospitalization data
author: Bill Thompson
license: GPL 3
copyright: 2022-04-15
=#

using DataFrames
using CSV
using Dates
using RollingFunctions

function main()
    url = "https://health.data.ny.gov/api/views/jw46-jpb7/rows.csv?accessType=DOWNLOAD"
    # datafile = "hosp_2022_04_18.csv"
    out_file = "hospitalizations_7day_ma_3.csv"
    hospitals = ["ALBANY MEDICAL CENTER HOSPITAL", "ST PETERS HOSPITAL", "COLUMBIA MEMORIAL HOSPITAL",
                 "SAMARITAN HOSPITAL", "SARATOGA HOSPITAL", "ELLIS HOSPITAL"]
    cols = ["As of Date", "Facility Name",
            "Patients Newly Admitted", "Patients Positive After Admission"]
    lag = 7

    df = CSV.read(download(url), DataFrame)
    # df = CSV.read(datafile, DataFrame)
    df = select(df, cols)
    df = filter(row -> row."Facility Name" ∈ hospitals, df)
    df[!, "As of Date"] = Date.(df."As of Date", "mm/dd/yyyy")
    df = combine(groupby(df, "As of Date"), ["Patients Newly Admitted", "Patients Positive After Admission"] .=> sum)
    df."New Covid Cases" = df."Patients Newly Admitted_sum" + df."Patients Positive After Admission_sum"
    df."Moving Average" = runmean(df."New Covid Cases", lag)
    df = select(df, ["As of Date", "New Covid Cases", "Moving Average"])
    sort!(df, "As of Date", rev = true)

    CSV.write(out_file, df)
end

main()