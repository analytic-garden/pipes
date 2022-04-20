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
using Pipe

function main()
    url = "https://health.data.ny.gov/api/views/jw46-jpb7/rows.csv?accessType=DOWNLOAD"
    out_file = "hospitalizations_7day_ma_4.csv"
    hospitals = ["ALBANY MEDICAL CENTER HOSPITAL", "ST PETERS HOSPITAL", "COLUMBIA MEMORIAL HOSPITAL",
                 "SAMARITAN HOSPITAL", "SARATOGA HOSPITAL", "ELLIS HOSPITAL"]
    cols = ["As of Date", "Facility Name",
            "Patients Newly Admitted", "Patients Positive After Admission"]
    lag = 7

    # helper for runmean
    mva(x) = runmean(x, lag)

    df = @pipe CSV.read(download(url), DataFrame, dateformat = "mm/dd/yyyy") |>
        select(_, cols) |>
        filter(row -> row."Facility Name" ∈ hospitals, _) |>
        combine(groupby(_, "As of Date"), ["Patients Newly Admitted", "Patients Positive After Admission"] .=> sum) |>
        transform!(_, ["Patients Newly Admitted_sum", "Patients Positive After Admission_sum"] => (+) => "New Covid Cases") |>
        transform!(_, ["New Covid Cases"] => mva => "Moving Average") |>
        select(_, ["As of Date", "New Covid Cases", "Moving Average"]) |>
        sort!(_, "As of Date", rev = true)

    CSV.write(out_file, df)
end

main()