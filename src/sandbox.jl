using CSV, DataFrames, Dates, VegaLite

# using Plots, StatsPlots
const datadir = joinpath(@__DIR__, "csse_covid_19_data", "csse_covid_19_time_series")
const figdir = joinpath(@__DIR__, "figures")

dateconv(s) = Date(string(s), "m/d/y") + Dates.Year(2000)

function ts(status::Symbol)
    df = CSV.File(joinpath(datadir, "time_series_19-covid-$status.csv")) |> DataFrame
    df = stack(df, Not([Symbol("Province/State"), Symbol("Country/Region"), :Lat, :Long]), variable_name=:Date, value_name=status)
    df.Date = dateconv.(df.Date)
    df
end

function ts()
    confirmed = ts(:Confirmed)
    deaths = ts(:Deaths)
    recovered = ts(:Recovered)

    join(join(confirmed, deaths, on=[Symbol("Province/State"), Symbol("Country/Region"), :Lat, :Long, :Date]),
            recovered, on=[Symbol("Province/State"), Symbol("Country/Region"), :Lat, :Long, :Date])
end

cdr = ts()

# ix = [!ismissing(x) && x == "St. Louis County, MO" for x in cdr[!, Symbol("Province/State")] ];
# cdr[ix, :]

function aggcdr(cdr; byvar=Symbol("Country/Region"), byvalues=["US"])
    ix = [!ismissing(x) && (x in byvalues) for x in cdr[!, byvar] ];

    by(cdr[ix,:], [:Date, byvar],
        Confirmed = :Confirmed => sum,
        Deaths = :Deaths => sum,
        Recovered = :Recovered => sum
    ) 
end

plotcountries = ["Mainland China", "Japan", "South Korea", "Italy", "Iran", "Germany", "US", "UK", "Israel"][2:end]
agg = aggcdr(cdr; byvalues=plotcountries)

p = agg |> @vlplot(:line, x=:Date, y=:Confirmed, color=Symbol("Country/Region"), width=800, height=600,
    config={
        background="#333",
        title={color="#fff"},
        style={"guide-label"={fill="#fff"}, "guide-title"={fill="#fff"}},
        axis={domainColor="#fff", gridColor="#888", tickColor="#fff"}
    }
)
save(joinpath(figdir, "agg.select.countries.pdf"), p)

agg = aggcdr(cdr; byvalues=["US"])

aggstacked(country) = stack(aggcdr(cdr; byvalues=[country]), Not([:Date, Symbol("Country/Region")]), variable_name=:Status, value_name=:People)
aggstacked("US")
p = aggstacked("US") |> 
    @vlplot(:line, x=:Date, y=:People, color=:Status,
        width=800, height=600,
        config={
            background="#333",
            title={color="#fff"},
            style={"guide-label"={fill="#fff"}, "guide-title"={fill="#fff"}},
            axis={domainColor="#fff", gridColor="#888", tickColor="#fff"}
        }
    )


# @df agg plot(x=:Date, y=:Confirmed, color=Symbol("Country/Region"))
