using CSV, DataFrames, Dates, VegaLite

const datadir = joinpath(@__DIR__, "csse_covid_19_data", "csse_covid_19_time_series")
const figdir = joinpath(@__DIR__, "figures")

dateconv(s) = Date(string(s), "m/d/y") + Dates.Year(2000)

stateisocsv = CSV.File(joinpath(@__DIR__, "geoinfo", "states.csv"))
stateisomap = Dict(row.state => row.name for row in stateisocsv)
function cleanstate(s, c) 
    if c != "US"
        s, false
    else
        if occursin(',', s)
            if endswith(s, ", D.C.")
                iso = "DC"
            else
                iso = rstrip(s)[end-1:end]
            end
            
            if haskey(stateisomap, iso)
                stateisomap[iso], false
            else
                @warn "iso not found for $s"
                missing, false
            end
        else
            s, true
        end
    end
end

# # tests
# cleanstate("Hubei", "Mainland China")
# cleanstate("bla, NY", "US")
# cleanstate("New York", "US")
# cleanstate("Washington, D.C.", "US")
# cleanstate("Jackson County, OR ", "US")

function ts(status::Symbol)
    df = CSV.File(joinpath(datadir, "time_series_19-covid-$status.csv")) |> DataFrame
    df = stack(df, Not([Symbol("Province/State"), Symbol("Country/Region"), :Lat, :Long]), variable_name=:Date, value_name=status)
    df.Date = dateconv.(df.Date)
    df
end

cleanint(x::Missing) = missing
cleanint(x) = Int(clamp(x, 0, Inf))

function ts()
    confirmed = ts(:Confirmed)
    deaths = ts(:Deaths)
    recovered = ts(:Recovered)

    cdr = join(join(confirmed, deaths, on=[Symbol("Province/State"), Symbol("Country/Region"), :Lat, :Long, :Date]),
            recovered, on=[Symbol("Province/State"), Symbol("Country/Region"), :Lat, :Long, :Date])

    stateisagg = cleanstate.(cdr[!,Symbol("Province/State")], cdr[!,Symbol("Country/Region")])
    cdr.State = first.(stateisagg)
    cdr.Total = last.(stateisagg)

    # totals per state were not calculated until 2020-03-10

    # disaggregated data before change
    ix = [!ismissing(x) && (x=="US") for x in cdr[!, Symbol("Country/Region")] ] .& (.! cdr.Total)
    usdisaggpre = cdr[ix,:]

    # manually calculated totals after change are missing other countries
    ix = [!ismissing(x) && (x=="US") for x in cdr[!, Symbol("Country/Region")] ] .& (cdr.Date .> Date(2020,03,9)) .& (.! cdr.Total)
    usstates2 = by(cdr[ix,:], [:Date, :State],
        Confirmed_Dis = :Confirmed => sum,
        Deaths_Dis = :Deaths => sum,
        Recovered_Dis = :Recovered => sum
    ) 

    # state totals from source
    ix = [!ismissing(x) && (x=="US") for x in cdr[!, Symbol("Country/Region")] ] .& (cdr.Date .> Date(2020,03,9)) .& cdr.Total
    usstates3 = cdr[ix, [:Date, Symbol("Country/Region"), :Lat, :Long, :State, :Confirmed, :Deaths, :Recovered]]

    # create other countries totals as residual
    usother = join(usstates2, usstates3, on=[:Date, :State])
    usother.Confirmed = cleanint.(usother.Confirmed .- usother.Confirmed_Dis)
    usother.Deaths = cleanint.(usother.Deaths .- usother.Deaths_Dis)
    usother.Recovered = cleanint.(usother.Recovered .- usother.Recovered_Dis)
    usother.Total = falses(length(usother.Recovered))
    usother[!,Symbol("Province/State")] .= "Other"

    usdisagg = vcat(usdisaggpre, usother[:, names(usdisaggpre)])

    # remove totals from cdr and add others
    cdr = vcat(cdr[(.! cdr.Total), :], usother[:, names(usdisaggpre)])

    cdr
end

cdr = ts()

function aggcdr(cdr; byvar=Symbol("Country/Region"), byvalues=["US"])
    ix = [!ismissing(x) && (x in byvalues) for x in cdr[!, byvar] ];

    agg = by(cdr[ix,:], [:Date, byvar],
        Confirmed = :Confirmed => sum,
        Deaths = :Deaths => sum,
        Recovered = :Recovered => sum
    ) 


    # fix date bug in tooltips
    agg.Date .+= Dates.Day(1)

    agg
end

function plotcountrystatus(cdr, countries, status::Symbol=:Confirmed)
    agg = aggcdr(cdr; byvalues=plotcountries)

    agg |> @vlplot(x=:Date, y=status, color=Symbol("Country/Region"), width=800, height=600,
        mark={:line, point={filled=false}},
        config={
            background="#333",
            title={color="#fff"},
            style={"guide-label"={fill="#fff"}, "guide-title"={fill="#fff"}},
            axis={domainColor="#fff", gridColor="#888", tickColor="#fff"}
        }
    )
end

# agg = aggcdr(cdr; byvalues=plotcountries)

function plotcountrystatuschange(cdr, countries, status::Symbol=:Confirmed)
    agg = aggcdr(cdr; byvalues=plotcountries)

    sort!(agg, [Symbol("Country/Region"), :Date])
    dagg = by(agg, Symbol("Country/Region")) do df
        (
            Country=df[!,Symbol("Country/Region")], 
            Date=df.Date,
            newstatus = vcat([missing], diff(df[!,status]))
        )
    end

    dagg |> @vlplot(x=:Date, y=:newstatus, color=Symbol("Country/Region"), width=800, height=600,
        mark={:line, point={filled=false}},
        ylabel="New $status",
        config={
            background="#333",
            title={color="#fff"},
            style={"guide-label"={fill="#fff"}, "guide-title"={fill="#fff"}},
            axis={domainColor="#fff", gridColor="#888", tickColor="#fff"}
        }
    )
end

# unique(cdr[Symbol("Country/Region")])
plotcountries = ["Italy", "Japan", "Korea, South", "Germany", "US", "UK", "Israel"]
p = plotcountrystatus(cdr, plotcountries, :Confirmed)
p = plotcountrystatuschange(cdr, plotcountries, :Confirmed)

plotcountries = ["US", "Israel"]
p = plotcountrystatus(cdr, plotcountries, :Confirmed)
p = plotcountrystatuschange(cdr, plotcountries, :Confirmed)

# save(joinpath(figdir, "agg.select.countries.pdf"), p)

aggstacked(cdr, country) = stack(aggcdr(cdr; byvalues=[country]), Not([:Date, Symbol("Country/Region")]), variable_name=:Status, value_name=:People)

function plotcountry(cdr, country)
    aggstacked(cdr, country) |> 
        @vlplot(x=:Date, y=:People, color=:Status, title=country,
            mark={:line, point={filled=false}},
            width=800, height=600,
            config={
                background="#333",
                title={color="#fff"},
                style={"guide-label"={fill="#fff"}, "guide-title"={fill="#fff"}},
                axis={domainColor="#fff", gridColor="#888", tickColor="#fff"}
            }
        )
end

plotcountry(cdr, "Israel")
plotcountry(cdr, "US")
plotcountry(cdr, "Italy")
plotcountry(cdr, "Korea, South")
