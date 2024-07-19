import Pkg
Pkg.activate(@__DIR__,io=devnull)

using CSV, DataFrames
import Printf.@sprintf
import SQLite
DBInterface = SQLite.DBInterface


function _describe_and_format(table::AbstractDataFrame, cols; csv_filename::String)
    summ = describe(table,:mean,:std,:q25,:median,:q75,cols=cols)

    style_var_name(s::Symbol) = replace(string(s),"fulltime_"=>"","market_average_"=>"",'_'=>' ')
    style_nums(x::Real)::String = x >= 1000 ? @sprintf("%.1e",x) : isinteger(x) ? string(Int(x)) : string(round(x,digits=3))

    summ[!,:variable] = style_var_name.(summ[!,:variable])
    summ[!,Not(:variable)] = style_nums.(summ[!,Not(:variable)])
    CSV.write(csv_filename,summ,append=isfile(csv_filename))
end

function create_summaries(table::AbstractDataFrame, cols)
    if :starters_total_market_val in propertynames(table)
        table[!,:lineup_value] = table[!,:starters_total_market_val] + table[!,:bench_total_market_val]
    end
    _describe_and_format(table, cols, csv_filename="csv_files/summary_all.csv")
    
    table[!,:country] = table[!,:league_id] .|> (x-> x<=4 ? "eng" : x<=6 ? "ger" : "sco")
    for sdf in groupby(table,:country)
        _describe_and_format(sdf, cols, csv_filename="csv_files/summary_$(lowercase(sdf[1,:country])).csv")
    end
end


# Runner script

for fn in ("csv_files/summary_all.csv","csv_files/summary_eng.csv","csv_files/summary_ger.csv","csv_files/summary_sco.csv")
    isfile(fn) && rm(fn)
end

disable_sigint() do
    db = SQLite.DB("football_data.sqlite")

    league_table = DataFrame(DBInterface.execute(db,"SELECT * FROM Leagues"))
    # team_table = DataFrame(DBInterface.execute(db,"SELECT * FROM Teams"))
    # team_marketvalue_table = DataFrame(DBInterface.execute(db,"SELECT * FROM TeamMarketvalues"))
    match_table = DataFrame(DBInterface.execute(db,"SELECT * FROM Matches"))
    lineup_table = DataFrame(DBInterface.execute(db,"SELECT * FROM LineupMarketvalues"))
    standings_table = DataFrame(DBInterface.execute(db,"SELECT * FROM EOSStandings"))

    create_summaries(match_table,r"fulltime.+goals|odds")
    create_summaries(leftjoin(lineup_table,match_table,on=:match_id=>:id),:lineup_value)
    create_summaries(standings_table,Between(:wins,:points))
end
