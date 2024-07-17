using DataFrames
import SQLite
DBInterface = SQLite.DBInterface

db = SQLite.DB("football_data.sqlite")

league_table = DataFrame(DBInterface.execute(db,"SELECT * FROM Leagues"))
team_table = DataFrame(DBInterface.execute(db,"SELECT * FROM Teams"))
team_marketvalue_table = DataFrame(DBInterface.execute(db,"SELECT * FROM TeamMarketvalues"))
match_table = DataFrame(DBInterface.execute(db,"SELECT * FROM Matches"))
lineup_table = DataFrame(DBInterface.execute(db,"SELECT * FROM LineupMarketvalues"))
standings_table = DataFrame(DBInterface.execute(db,"SELECT * FROM EOSStandings"))

my_describe(table::AbstractDataFrame, cols)::DataFrame = describe(table,:mean,:std,:min,:q25,:median,:q75,:max,cols=cols)
function println_my_describe_by_league(table::AbstractDataFrame, cols)
    println("All leagues:\n",my_describe(table,cols))
    foreach(sdf->println("League: $(league_table.league_name[sdf.league_id[1]])\n",my_describe(sdf,cols)),groupby(table,:league_id))
end

println_my_describe_by_league(match_table,r"fulltime.+goals|odds")
println_my_describe_by_league(leftjoin(lineup_table,match_table,on=:match_id=>:id),r"num|market_val")
println_my_describe_by_league(standings_table,Between(:wins,:points))
