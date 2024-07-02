import Pkg
Pkg.activate(@__DIR__,io=devnull)

using ArgParse, Cascadia, DataFrames, Dates, Gumbo
import CSV, HTTP, JSON, REPL.TerminalMenus, SQLite, StringDistances
import MD5.md5
import DataStructures.OrderedDict
import InlineStrings: InlineString, String15, String31

if !isfile("team_alias_dict.json")
	write("team_alias_dict.json","{}")
end
if !isdir("rawdata")
	mkdir("rawdata")
end
if !isdir("csv_files")
	mkdir("csv_files")
end

const HTTP_REQUEST_HEADERS::Dict{String,String} = Dict("User-Agent"=>"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
const LEAGUES::Vector{String15} = ["premier-league","championship","league-one","league-two"]
const MATCH_HEADERS_MAPPING::OrderedDict{Symbol,Symbol} = OrderedDict(:Date=>:date,:HomeTeam=>:HomeTeam,:AwayTeam=>:AwayTeam,:FTHG=>:fulltime_home_goals,
	:FTAG=>:fulltime_away_goals,:FTR=>:fulltime_result,:HTHG=>:halftime_home_goals,:HTAG=>:halftime_away_goals,:HTR=>:halftime_result,:HS=>:home_shots,
	:AS=>:away_shots,:HST=>:home_shots_on_target,:AST=>:away_shots_on_target,:HC=>:home_corners,:AC=>:away_corners,:HF=>:home_fouls,:AF=>:away_fouls,
	:HY=>:home_yellow_cards,:AY=>:away_yellow_cards,:HR=>:home_red_cards,:AR=>:away_red_cards,:AvgH=>:market_average_home_win_odds,:AvgD=>:market_average_draw_odds,
	:AvgA=>:market_average_away_win_odds)
const TEAM_ALIAS_DICT::Dict{String,String} = JSON.parsefile("team_alias_dict.json")
SCRAPE_YEAR_RANGE = 2010:2023
SCRAPE_DELAY_RANGE = 1:.1:3

function get_raw_data_from(url::String; check_web_cache::Bool=true, enable_web_cache::Bool=true)::String
	path = joinpath("rawdata",bytes2hex(md5(url)))
	@debug "getting $(url), locally should be $(path)"
	if !isfile(path) || !check_web_cache
		response = HTTP.get(url,headers=HTTP_REQUEST_HEADERS)
		if response.status != 200
			throw(response)
		end
		data = String(response.body)
		if !endswith(url,".csv")
			@debug "reducing the size of html"
			data = remove_excess_html(data)
		end
		sleep(rand(SCRAPE_DELAY_RANGE))
		if enable_web_cache
			@debug "saving file to cache"
			write(path,data)
		end
		return data
	else
		data = read(path,String)
		if data == "" || occursin('\0',data)
			@warn "$(path) is empty/corrupted! Attempting to refetch"
			return get_raw_data_from(url, check_web_cache=false, enable_web_cache=enable_web_cache)
		else
			return data
		end
	end
end

function remove_excess_html(str::String)::String
	buf = IOBuffer()
	scripts = Iterators.partition(findall(r"</?script[^>]*>",str),2) .|> (t->first(t[1]):last(t[2]))
	i = 1
	for s in scripts
		write(buf,str[i:first(s)-1])
		i = last(s)+1
	end
	write(buf,str[i:end])
	seekstart(buf)
	return read(buf,String)
end

function save_alias_dict()
	write("team_alias_dict.json",JSON.json(TEAM_ALIAS_DICT,4))
end

function date_obj_to_str(date_obj::Date)::String
	return Dates.format(date_obj,dateformat"YYYY-mm-dd")
end

function standardize_date(str::AbstractString)::String
	# Note: This function assumes that truncated years refer to 20XX
	date_obj = Date(str,dateformat"dd/mm/YY")
	if year(date_obj) < 100
		date_obj += Year(2000)
	end
	return date_obj_to_str(date_obj)
end

function standardize_team_name(team_name::AbstractString)::AbstractString
	team_name = String(strip(team_name))
	return get!(TEAM_ALIAS_DICT,team_name) do
		println("Name \"$(team_name)\" not found!")
		menu = TerminalMenus.RadioMenu(vcat(["(This is the standardized name)"],sort!(string.(unique(values(TEAM_ALIAS_DICT))),by=s->StringDistances.RatcliffObershelp()(team_name,s)),["(None of the above)"]))
		idx = TerminalMenus.request("Select the standardized name",menu)
		if idx == 1
			return team_name
		elseif idx == length(menu.options)
			println("Enter the standardized name")
			std_name = readline()
			TEAM_ALIAS_DICT[std_name] = std_name
			return std_name
		else
			return menu.options[idx]
		end
	end
end

function assert_zero_missing_values(table::DataFrame)
	rows_with_missings = filter(r->any(ismissing,collect(r)),eachrow(table))
	if !isempty(rows_with_missings)
		println("These rows have missing values!")
		foreach(println,rows_with_missings)
		error("You have missing values!")
	end
end

# Market data functions

function parse_market_val(str::AbstractString)::Float64
	# Using list of pairs to ensure that the larger suffixes are tested first
	if str == "-"
		return 0.0
	end
	suffixes = ["bn"=>10^9,"k"=>10^3,"m"=>10^6,"b"=>10^9]
	mult = 1
	for (s,m) in suffixes
		if endswith(str,s)
			mult = m
			str = replace(str,'€'=>"",s=>"")
			break
		end
	end
	return parse(Float64,str)*mult
end
parse_market_val(::Missing)::Missing = missing

function scrape_team_marketvalue_data(; check_web_cache::Bool=true, enable_web_cache::Bool=true)::DataFrame
	@info "Scraping team marketvalue data"
	baseurl = "https://www.transfermarkt.us/{LEAGUE}/startseite/wettbewerb/GB{NUM}/plus/?saison_id={SEASON}"

	df = DataFrame(season=Int[],league=String15[],league_num=Int[],transfermarkt_team_id=Int[],team_name=String31[],squad_size=Int[],avg_age=Float64[],num_foreigners=Int[],avg_market_val=String15[],total_market_val=String15[])
	
	for season = SCRAPE_YEAR_RANGE
		@debug "Scraping season $(season)"
		for (num,league) in enumerate(LEAGUES)
			url = replace(baseurl,"{NUM}"=>num,"{LEAGUE}"=>league,"{SEASON}"=>season)
			data = get_raw_data_from(url, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
			htmldata = parsehtml(data)
			
			for row in eachmatch(sel"div#yw1 table.items tbody tr",htmldata.root)
				_,name_cell,squad_cell,avg_age_cell,foreigners_cell,avg_market_cell,total_market_cell = eachmatch(sel"td",row)
				team_id = parse(Int,match(r"verein/(\d+)\b",getattr(name_cell[1],"href")).captures[1])
				team_name = name_cell[1][1].text
				squad_size = parse(Int,squad_cell[1][1].text)
				avg_age = parse(Float64,avg_age_cell[1].text)
				foreigners = parse(Int,foreigners_cell[1].text)
				avg_market_val = avg_market_cell[1].text
				total_market_val = total_market_cell[1][1].text
				push!(df,[season,league,num,team_id,team_name,squad_size,avg_age,foreigners,avg_market_val,total_market_val])
			end
		end
	end

	return df
end

function clean_team_marketvalue_data!(df::DataFrame)
	transform!(df,:avg_market_val=>ByRow(parse_market_val),:total_market_val=>ByRow(parse_market_val),:team_name=>ByRow(standardize_team_name),renamecols=false)
	assert_zero_missing_values(df)
end

function scrape_lineup_data(; check_web_cache::Bool=true, enable_web_cache::Bool=true)::DataFrame
	@info "Scraping lineup data"
	baseurl = "https://www.transfermarkt.com/{LEAGUE}/gesamtspielplan/wettbewerb/GB{NUM}/saison_id/{SEASON}"

	df = DataFrame(transfermarkt_team_id=Int[],team_name=String31[],date=String15[],starters_num_foreigners=Int[],starters_avg_age=Float64[],starters_purchase_val=String15[],
		starters_total_market_val=String15[],bench_num_foreigners=Int[],bench_avg_age=Float64[],bench_purchase_val=String15[],bench_total_market_val=String15[])
	allowmissing!(df,r"starters|bench")
	
	for season = SCRAPE_YEAR_RANGE
		@debug "Scraping season $(season)"
		for (num,league) in enumerate(LEAGUES)
			url = replace(baseurl,"{NUM}"=>num,"{LEAGUE}"=>league,"{SEASON}"=>season)
			data = get_raw_data_from(url, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
			htmldata = parsehtml(data)
			
			for gamelink in eachmatch(sel"td.zentriert.hauptlink a",htmldata.root)
				lineupurl = replace(getattr(gamelink,"href"),"/index/"=>"/aufstellung/")
				if !startswith(lineupurl,"https")
					lineupurl = "https://www.transfermarkt.com/"*lstrip(lineupurl,'/')
				end
				lineuppage = get_raw_data_from(lineupurl, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
				lineuphtml = parsehtml(lineuppage)
				date_str = splitdir(getattr(Cascadia.matchFirst(sel"p.sb-datum > a:nth-of-type(2)",lineuphtml.root),"href"))[2]

				team_links = eachmatch(sel"a.sb-vereinslink",lineuphtml.root)
				temp = Dict{Int,Vector}()
				for (i,div) in zip(1:4,eachmatch(sel"div.large-6",lineuphtml.root))
					tr = Cascadia.matchFirst(sel"div.table-footer tr",div)
					if tr === nothing
						temp[i] = [missing for _ = 1:4]
					else
						foreigners_cell,avg_age_cell,purchase_cell,total_market_cell = eachmatch(sel"td",tr)
						foreigners = parse(Int,replace(foreigners_cell[1].text,"Foreigners: "=>"",r" \([\d\.%]+\)"=>""))
						avg_age = parse(Float64,replace(avg_age_cell[1].text,"Avg. age: "=>""))
						purchase_val = replace(purchase_cell[1].text,"Purchase value: "=>"")
						total_market_val = replace(total_market_cell[1].text,"Total MV: "=>"")
						temp[i] = [foreigners,avg_age,purchase_val,total_market_val]
					end
				end
				
				for (i,team_link) in enumerate(team_links)
					team_name = team_link[1].text
					tmkt_team_id = parse(Int,match(r"verein/(\d+)\b",getattr(team_link,"href")).captures[1])
					push!(df,vcat([tmkt_team_id,team_name,date_str],temp[i],temp[i+2]))
				end
			end
		end
	end

	return df
end

function clean_lineup_data!(df::DataFrame)
	transform!(df,:team_name=>ByRow(standardize_team_name),:starters_purchase_val=>ByRow(parse_market_val),:starters_total_market_val=>ByRow(parse_market_val),
		:bench_purchase_val=>ByRow(parse_market_val),:bench_total_market_val=>ByRow(parse_market_val),renamecols=false)

	# Date corrections
	df[(df.date .== typeof(df[1,:date])("2019-12-07")) .&& ((df.team_name .== "Millwall") .|| (df.team_name .== "Nottingham Forest")),:date] = (df[1,:date] isa Date) ? [Date(2019,12,6),Date(2019,12,6)] : ["2019-12-06","2019-12-06"]
	df[(df.date .== typeof(df[1,:date])("2021-12-18")) .&& ((df.team_name .== "Northampton Town") .|| (df.team_name .== "Barrow")),:date] = (df[1,:date] isa Date) ? [Date(2022,2,1),Date(2022,2,1)] : ["2022-02-01","2022-02-01"]
	df[(df.date .== typeof(df[1,:date])("2021-12-18")) .&& ((df.team_name .== "Port Vale") .|| (df.team_name .== "Exeter City")),:date] = (df[1,:date] isa Date) ? [Date(2022,3,22),Date(2022,3,22)] : ["2022-03-22","2022-03-22"]
	df[(df.date .== typeof(df[1,:date])("2022-04-30")) .&& ((df.team_name .== "Port Vale") .|| (df.team_name .== "Newport County")),:date] = (df[1,:date] isa Date) ? [Date(2022,5,2),Date(2022,5,2)] : ["2022-05-02","2022-05-02"]
	df[(df.date .== typeof(df[1,:date])("2023-01-28")) .&& ((df.team_name .== "Luton Town") .|| (df.team_name .== "Cardiff City")),:date] = (df[1,:date] isa Date) ? [Date(2023,1,31),Date(2023,1,31)] : ["2023-01-31","2023-01-31"]
	df[(df.date .== typeof(df[1,:date])("2022-10-01")) .&& ((df.team_name .== "Mansfield Town") .|| (df.team_name .== "Hartlepool United")),:date] = (df[1,:date] isa Date) ? [Date(2022,9,30),Date(2022,9,30)] : ["2022-09-30","2022-09-30"]
	df[(df.date .== typeof(df[1,:date])("2023-03-21")) .&& ((df.team_name .== "Mansfield Town") .|| (df.team_name .== "Grimsby Town")),:date] = (df[1,:date] isa Date) ? [Date(2023,3,22),Date(2023,3,22)] : ["2023-03-22","2023-03-22"]
	df[(df.date .== typeof(df[1,:date])("2023-09-09")) .&& ((df.team_name .== "Barrow") .|| (df.team_name .== "Morecambe")),:date] = (df[1,:date] isa Date) ? [Date(2023,10,31),Date(2023,10,31)] : ["2023-10-31","2023-10-31"]
	df[(df.date .== typeof(df[1,:date])("2024-01-06")) .&& ((df.team_name .== "Gillingham") .|| (df.team_name .== "Stockport County")),:date] = (df[1,:date] isa Date) ? [Date(2024,2,20),Date(2024,2,20)] : ["2024-02-20","2024-02-20"]
	
	# Fix the missing data in the Southend vs Stevenage
	df[(df.date .== typeof(df[1,:date])("2021-03-13")) .&& (df.team_name .== "Southend United"),Between(:starters_num_foreigners,:bench_total_market_val)] = [7 25.4 0 750000.0 1 25.0 0 0.0]
	df[(df.date .== typeof(df[1,:date])("2021-03-13")) .&& (df.team_name .== "Stevenage"),Between(:starters_num_foreigners,:bench_total_market_val)] = [4 26.5 0 1200000.0 1 25.9 0 500000.0]

	rows_to_delete = findall((df.date .== typeof(df[1,:date])("2019-04-27")) .&& ((df.team_name .== "Bolton Wanderers") .|| (df.team_name .== "Brentford")))
	@assert length(rows_to_delete)==2
	deleteat!(df,rows_to_delete)

	assert_zero_missing_values(df)
end

# Match data functions

function scrape_match_data(; check_web_cache::Bool=true, enable_web_cache::Bool=true)::DataFrame
	@info "Scraping match data"
	baseurl = "https://www.football-data.co.uk/mmz4281/{SEASON}/E{NUM}.csv"
	include_columns = collect(keys(MATCH_HEADERS_MAPPING))
	first_dump = true
	result_df = nothing

	for season = SCRAPE_YEAR_RANGE
		@debug "Scraping season $(season)"
		for i = 0:3
			url = replace(baseurl,"{SEASON}"=>string(season%100,pad=2)*string((season+1)%100,pad=2),"{NUM}"=>i)
			data = get_raw_data_from(url, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
			raw_df = CSV.read(IOBuffer(data),DataFrame)
			if :BbAvH in propertynames(raw_df)
				rename!(raw_df,:BbAvH=>:AvgH,:BbAvD=>:AvgD,:BbAvA=>:AvgA)
			end
			selected_df = select(raw_df,include_columns,copycols=false)
			dropmissing!(selected_df,[:HomeTeam,:AwayTeam])
			insertcols!(selected_df,1,:season=>season,:league=>LEAGUES[i+1],:league_num=>i+1)
			
			if first_dump
				result_df = selected_df
				first_dump = false
			else
				append!(result_df,selected_df,promote=true)
			end
		end
	end

	return result_df
end

function clean_match_data!(df::DataFrame)
	transform!(df,:Date=>ByRow(standardize_date),:HomeTeam=>ByRow(standardize_team_name),:AwayTeam=>ByRow(standardize_team_name),renamecols=false)

	rows_to_delete = findall((df.Date .== typeof(df[1,:Date])("2019-04-27")) .&& (df.HomeTeam .== "Bolton Wanderers") .&& (df.AwayTeam .== "Brentford"))
	@assert length(rows_to_delete)==1
	deleteat!(df,rows_to_delete)

	# Correcting two rows that are only missing the fouls data
	df[(df.Date .== typeof(df[1,:Date])("2017-04-22")) .&& (df.HomeTeam .== "Luton Town") .&& (df.AwayTeam .== "Notts County"),[:HF,:AF]] = [8 15]
	df[(df.Date .== typeof(df[1,:Date])("2017-04-29")) .&& (df.HomeTeam .== "Cheltenham Town") .&& (df.AwayTeam .== "Hartlepool United"),[:HF,:AF]] = [13 14]
	df[(df.Date .== typeof(df[1,:Date])("2019-08-31")) .&& (df.HomeTeam .== "Gillingham") .&& (df.AwayTeam .== "Bolton Wanderers"),[:AvgH,:AvgD,:AvgA]] = [1.12 6.5 15.5]
	df[(df.Date .== typeof(df[1,:Date])("2019-11-16")) .&& (df.HomeTeam .== "Macclesfield Town") .&& (df.AwayTeam .== "Mansfield Town"),[:AvgH,:AvgD,:AvgA]] = [3.75 3.35 1.93]
	df[(df.Date .== typeof(df[1,:Date])("2019-12-14")) .&& (df.HomeTeam .== "Walsall") .&& (df.AwayTeam .== "Macclesfield Town"),[:AvgH,:AvgD,:AvgA]] = [1.65 3.85 5.0]
	df[(df.Date .== typeof(df[1,:Date])("2019-12-26")) .&& (df.HomeTeam .== "Macclesfield Town") .&& (df.AwayTeam .== "Grimsby Town"),[:AvgH,:AvgD,:AvgA]] = [2.95 3.1 2.4]
	df[(df.Date .== typeof(df[1,:Date])("2020-01-01")) .&& (df.HomeTeam .== "Port Vale") .&& (df.AwayTeam .== "Macclesfield Town"),[:AvgH,:AvgD,:AvgA]] = [1.83 3.4 4.5]
	df[(df.Date .== typeof(df[1,:Date])("2022-11-08")) .&& (df.HomeTeam .== "Mansfield Town") .&& (df.AwayTeam .== "Bradford City"),[:AvgH,:AvgD,:AvgA]] = [1.95 3.5 3.7]
	
	rename!(df,MATCH_HEADERS_MAPPING)
	assert_zero_missing_values(df)
end


# Standings data functions

function scrape_standings_data(; check_web_cache::Bool=true, enable_web_cache::Bool=true)::DataFrame
	println(SCRAPE_DELAY_RANGE)
	println(SCRAPE_YEAR_RANGE)

	@info "Scraping standings data"
	baseurl = "https://www.espn.com/soccer/standings/_/league/ENG.{NUM}/season/{YEAR}"
	df = DataFrame(season=Int[],league=String15[],league_num=Int[],ranking=Int[],espn_team_id=Int[],team_name=String31[],games_played=Int[],wins=Int[],draws=Int[],losses=Int[],goals_for=Int[],goals_against=Int[],goal_diff=Int[],points=Int[])

	for season = SCRAPE_YEAR_RANGE
		@debug "Scraping season $(season)"
		for (num,league) in enumerate(LEAGUES)
			url = replace(baseurl,"{NUM}"=>num,"{YEAR}"=>season)
			data = get_raw_data_from(url, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
			htmldata = parsehtml(data)
			
			temp = Tuple{Int,String,Int}[]
			for row in eachmatch(sel"table.Table--fixed tbody tr",htmldata.root)
				rank_span,logo_span,shortname_span,fullname_span = eachmatch(sel"span",row)
				push!(temp,(parse(Int,rank_span[1].text),fullname_span[1][1].text,parse(Int,match(r"id/(\d+)\b",getattr(fullname_span[1],"href")).captures[1])))
			end
				
			for row in eachmatch(sel"div.Table__Scroller tbody tr",htmldata.root)
				idx = parse(Int,getattr(row,"data-idx"))
				gp,w,d,l,f,a,gd,p = map(span->parse(Int,span[1].text),eachmatch(sel"span",row))
				ranking,team_name,espn_team_id = temp[idx+1]
				push!(df,[season,league,num,ranking,espn_team_id,team_name,gp,w,d,l,f,a,gd,p])
			end
		end
	end

	return df
end

function clean_standings_data!(df::DataFrame)
	transform!(df,:team_name=>ByRow(standardize_team_name),renamecols=false)
	assert_zero_missing_values(df)
end


# Export to database function

function export_to_database(db::SQLite.DB, team_marketvalue_data, lineup_data, match_data, standings_data; csv_preview::Bool=false)
	# Create the league table (essentially an enum)
	league_table = DataFrame(id=collect(1:4),name=LEAGUES)

	# Create the team table from the team marketvalue data
	team_table = outerjoin(unique!(select(team_marketvalue_data,[:team_name,:transfermarkt_team_id])),unique!(select(standings_data,[:team_name,:espn_team_id])),on=:team_name,validate=true=>true)
	insertcols!(team_table,1,:id=>1:size(team_table,1))
	rename!(team_table,:team_name=>:name)

	# Create the team marketvalue table from the data
	team_name_to_id_dict = Dict([r.name=>r.id for r in eachrow(team_table)])
	team_marketvalue_table = select(team_marketvalue_data,:season,:league_num=>:league_id,:team_name=>ByRow(k->team_name_to_id_dict[k])=>:team_id,Not([:season,:league,:league_num,:transfermarkt_team_id,:team_name]))

	# Create the match table from the data
	match_table = select(match_data,:season,:league_num=>:league_id,:HomeTeam=>ByRow(k->team_name_to_id_dict[k])=>:home_team_id,:AwayTeam=>ByRow(k->team_name_to_id_dict[k])=>:away_team_id,
		:date=>ByRow(d->d isa Date ? date_obj_to_str(d) : d)=>:date,Not([:season,:league,:league_num,:date,:HomeTeam,:AwayTeam]))
	insertcols!(match_table,1,:id=>1:size(match_table,1))
	
	# Create the standings table
	standings_table = select(standings_data,:season,:league_num=>:league_id,:team_name=>ByRow(k->team_name_to_id_dict[k])=>:team_id,Not([:season,:league,:league_num,:espn_team_id,:team_name]))

	# Create the lineup table
	lineup_table = select(lineup_data,:date=>ByRow(d->d isa Date ? date_obj_to_str(d) : d)=>:date,:team_name=>ByRow(k->team_name_to_id_dict[k])=>:team_id,Not([:transfermarkt_team_id,:date,:team_name]))
	insertcols!(lineup_table,1,:match_id=>Union{Int,Missing}[missing for _ = 1:size(lineup_table,1)])
	for row in eachrow(lineup_table)
		row.match_id = match_table[(match_table.date .== row.date) .&& ((match_table.home_team_id .== row.team_id) .|| (match_table.away_team_id .== row.team_id)),:id][1]
	end
	select!(lineup_table,Not(:date))

	# Verify that the tables are not missing values
	foreach(assert_zero_missing_values,(league_table,team_table,team_marketvalue_table,match_table,lineup_table,standings_table))

	@assert 2*size(match_table,1)==size(lineup_table,1)

	if csv_preview
		CSV.write("csv_files/english_league_table_database.csv",league_table)
		CSV.write("csv_files/english_team_table_database.csv",team_table)
		CSV.write("csv_files/english_team_marketvalue_table_database.csv",team_marketvalue_table)
		CSV.write("csv_files/english_match_table_database.csv",match_table)
		CSV.write("csv_files/english_lineup_table_database.csv",lineup_table)
		CSV.write("csv_files/english_standings_table_database.csv",standings_table)
	end

	SQLite.load!(league_table,db,"Leagues",ifnotexists=false)
	SQLite.load!(team_table,db,"Teams",ifnotexists=false)
	SQLite.load!(team_marketvalue_table,db,"TeamMarketvalues",ifnotexists=false)
	SQLite.load!(match_table,db,"Matches",ifnotexists=false)
	SQLite.load!(lineup_table,db,"LineupMarketvalues",ifnotexists=false)
	SQLite.load!(standings_table,db,"EOSStandings",ifnotexists=false)
end

function export_to_database(filename::AbstractString, dfs::DataFrame...; csv_preview::Bool=false)
	if isfile(filename)
		rm(filename)
	end
	export_to_database(SQLite.DB(filename),dfs...,csv_preview=csv_preview)
end


# Runner script

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
	s = ArgParseSettings()
	@add_arg_table! s begin
		"--ignore-web-cache"
			help = "Do not check rawdata cache for webpages"
			action = :store_true
		"--disable-web-cache"
			help = "Do not save new pages to the rawdata cache"
			action = :store_true
		"--start-at-season"
			help = "Season to begin scraping from"
			arg_type = Int
			default = 2010
		"--end-at-season"
			help = "Season to end scraping at (inclusive)"
			arg_type = Int
			default = year(Dates.now())-1
		"--http-delay-min"
			help = "Minimum amount of time to wait after a web request is made (seconds)"
			arg_type = Float64
			default = 1
		"--http-delay-max"
			help = "Maximum amount of time to wait after a web request is made (seconds)"
			arg_type = Float64
			default = 5
		"--check-csv-cache"
			help = "Use csv_files cache if available (may not reflect changes to this code)"
			action = :store_true
		"--use-csv-cache"
			help = "Save intermediate CSV files to csv_files cache"
			action = :store_true
		"--csv-preview"
			help = "Save a copy of the database tables as CSV files (also sent to csv_files)"
			action = :store_true
		"--verbose"
			help = "Verbose output (turns on debug logging)"
			action = :store_true
	end

	parsed_args = parse_args(s)

	if parsed_args["verbose"]
		ENV["JULIA_DEBUG"] = Main
	end
	global SCRAPE_YEAR_RANGE = parsed_args["start-at-season"]:parsed_args["end-at-season"]
	m,M = minmax(parsed_args["http-delay-min"],parsed_args["http-delay-max"])
	global SCRAPE_DELAY_RANGE = m:.1:M

	function prepare_data(subject::String)::DataFrame
		clean_data_path = "csv_files/english_$(subject)_data_clean.csv"
		dirty_data_path = "csv_files/english_$(subject)_data_dirty.csv"
		if parsed_args["check-csv-cache"] && isfile(clean_data_path)
			data = CSV.read(clean_data_path,DataFrame)
		else
			if parsed_args["check-csv-cache"] && isfile(dirty_data_path)
				data = CSV.read(dirty_data_path,DataFrame)
			else
				data = eval(Symbol("scrape_",subject,"_data"))(check_web_cache=!parsed_args["ignore-web-cache"],enable_web_cache=!parsed_args["disable-web-cache"])
				if parsed_args["use-csv-cache"]
					CSV.write(dirty_data_path,data)
				end
			end
			eval(Symbol("clean_",subject,"_data!"))(data)
			if parsed_args["use-csv-cache"]
				CSV.write(clean_data_path,data)
			end
		end
		return data
	end
	
	team_marketvalue_data,lineup_data,match_data,standings_data = prepare_data.(["team_marketvalue","lineup","match","standings"])

	export_to_database("english_football_data.sqlite",team_marketvalue_data,lineup_data,match_data,standings_data,csv_preview=parsed_args["csv-preview"])
else
	@warn "This script was not meant to be imported. Proceed at your own risk!"
end
