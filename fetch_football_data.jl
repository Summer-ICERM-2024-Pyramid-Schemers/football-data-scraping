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
# Order is league_num, league_name, tmkt_league_id, fdcu_league_id, espn_league_id, (team_marketvalue years, lineup years, match years, standings years)
# By years (the last 4 columns, I mean which years are valid years to scrape the website in question)
# The leagues that get put into the database should come first monotonically. If you add new leagues, you must follow the patterns and then modify the slice that occurs in the beginning of export_to_database()
const LEAGUES::Matrix{Any} = [1 "premier-league" "GB1" "E0" "ENG.1" (1992:2024, 1992:2024, 2000:2024, 2003:2024);
								2 "championship" "GB2" "E1" "ENG.2" (2004:2024, 2004:2024, 2000:2024, 2003:2024);
								3 "league-one" "GB3" "E2" "ENG.3" (2004:2024, 2004:2024, 2000:2024, 2003:2024);
								4 "league-two" "GB4" "E3" "ENG.4" (2004:2024, 2004:2024, 2000:2024, 2003:2024);
								5 "bundesliga" "L1" "D1" "GER.1" (1963:2024, 1963:2024, 2000:2024, 2006:2024);
								6 "2-bundesliga" "L2" "D2" "GER.2" (1981:2024, 1981:2024, 2000:2024, 2006:2024);
								7 "scottish-premiership" "SC1" "SC0" "SCO.1" (1997:2024, 1997:2024, 2000:2024, 2003:2024);
								8 "scottish-championship" "SC2" "SC1" "SCO.2" (1998:2024, 1998:2024, 2000:2024, 2003:2024);
								9 "scottish-league-one" "SC3" "SC2" "SCO.3" (2000:2024, 2000:2024, 2000:2024, 2003:2024);
								10 "scottish-league-two" "SC4" "SC3" "SCO.4" (2003:2024, 2003:2024, 2000:2024, 2003:2024);
								7 "scottish-premiership-top-six-split" "SCPM" "" "" (1:0, vcat([2002],collect(2017:2024)), 1:0, 1:0);
								7 "scottish-premiership-relegation-round" "SCPA" "" "" (1:0, vcat([2002],collect(2017:2024)), 1:0, 1:0);
								9 "scottish-league-one-meisterrunde" "SC3M" "" "" (1:0, 2020:2020, 1:0, 1:0);
								9 "scottish-league-one-abstiegsrunde" "SC3A" "" "" (1:0, 2020:2020, 1:0, 1:0);
								10 "scottish-league-two-meisterrunde" "SC4M" "" "" (1:0, 2020:2020, 1:0, 1:0);
								10 "scottish-league-two-abstiegsrunde" "SC4A" "" "" (1:0, 2020:2020, 1:0, 1:0)]
const MATCH_HEADERS_MAPPING::OrderedDict{Symbol,Symbol} = OrderedDict(:Date=>:date,:HomeTeam=>:HomeTeam,:AwayTeam=>:AwayTeam,:FTHG=>:fulltime_home_goals,
	:FTAG=>:fulltime_away_goals,:FTR=>:fulltime_result,:HTHG=>:halftime_home_goals,:HTAG=>:halftime_away_goals,:HTR=>:halftime_result,
	:AvgH=>:market_average_home_win_odds,:AvgD=>:market_average_draw_odds,:AvgA=>:market_average_away_win_odds)
const TEAM_ALIAS_DICT::Dict{String,String} = JSON.parsefile("team_alias_dict.json")
TEAM_ALIAS_DICT_MODIFIED::Bool = false
SCRAPE_YEAR_RANGE = 2010:2023
SCRAPE_DELAY_RANGE = 1:.1:3

function get_raw_data_from(url::String; check_web_cache::Bool=true, enable_web_cache::Bool=true)::String
	path = joinpath("rawdata",bytes2hex(md5(url)))
	@debug "getting $(url), locally should be $(path)"
	if !isfile(path) || !check_web_cache
		@debug "fetching from web"
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

function save_alias_dict(;force::Bool=false)
	if TEAM_ALIAS_DICT_MODIFIED || force
		@debug "Saving the team alias dict"
		write("team_alias_dict.json",JSON.json(TEAM_ALIAS_DICT,4))
		global TEAM_ALIAS_DICT_MODIFIED = false
	end
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
		global TEAM_ALIAS_DICT_MODIFIED = true
		println("Name \"$(team_name)\" not found!")
		menu = TerminalMenus.RadioMenu(vcat(["(This is the standardized name)"],sort!(unique(values(TEAM_ALIAS_DICT)),by=s->StringDistances.RatcliffObershelp()(team_name,s)),["(None of the above)"]))
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

function assert_zero_missing_values(df::DataFrame, cols=All())
	complete_rows = completecases(df,cols)
	if !all(complete_rows)
		println("These rows have missing values!")
		println(df[.!complete_rows,All()])
		error("You have missing values!")
	end
end


# Market data functions

function parse_market_val(str::AbstractString)::Union{Float64,Missing}
	# Using list of pairs to ensure that the larger suffixes are tested first
	if str == "-"
		# If there is a hyphen, just assume that it means zero instead of missing for now
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

function scrape_team_marketvalue_data(; check_web_cache::Bool=true, enable_web_cache::Bool=true, download_only::Bool=false)::DataFrame
	@info "Scraping team marketvalue data"
	baseurl = "https://www.transfermarkt.us/{LEAGUE}/startseite/wettbewerb/{LEAGUE_ID}/plus/?saison_id={SEASON}"

	df = DataFrame(season=Int[],league_name=String31[],league_num=Int[],tmkt_team_id=Int[],team_name=String31[],squad_size=Int[],avg_age=Float64[],num_foreigners=Int[],avg_market_val=String15[],total_market_val=String15[])
	allowmissing!(df,r"market_val")

	for season = SCRAPE_YEAR_RANGE
		@debug "Scraping season $(season)"
		for (league_num,league_name,tmkt_league_id,fdcu_league_id,espn_league_id,(valid_years,_,_,_)) in eachrow(LEAGUES)
			if !in(season,valid_years)
				continue
			end
			url = replace(baseurl,"{LEAGUE_ID}"=>tmkt_league_id,"{LEAGUE}"=>league_name,"{SEASON}"=>season)
			data = get_raw_data_from(url, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
			if download_only
				continue
			end
			htmldata = parsehtml(data)
			
			for row in eachmatch(sel"div#yw1 table.items tbody tr",htmldata.root)
				temp = eachmatch(sel"td",row)
				if length(temp) == 7
					_,name_cell,squad_cell,avg_age_cell,foreigners_cell,avg_market_cell,total_market_cell = temp
					avg_market_val = avg_market_cell[1].text
					total_market_val = total_market_cell[1][1].text
				else
					_,name_cell,squad_cell,avg_age_cell,foreigners_cell = temp
					avg_market_val = total_market_val = missing
				end
				team_id = parse(Int,match(r"verein/(\d+)\b",getattr(name_cell[1],"href")).captures[1])
				team_name = name_cell[1][1].text
				squad_size = parse(Int,squad_cell[1][1].text)
				avg_age = parse(Float64,avg_age_cell[1].text)
				foreigners = parse(Int,foreigners_cell[1].text)
				push!(df,[season,league_name,league_num,team_id,team_name,squad_size,avg_age,foreigners,avg_market_val,total_market_val])
			end
		end
	end

	return df
end

function clean_team_marketvalue_data!(df::DataFrame)
	transform!(df,:avg_market_val=>ByRow(parse_market_val),:total_market_val=>ByRow(parse_market_val),:team_name=>ByRow(standardize_team_name),renamecols=false)
	save_alias_dict()
	assert_zero_missing_values(df,Not([:avg_market_val,:total_market_val]))
end

function scrape_lineup_data(; check_web_cache::Bool=true, enable_web_cache::Bool=true, download_only::Bool=false)::DataFrame
	@info "Scraping lineup data"
	baseurl1 = "https://www.transfermarkt.com/{LEAGUE}/gesamtspielplan/wettbewerb/{LEAGUE_ID}/saison_id/{SEASON}"
	baseurl2 = "https://www.transfermarkt.com/{LEAGUE}/spieltag/wettbewerb/{LEAGUE_ID}/saison_id/{SEASON}"

	df = DataFrame(tmkt_team_id=Int[],team_name=String31[],date=String15[],starters_num_foreigners=Int[],starters_avg_age=Float64[],starters_purchase_val=String15[],
		starters_total_market_val=String15[],bench_num_foreigners=Int[],bench_avg_age=Float64[],bench_purchase_val=String15[],bench_total_market_val=String15[],bench_size=Int[])
	allowmissing!(df,r"starters|bench")
	scraped_game_ids = Set{Int32}()
	
	for season = SCRAPE_YEAR_RANGE
		@debug "Scraping season $(season)"
		for (league_num,league_name,tmkt_league_id,fdcu_league_id,espn_league_id,(_,valid_years,_,_)) in eachrow(LEAGUES)
			if !in(season,valid_years)
				continue
			end
			url1 = replace(baseurl1,"{LEAGUE_ID}"=>tmkt_league_id,"{LEAGUE}"=>league_name,"{SEASON}"=>season)
			data1 = get_raw_data_from(url1, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
			htmldata1 = parsehtml(data1)
			
			url2 = replace(baseurl2,"{LEAGUE_ID}"=>tmkt_league_id,"{LEAGUE}"=>league_name,"{SEASON}"=>season)
			data2 = get_raw_data_from(url2, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
			htmldata2 = parsehtml(data2)
			
			for gamelink in vcat(eachmatch(sel"td.zentriert.hauptlink a",htmldata1.root),eachmatch(sel"td.spieltagsansicht-ergebnis span a",htmldata2.root))
				lineupurl = replace(getattr(gamelink,"href"),"/index/"=>"/aufstellung/")
				val = parse(Int32,match(r"(\d+)$",lineupurl).captures[1])
				if val in scraped_game_ids
					continue
				else
					push!(scraped_game_ids,val)
				end

				if !startswith(lineupurl,"https")
					lineupurl = "https://www.transfermarkt.com/"*lstrip(lineupurl,'/')
				end
				lineuppage = get_raw_data_from(lineupurl, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
				if download_only
					continue
				end
				
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
					if i in (3,4)
						push!(temp[i],tr === nothing ? missing : length(eachmatch(sel"div.responsive-table > table > tbody > tr",div)))
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
	save_alias_dict()

	# Date corrections
	date_corrections = ["2010-02-01" "Airdrieonians" "Dumbarton" "2011-02-01";
						"2010-12-07" "Peterhead" "Airdrieonians" "2010-12-14";
						"2011-01-18" "Karlsruher" "Greuther Fürth" "2011-01-14";
						"2011-05-07" "Kilmarnock" "Celtic" "2011-05-08";
						"2011-08-06" "Aberdeen" "Celtic" "2011-08-07";
						"2015-01-31" "Partick Thistle" "St. Mirren" "2015-01-30";
						"2015-11-14" "Hibernian" "Livingston" "2015-11-17";
						"2019-03-23" "Queen of the South" "Falkirk" "2019-04-02";
						"2019-12-07" "Millwall" "Nottingham Forest" "2019-12-06";
						"2020-08-08" "Rangers" "St. Mirren" "2020-08-09";
						"2021-12-18" "Northampton Town" "Barrow" "2022-02-01";
						"2021-12-18" "Port Vale" "Exeter City" "2022-03-22";
						"2022-04-30" "Port Vale" "Newport County" "2022-05-02";
						"2023-01-28" "Luton Town" "Cardiff City" "2023-01-31";
						"2022-10-01" "Mansfield Town" "Hartlepool United" "2022-09-30";
						"2023-01-07" "Dundee United" "Rangers" "2023-01-08";
						"2023-03-18" "Arbroath" "Greenock Morton" "2023-03-17";
						"2023-03-21" "Mansfield Town" "Grimsby Town" "2023-03-22";
						"2023-03-25" "Inverness Caledonian Thistle" "Partick Thistle" "2023-03-24";
						"2023-04-01" "Arbroath" "Ayr United" "2023-03-31";
						"2023-04-08" "Dundee United" "Hibernian" "2023-04-09";
						"2023-04-22" "Ayr United" "Queen's Park" "2023-04-21";
						"2023-09-09" "Barrow" "Morecambe" "2023-10-31";
						"2023-09-23" "Rangers" "Motherwell" "2023-09-24";
						"2023-09-23" "Aberdeen" "Ross County" "2023-09-24";
						"2023-10-07" "St. Mirren" "Rangers" "2023-10-08";
						"2023-10-07" "Dundee" "Ross County" "2023-10-24";
						"2023-10-07" "Aberdeen" "St. Johnstone" "2023-10-08";
						"2024-01-06" "Gillingham" "Stockport County" "2024-02-20"]
	for (baddate_str,home_team_name,away_team_name,newdate_str) in eachrow(date_corrections)
		df[(df.date .== typeof(df[1,:date])(baddate_str)) .&& ((df.team_name .== home_team_name) .|| (df.team_name .== away_team_name)),:date] = repeat([typeof(df[1,:date])(newdate_str)],2)
	end

	# Fix the missing data in the Southend vs Stevenage
	df[(df.date .== typeof(df[1,:date])("2021-03-13")) .&& (df.team_name .== "Southend United"),Between(:starters_num_foreigners,:bench_size)] = [7 25.4 0 750000.0 1 25.0 0 0.0 7]
	df[(df.date .== typeof(df[1,:date])("2021-03-13")) .&& (df.team_name .== "Stevenage"),Between(:starters_num_foreigners,:bench_size)] = [4 26.5 0 1200000.0 1 25.9 0 500000.0 7]
	# Fix the missing data in the Berwick Rangers vs Stenhousemuir
	df[(df.date .== typeof(df[1,:date])("2018-04-24")) .&& (df.team_name .== "Berwick Rangers"),Between(:starters_num_foreigners,:bench_size)] = [0 23.4 0 0 0 23.9 0 0 7]
	df[(df.date .== typeof(df[1,:date])("2018-04-24")) .&& (df.team_name .== "Stenhousemuir"),Between(:starters_num_foreigners,:bench_size)] = [1 26.5 0 0 0 24.8 0 0 4]

	rows_to_delete = findall((df.date .== typeof(df[1,:date])("2019-04-27")) .&& ((df.team_name .== "Bolton Wanderers") .|| (df.team_name .== "Brentford")))
	@assert length(rows_to_delete)==2
	deleteat!(df,rows_to_delete)

	assert_zero_missing_values(df)
end


# Match data functions

function scrape_match_data(; check_web_cache::Bool=true, enable_web_cache::Bool=true, download_only::Bool=false)::DataFrame
	@info "Scraping match data"
	baseurl = "https://www.football-data.co.uk/mmz4281/{SEASON}/{LEAGUE_ID}.csv"
	include_columns = collect(keys(MATCH_HEADERS_MAPPING))
	first_dump = true
	result_df = nothing

	for season = SCRAPE_YEAR_RANGE
		@debug "Scraping season $(season)"
		for (league_num,league_name,tmkt_league_id,fdcu_league_id,espn_league_id,(_,_,valid_years,_)) in eachrow(LEAGUES)
			if !in(season,valid_years)
				continue
			end
			url = replace(baseurl,"{SEASON}"=>string(season%100,pad=2)*string((season+1)%100,pad=2),"{LEAGUE_ID}"=>fdcu_league_id)
			data = get_raw_data_from(url, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
			if download_only
				continue
			end
			raw_df = CSV.read(IOBuffer(data),DataFrame)
			if :BbAvH in propertynames(raw_df)
				rename!(raw_df,:BbAvH=>:AvgH,:BbAvD=>:AvgD,:BbAvA=>:AvgA)
			end
			selected_df = select(raw_df,include_columns,copycols=false)
			dropmissing!(selected_df,[:HomeTeam,:AwayTeam])
			insertcols!(selected_df,1,:season=>season,:league_name=>league_name,:league_num=>league_num)
			
			if first_dump
				result_df = selected_df
				first_dump = false
			else
				append!(result_df,selected_df,promote=true)
			end
		end
	end
	if result_df === nothing
		result_df = DataFrame()
	end

	return result_df
end

function clean_match_data!(df::DataFrame)
	transform!(df,:Date=>ByRow(standardize_date),:HomeTeam=>ByRow(standardize_team_name),:AwayTeam=>ByRow(standardize_team_name),renamecols=false)
	save_alias_dict()

	rows_to_delete = findall((df.Date .== typeof(df[1,:Date])("2019-04-27")) .&& (df.HomeTeam .== "Bolton Wanderers") .&& (df.AwayTeam .== "Brentford"))
	@assert length(rows_to_delete)==1
	deleteat!(df,rows_to_delete)

	# Correcting two rows that are only missing the fouls data
	if :HF in propertynames(df)
		df[(df.Date .== typeof(df[1,:Date])("2017-04-22")) .&& (df.HomeTeam .== "Luton Town") .&& (df.AwayTeam .== "Notts County"),[:HF,:AF]] = [8 15]
		df[(df.Date .== typeof(df[1,:Date])("2017-04-29")) .&& (df.HomeTeam .== "Cheltenham Town") .&& (df.AwayTeam .== "Hartlepool United"),[:HF,:AF]] = [13 14]
	end
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

function scrape_standings_data(; check_web_cache::Bool=true, enable_web_cache::Bool=true, download_only::Bool=false)::DataFrame
	@info "Scraping standings data"
	baseurl = "https://www.espn.com/soccer/standings/_/league/{LEAGUE_ID}/season/{YEAR}"
	df = DataFrame(season=Int[],league_name=String31[],league_num=Int[],ranking=Int[],espn_team_id=Int[],team_name=String31[],games_played=Int[],wins=Int[],draws=Int[],losses=Int[],goals_for=Int[],goals_against=Int[],goal_diff=Int[],points=Int[])

	for season = SCRAPE_YEAR_RANGE
		@debug "Scraping season $(season)"
		for (league_num,league_name,tmkt_league_id,fdcu_league_id,espn_league_id,(_,_,_,valid_years)) in eachrow(LEAGUES)
			if !in(season,valid_years)
				continue
			end
			url = replace(baseurl,"{LEAGUE_ID}"=>espn_league_id,"{YEAR}"=>season)
			data = get_raw_data_from(url, check_web_cache=check_web_cache, enable_web_cache=enable_web_cache)
			if download_only
				continue
			end
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
				push!(df,[season,league_name,league_num,ranking,espn_team_id,team_name,gp,w,d,l,f,a,gd,p])
			end
		end
	end

	return df
end

function clean_standings_data!(df::DataFrame)
	transform!(df,:team_name=>ByRow(standardize_team_name),renamecols=false)
	save_alias_dict()
	assert_zero_missing_values(df)
end


# Export to database function

function export_to_database(db::SQLite.DB, team_marketvalue_data, lineup_data, match_data, standings_data; csv_preview::Bool=false)
	# Create the league table (essentially an enum)
	league_table = DataFrame(LEAGUES[1:10,1:5],[:id,:league_name,:tmkt_league_id,:fdcu_league_id,:espn_league_id])

	# Create the team table from the team marketvalue data
	team_table = outerjoin(unique!(select(team_marketvalue_data,[:team_name,:tmkt_team_id])),unique!(select(standings_data,[:team_name,:espn_team_id])),on=:team_name,validate=true=>true)
	insertcols!(team_table,1,:id=>1:size(team_table,1))

	# Create the team marketvalue table from the data
	team_name_to_id_dict = Dict([r.team_name=>r.id for r in eachrow(team_table)])
	team_marketvalue_table = select(team_marketvalue_data,:season,:league_num=>:league_id,:team_name=>ByRow(k->team_name_to_id_dict[k])=>:team_id,Not([:season,:league_name,:league_num,:tmkt_team_id,:team_name]))

	# Create the match table from the data
	match_table = select(match_data,:season,:league_num=>:league_id,:HomeTeam=>ByRow(k->team_name_to_id_dict[k])=>:home_team_id,:AwayTeam=>ByRow(k->team_name_to_id_dict[k])=>:away_team_id,
		:date=>ByRow(d->d isa Date ? date_obj_to_str(d) : d)=>:date,Not([:season,:league_name,:league_num,:date,:HomeTeam,:AwayTeam]))
	insertcols!(match_table,1,:id=>1:size(match_table,1))
	
	# Create the standings table
	standings_table = select(standings_data,:season,:league_num=>:league_id,:team_name=>ByRow(k->team_name_to_id_dict[k])=>:team_id,Not([:season,:league_name,:league_num,:espn_team_id,:team_name]))

	# Create the lineup table
	lineup_table = select(lineup_data,:date=>ByRow(d->d isa Date ? date_obj_to_str(d) : d)=>:date,:team_name=>ByRow(k->team_name_to_id_dict[k])=>:team_id,Not([:tmkt_team_id,:date,:team_name]))
	insertcols!(lineup_table,1,:match_id=>Union{Int,Missing}[missing for _ = 1:size(lineup_table,1)])
	grouped_match_table = groupby(match_table,:date)
	for row in eachrow(lineup_table)
		subtable = grouped_match_table[(date=row.date,)]
		row.match_id = subtable[(subtable.home_team_id .== row.team_id) .|| (subtable.away_team_id .== row.team_id),:id][1]
	end
	select!(lineup_table,Not(:date))

	# Verify that the tables are not missing values
	assert_zero_missing_values(league_table)
	assert_zero_missing_values(team_table)
	assert_zero_missing_values(team_marketvalue_table,Not([:avg_market_val,:total_market_val]))
	assert_zero_missing_values(match_table)
	assert_zero_missing_values(lineup_table)
	assert_zero_missing_values(standings_table)

	if 2*size(match_table,1)!=size(lineup_table,1)
		@error "Number of lineups and games do not match!" 2*size(match_table,1) size(lineup_table,1)
		error("These games are missing lineups!:\n$(match_table[setdiff(match_table.id,lineup_table.match_id),:])\n$(team_table)")
	end

	if csv_preview
		CSV.write("csv_files/football_league_table_database.csv",league_table)
		CSV.write("csv_files/football_team_table_database.csv",team_table)
		CSV.write("csv_files/football_team_marketvalue_table_database.csv",team_marketvalue_table)
		CSV.write("csv_files/football_match_table_database.csv",match_table)
		CSV.write("csv_files/football_lineup_table_database.csv",lineup_table)
		CSV.write("csv_files/football_standings_table_database.csv",standings_table)
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
			default = first(SCRAPE_YEAR_RANGE)
		"--end-at-season"
			help = "Season to end scraping at (inclusive)"
			arg_type = Int
			default = last(SCRAPE_YEAR_RANGE)
		"--http-delay-min"
			help = "Minimum amount of time to wait after a web request is made (seconds)"
			arg_type = Float64
			default = first(SCRAPE_DELAY_RANGE)
		"--http-delay-max"
			help = "Maximum amount of time to wait after a web request is made (seconds)"
			arg_type = Float64
			default = last(SCRAPE_DELAY_RANGE)
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
		"--download-only"
			help = "Only attempt to download the webpages, nothing else"
			action = :store_true
	end

	parsed_args = parse_args(s)

	if parsed_args["verbose"]
		ENV["JULIA_DEBUG"] = Main
	end
	if parsed_args["download-only"]
		parsed_args["disable-web-cache"] = parsed_args["check-csv-cache"] = parsed_args["use-csv-cache"] = false
	end
	global SCRAPE_YEAR_RANGE = parsed_args["start-at-season"]:parsed_args["end-at-season"]
	m,M = minmax(parsed_args["http-delay-min"],parsed_args["http-delay-max"])
	global SCRAPE_DELAY_RANGE = m:.1:M

	function prepare_data(subject::String)::DataFrame
		clean_data_path = "csv_files/football_$(subject)_data_clean.csv"
		dirty_data_path = "csv_files/football_$(subject)_data_dirty.csv"
		if parsed_args["check-csv-cache"] && isfile(clean_data_path)
			data = CSV.read(clean_data_path,DataFrame)
		else
			if parsed_args["check-csv-cache"] && isfile(dirty_data_path)
				data = CSV.read(dirty_data_path,DataFrame)
			else
				data = eval(Symbol("scrape_",subject,"_data"))(check_web_cache=!parsed_args["ignore-web-cache"],
					enable_web_cache=!parsed_args["disable-web-cache"],download_only=parsed_args["download-only"])
				if parsed_args["use-csv-cache"]
					CSV.write(dirty_data_path,data)
				end
			end
			if !parsed_args["download-only"]
				eval(Symbol("clean_",subject,"_data!"))(data)
				if parsed_args["use-csv-cache"]
					CSV.write(clean_data_path,data)
				end
			end
		end
		return data
	end
	
	team_marketvalue_data,lineup_data,match_data,standings_data = prepare_data.(["team_marketvalue","lineup","match","standings"])
	if !parsed_args["download-only"]
		export_to_database("football_data.sqlite",team_marketvalue_data,lineup_data,match_data,standings_data,csv_preview=parsed_args["csv-preview"])
	end
else
	@warn "This script was not meant to be imported. Proceed at your own risk!"
end
