using Cascadia, Gumbo
import HTTP
import MD5.md5

if !isdir("rawdata")
	mkdir("rawdata")
end

const BROWSER_AGENT::String = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
const LEAGUES::Vector{String} = ["premier-league","championship","league-one","league-two"]
const SCRAPE_YEAR_RANGE = 2010:2023
const SCRAPE_DELAY_RANGE = 1:.1:3

function get_raw_data_from(url::String; check_cache::Bool=true, cache_result::Bool=true)::String
	#TODO pass these options to the scraper funcs
	path = joinpath("rawdata",bytes2hex(md5(url)))
	# println("getting $(url), locally should be $(path)")
	if !isfile(path) || !check_cache
		response = HTTP.get(url,headers=Dict("User-Agent"=>BROWSER_AGENT))
		if response.status != 200
			error(response)
		end
		data = String(response.body)
		if !endswith(url,".csv")
			data = remove_excess_html(data)
		end
		sleep(rand(SCRAPE_DELAY_RANGE))
		if cache_result
			write(path,data)
		end
		return data
	else
		data = read(path,String)
		if data == "" || occursin('\0',data)
			@warn "$(path) is empty/corrupted! Attempting to refetch"
			return get_raw_data_from(url, check_cache=false, cache_result=true)
		else
			return data
		end
	end
end

function remove_excess_html(str::String)::String
	str = replace(str,r"<script[^>]*>(?:.|\n)*?</script>"=>"")
	str = replace(str,r"\s+"=>" ")
	return str
end

function fetch_lineup_data()
	println("Scraping lineup data")
	baseurl = "https://www.transfermarkt.com/{LEAGUE}/gesamtspielplan/wettbewerb/GB{NUM}/saison_id/{SEASON}"

    for season = SCRAPE_YEAR_RANGE
        println("Scraping season $(season)")
        for (num,league) in enumerate(LEAGUES)
            url = replace(baseurl,"{NUM}"=>num,"{LEAGUE}"=>league,"{SEASON}"=>season)
			data = get_raw_data_from(url)
			htmldata = parsehtml(data)
			
			for gamelink in eachmatch(sel"a.ergebnis-link",htmldata.root)
				lineupurl = replace(getattr(gamelink,"href"),"/index/"=>"/aufstellung/")
				if !startswith(lineupurl,"https")
					lineupurl = "https://www.transfermarkt.com/"*lstrip(lineupurl,'/')
				end
				lineuppage = get_raw_data_from(lineupurl)
			end
		end
	end
end


# Runner script

if abspath(PROGRAM_FILE) == abspath(@__FILE__)
	fetch_lineup_data()
end
