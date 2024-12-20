# football-data-scraping

football-data-scraping is a project to scrape, clean, and compile data relating to the top 4 leagues in the English Football pyramid, the top 2 German leagues, and the top 4 Scottish leagues. The data is pulled from ESPN, football-data.co.uk, and Transfermarkt.

fetch_football_data.jl is the main script and can handle all parts of the process. It now has a command line option that ignores the scraping and cleaning process and just works on downloading pages, which is useful for running on a raspberry pi.

## Installation

1. To run this project, you need a recent version of Julia, preferably 1.10+.
2. Clone this repository and cd into the folder.
3. Open a Julia REPL in the current folder and run the following:
```
]activate .
instantiate
```
4. If you wish to use an existing snapshot of the webpages instead of downloading them, extract a snapshot from the "rawdata_snapshots/" folder into a folder named "rawdata/". This may require installing git-lfs.

## Usage

Run `julia fetch_football_data.jl --help` for more in-depth information on command line arguments that can be used.

Passing command line flags to "fetch_football_data.jl" changes what the script does or what it outputs. Here are a few examples:
* `julia fetch_football_data.jl`: run the process from almost scratch. By default, the script will attempt to read and write from the web cache at "rawdata/".
* `julia fetch_football_data.jl --ignore-web-cache --disable-web-cache`: run the process from scratch. This will involve fetching all pages from the web, which will take a *long* time.
* `julia fetch_football_data.jl --use-csv-cache --verbose`: run the process and create csv files after the scraping and cleaning phases. The csv files can be used to locate issues and to speed up the process next time. Passing the verbose flag will make the program print tons of debug information, which is helpful for finding issues in the scraping process.
* `julia fetch_football_data.jl --check-csv-cache`: look for intermediate csv files and use them if found. This can be used to skip a very long scraping process and skip to the cleaning or exporting phase.

## General developer notes

### Program constants and globals

- `HTTP_REQUEST_HEADERS`: Headers to pass to the HTTP request. This is used to reduce the chance of the scraper getting blocked.
- `LEAGUES`: A matrix of data that determines how the scraper works. The reason this is so complicated is because of Transfermarkt and Scotland. By column...
    1. The first column is the league id. League ids must go from 1,2,...,N, which must reside in the first N rows of the matrix. Remaining rows should reuse one of the league ids above, which will insert the data from this new "league" into the original league (this is what we must do for the Scottish data).
    2. The second column is the name of the league (according to Transfermarkt).
    3. The third column is the Transfermarkt league id.
    4. The fourth column is the football-data.co.uk league id.
    5. The fifth column is the ESPN league id.
    6. The last column is a tuple of iterables with one item for each subject. Each item in the tuple specifies the years that this league can be scraped for that subject. Currently, the list of subjects is (team_marketvalue, lineup, match, standings).
- `MATCH_HEADERS_MAPPING`: Which columns to select from the match csv files and what each column should be renamed to.
- `TEAM_ALIAS_DICT`: Dict of team names to their standardized form. If `s`=>`s` exists in the dictionary, then `s` is a standardized team name. While the Dict reference cannot be changed, the keys and values can. If a team name doesn't exist, the program will block and prompt the user to resolve the issue.
- `TEAM_ALIAS_DICT_MODIFIED`: Global flag that stores whether `TEAM_ALIAS_DICT` has been changed (by the user prompt). Calling `save_alias_dict` will save the dictionary if this flag is set (and then reset it).
- `SCRAPE_YEAR_RANGE`: Default range of years to scrape. It can be overridden using the command line arguments.
- `SCRAPE_DELAY_RANGE`: Default range of time to wait after making a web request. It can be overridden using command line arguments.

### Function names

The function `prepare_data(::String)::DataFrame` is defined as part of the runner script.
`prepare_data` assumes that the scraping function for some `subject` is `scrape_$(subject)_data` and the cleaning function is named `clean_$(subject)_data!`.
If you wish to extent this program and scrape more subjects, make sure your function names follow this pattern.

### Web requests and rawdata cache

Scraping all this data requires scraping tens of thousands of webpages.
To reduce the chance of getting rate limited and in order to be respectful, the program waits some time after a request is made.
However, this means that scraping will take hours to days to complete.
Instead, the program will save the responses to the rawdata folder so that future scraping attempts can save time by opening the files locally.
We are aware that opening thousands of files is not a perfect solution, but restructuring the existing cache into another database or concatenating them seems to be more trouble than it's worth.

Each file in the rawdata folder is a response from the web. If the response was not a csv file, the program assumes it is a webpage and attempts to strip all "script" tags from the page to reduce file size (this occurs before saving the file).
The name of each file is the md5 hash of the url it was located at. We could have made a subfolder for each site and organized the pages that way; We chose the md5 approach because it was simpler to use in code and did not require further planning.
If the program crashes in the scraping phase, turn on verbose output and look for which page caused the crash. Verbose output will list which url it attempted to grab (and its md5 hash).


