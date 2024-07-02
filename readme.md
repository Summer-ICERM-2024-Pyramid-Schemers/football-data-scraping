# EFL-data-scraping

EFL-data-scraping is a project to scrape, clean, and compile data relating to the first 4 leagues in the English Football pyramid.

* fetch_english_football_data.jl is the main script and can handle all parts of the process.
* collect_lineup_pages.jl is a helper script that can be run on other machines to download the large amounts of lineup pages from Transfermarkt.

## Installation

1. To run this project, you need a recent version of Julia, preferably 1.10+
2. Clone this repository and cd into the folder
3. Open a Julia REPL in the current folder
4. Run the following:
`]activate .`
`instantiate`
5. If you wish to use an existing snapshot of the webpages instead of downloading them, extract a snapshot from the "rawdata_snapshots/" folder into a folder named "rawdata/".

## Usage

Run `julia fetch_english_football_data.jl --help` for more in-depth information on command line arguments that can be used.

* `julia fetch_english_football_data.jl`: run the process from almost scratch. By default, the script will attempt to read and write from the cache at "rawdata/"
* `julia fetch_english_football_data.jl --ignore-web-cache --disable-web-cache`: run the process from scratch. This will take a long time.

## TODO

* Improve CSV cache to automatically remove csv files that do not match the year span, league span, etc
* Update collect_lineup_pages to reflect the improvements made to the rest of the program.
    * Project setting
    * html script removal
    * ArgParse cmd args
