# football-data-scraping

football-data-scraping is a project to scrape, clean, and compile data relating to the first 4 leagues in the English Football pyramid and the first 2 German leagues.

* fetch_football_data.jl is the main script and can handle all parts of the process.
* collect_lineup_pages.jl is a helper script that can be run on other machines to download the large amounts of lineup pages from Transfermarkt.

## Installation

1. To run this project, you need a recent version of Julia, preferably 1.10+
2. Clone this repository and cd into the folder
3. Open a Julia REPL in the current folder and run the following:
```
]activate .
instantiate
```
4. If you wish to use an existing snapshot of the webpages instead of downloading them, install git-lfs (on Windows its `git lfs install`). Then download the large files; you may need to use the commands `git lfs fetch` or `git lfs checkout`. Then extract a snapshot from the "rawdata_snapshots/" folder into a folder named "rawdata/".

## Usage

Run `julia fetch_football_data.jl --help` for more in-depth information on command line arguments that can be used.

* `julia fetch_football_data.jl`: run the process from almost scratch. By default, the script will attempt to read and write from the cache at "rawdata/"
* `julia fetch_football_data.jl --ignore-web-cache --disable-web-cache`: run the process from scratch. This will take a long time.

## TODO

* Improve CSV cache to automatically remove csv files that do not match the year span, league span, etc
