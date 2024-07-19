# football-data-scraping

football-data-scraping is a project to scrape, clean, and compile data relating to the first 4 leagues in the English Football pyramid, the first 2 German leagues, and the top 4 Scottish leagues.
fetch_football_data.jl is the main script and can handle all parts of the process. It now also has a command line option that ignores the scraping and cleaning process and just works on downloading pages, which is useful for running on a raspberry pi.

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
* `julia fetch_football_data.jl --ignore-web-cache --disable-web-cache`: run the process from scratch. This will take a *long* time
* `julia fetch_football_data.jl --use-csv-cache`: run the process and create csv files after the scraping and cleaning steps. The csv files can be used to hunt down issues and to speed up the process next time
* `julia fetch_football_data.jl --check-csv-cache`: look for intermediate csv files and use them if found. This can be used to skip a very long scraping process and skip to the cleaning or exporting step
