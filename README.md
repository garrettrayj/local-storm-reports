Storm Reports
=============

Utilities for downloading and preprocessing storm reports curated by [NOAA's National Weather Service Storm Prediction Center](https://www.spc.noaa.gov/climo/reports/today.html). Pre-processing applies some basic quality control rules, formats data for easier use with applications, and aggregates daily data into a single CSV.

Etc/GMT+12, what I like to call the "convective timezone", is used for report times during collection. The effect is that reports covering storms' entire cycles are usually (but not always) neatly binned into single dates. Not due to the offset, but rather the usual burdens of data collection I imagine, minute accuracy is a stretch. Additional quality control should be performed when high time accuracy is a requirement.

Usage
-----

    git clone git@github.com:garrettrayj/storm-reports.git
    cd local-storm-reports
    ./lsrtool.py download
    ./lsrtool.py preprocess
    
Development Status
------------------

Currently, this script only handles hail reports, and not wind or tornado reports. I'll probably, eventually add the others, but to be honest; it's a low priority until I take up a project using
the other report types. A pull request would be swell if someone happens upon the requirement before I do. 🤓
