#!/bin/sh
echo "Starting to run weekly financial data update"

dwnld_file=$(dirname $(dirname "$(readlink -f $0)"))/src/downloader.py;

echo "Downloading new Master Data file"
#python3 $dwnld_file master_updt

echo "Downloading External Data"
#python3 $dwnld_file external

echo "Downloading the SEC Data from Edgar"
python3 $dwnld_file sec

echo "Downloading Stock Data from TD Ameritrade"
#python3 $dwnld_file stocks

echo "Finished weekly extract of financial data"
exit 0;
