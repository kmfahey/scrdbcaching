#!/bin/sh -x

# Check if the number of arguments is either 3 or 4
if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
    echo "Usage: $0 <True|False> <start_integer> <end_integer> [jobdir_path]"
    exit 1
fi

# Validate first argument
if [ "$1" != "True" ] && [ "$1" != "False" ]; then
    echo "First argument must be either 'True' or 'False'"
    exit 1
fi

# Validate second and third arguments are integers and second is less than third
if ! [ "$2" -eq "$2" ] 2> /dev/null || ! [ "$3" -eq "$3" ] 2> /dev/null || ! [ "$2" -le "$3" ]; then
    echo "Second and third arguments must be integers, and the second must be less than or equal toor equal to the third"
    exit 1
fi

# If the fourth argument is provided, check if it's a path to an existing directory
if [ -n "$4" ] && ! [ -d "$4" ]; then
    echo "Fourth argument must be a path to an existing directory"
    exit 1
fi

export HTTP_PROXY="http://scraperapi:6056b5b46c5d1648afe91e266c1f7988@proxy-server.scraperapi.com:8001/"
export HTTPS_PROXY="http://scraperapi:6056b5b46c5d1648afe91e266c1f7988@proxy-server.scraperapi.com:8001/"

# Conditional command execution based on the presence of the fourth argument
if [ -n "$4" ]; then
    scrapy crawl propertyrecords -a "start_with_tacoma=$1" -a "credits_used=$2" -a "credits_threshold=$3" -s "JOBDIR=$4" 2>&1 | tee -a countyoffice_spider.log
else
    scrapy crawl propertyrecords -a "start_with_tacoma=$1" -a "credits_used=$2" -a "credits_threshold=$3" 2>&1 | tee -a countyoffice_spider.log
fi
