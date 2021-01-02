#!/usr/bin/env python3
import configparser
import os
import shutil
import sys
import socket
import argparse
import logging
import gzip
import time
from datetime import datetime, timedelta
import splunklib.client as client
from splunklib.binding import AuthenticationError
from splunklib.binding import HTTPError
import progressbar

#Global Variables
## Dict to keep track of what logs have been downloaded and written
ran_jobs = {}
## 3 path objects for output dir, config dir, and oneshot ran dir
out_dir_name = os.path.dirname(os.path.abspath(__file__))
conf_dir_name = os.path.dirname(os.path.abspath(__file__)) + "/conf"
ran_dir_name = os.path.dirname(os.path.abspath(__file__)) + "/complete"

#Global Log Settings
logging.basicConfig(
    filename="./log/splunk_log_grabber.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s"
    )

# Global Splunk Access
hostname ="logsearch.central1.com"
username ="Sample_User"
password ="Sample_pass"

## This is the main function when an argument is passed
def main():
    parser = argparse.ArgumentParser(description="Run without arguments the program runs in daemon mode and parses all conf files in it's working directory")
    parser.add_argument('--quiet', '-q', action='store_true', help='run without console output' )
    parser.add_argument('--syntax', action='store_true', help='syntax checker' )
    parser.add_argument('--input', '-i',type=str, default=None, help='Input Config File taken as argument, should be given witout other arguments' )
    parser.add_argument('--output', '-o', type=str, default=None, help='Output File location will be local directory, unless otherwise specified' )
    parser.add_argument('--outputdir', '-o', type=str, default=out_dir_name, help='Output directory location' )
    parser.add_argument('--earliest', '-e', type=str, default=None, help='Earliest Time Frame for search' )
    parser.add_argument('--latest', '-l', type=str, default=None, help='Latest Time Frame for search' )
    parser.add_argument('--search', '-s', type=str, default=None, help='Search String' )
    parser.add_argument('--logtype', '-t', choices=['raw', 'json', 'xml'], default='raw', help='log output type' )

    args = parser.parse_args()

    if args.syntax:
        folder_reader(syntax_check=True)

    splunkSearchDownload(quiet=args.quiet, inputfile=args.input, outputfile=args.output, outputdir=args.outputdir,
                        earliest_time=args.earliest, latest_time=args.latest,
                        search_string=args.search, logtype=args.logtype )

## Function to parse the config file, not used if command line arguments are used
## This uses a --syntax CLI arg to ensure config files will not cause the program to error out.
def config_parser(inputfile=None):
    with open(os.path.join(conf_dir_name, inputfile), "r") as f:
        config = configparser.RawConfigParser()
        try:
            config.read_file(f)
        except Exception as e:
            logging.error(e)
## Two types of configs Oneshot and Scheduled.
## Oneshot you give the start and stop time/date
## Scheduled just feeds the previous hour/day based on your search
    if "oneshot" in config.sections():
        conf_type = "oneshot"
        logging.debug("oneshot config loaded")
        try:
            outputfile = config.get("oneshot", "outputfile")
            outputdir = config.get("oneshot","outputdir")
            search_string = config.get("oneshot", "search_string")
            earliest_time = config.get("oneshot", "earliest_time")
            latest_time = config.get("oneshot", "latest_time")
            logtype = config.get("oneshot", "logtype")
        except configparser.NoOptionError:
            sys.exit("Error: Option not parsed in configuration file {}".format(inputfile))

        if '' in (hostname, username, password, search_string, earliest_time, latest_time):
            sys.exit("Error: missing config value in {}".format(inputfile))
        return outputfile, outputdir, search_string, earliest_time, latest_time, logtype, conf_type

    elif "scheduled" in config.sections():
        conf_type = "scheduled"
        logging.debug("Scheduled config loaded for {}".format(inputfile))
        try:
            outputfile = config.get("scheduled", "outputfile")
            outputdir = config.get("scheduled", "outputdir")
            search_string = config.get("scheduled", "search_string")
            frequency = config.get("scheduled", "frequency")
            logtype = config.get("scheduled", "logtype")

        except configparser.NoOptionError:
            sys.exit("Error: Option not parsed in configuration file {}".format(inputfile))

        if '' in (search_string, frequency):
            sys.exit("Error: missing config value in {}".format(inputfile))
    ## Previous_time_window rounds down the time based on the frequency
    ## Duplicate search are not run because the output file has the timestamp attached
    ## this output file references against a dictionary(ran_jobs) to ensure duplication doesn't occur
        latest_time, earliest_time = previous_time_window(frequency)
        return outputfile, outputdir, search_string, earliest_time, latest_time, logtype, conf_type

def splunkSearchDownload(inputfile="config.conf", outputfile=None, outputdir=out_dir_name, quiet=True, earliest_time=None, latest_time=None, search_string=None, frequency=None, logtype="raw"):
    ## This block of code differentiates between a config file and CLI arguments
    if inputfile != None:
        if outputfile == None:
            outputfile, outputdir, search_string, earliest_time, latest_time, logtype, conf_type = config_parser(inputfile)
        else:
            _, outputdir, search_string, earliest_time, latest_time, logtype, conf_type = config_parser(inputfile)
    ## This is the main code that interacts with Splunk
    ## The while statement runs against a function that checks if the job has run
    while not has_job_run(inputfile, outputfile):
        logging.debug("{outputfile} has not been run previously")
        try:
            service = client.connect(host=hostname, port=8089, username=username, password=password)
        except AuthenticationError:
            logging.error("Error: incorrect credentials")
            sys.exit("Error: incorrect credentials")
        ## This exception is to account for network issues when running in daemon mode
        ## if network activity is lost then this will go into a 30 minute sleep before trying again
        ## this is a recursive run and python has recursion limits, so it's admitedly hacky
        except socket.gaierror:
            logging.error(f"could not reach host: {hostname}")
            logging.error("Waiting 10 minutes before trying again")
            time.sleep(600)
            splunkSearchDownload(inputfile=inputfile, outputfile=outputfile, outputdir=outputdir, earliest_time=earliest_time, latest_time=latest_time, search_string=search_string, logtype=logtype)
        if not quiet:
            print("Successfully connected to Splunk")
        logging.debug("Successfully connected to Splunk {}@{}".format(username,hostname))
        logging.info("Processing Search: {} from {} to {}".format(search_string, earliest_time, latest_time))
        jobs = service.jobs
        kwargs = {"exec_mode": "normal",
                                "earliest_time": earliest_time,
                                "latest_time": latest_time}
        if not quiet:
            print("Running search")
        try:
            job = jobs.create("search " + search_string, **kwargs)
        except AuthenticationError:
            logging.info("Splunk Session timed out. Reauthenticating")
            service = client.connect(host=hostname, port=8089, username=username, password=password)
            job = jobs.create("search " + search_string, **kwargs)
        logging.info("Search job created with SID:{} for {} outputing to {}".format(job.sid, inputfile, outputfile))

        # Progress bar fanciness
        if not quiet:
            widgets = [progressbar.Percentage(), progressbar.Bar()]
            bar = progressbar.ProgressBar(widgets=widgets, max_value=1000).start()

        # Wait for job to complete
        while True:
            while not job.is_ready():
                pass
            if job["isDone"] == "1":
                if not quiet:
                    bar.finish()
                logging.info("Search Job completed - SID:{}".format(job.sid))
                break
            elif not quiet:
                progress_percent = round(float(job["doneProgress"])*100, 1)
                bar.update(int(progress_percent*10))
            time.sleep(2)


        event_count = int(job["eventCount"])
        if event_count == 0:
            logging.info("Search Results empty for SID: {} Search Parameters: {}".format(job.sid, search_string))
        logging.info("Downloading and writing results to file {} - SID:{}".format(outputfile, job.sid))
        # Progress bar fanciness round 2
        i = 0

        if not quiet:
            print("Downloading")
            widgets = [progressbar.Percentage(), progressbar.Bar()]
            try:
                bar = progressbar.ProgressBar(widgets=widgets, max_value=(event_count-1)).start()
            except ValueError:
                print("WARNING: Search Results Empty")
                logging.warn("Search Results Empty")

        # Read results and write to file
        outputfile_updated = outputfile + "--" + earliest_time + "--" + latest_time + ".log.gz"
        ## Use gzip here which allows us to store the log in memory, zip, then write to disk
        ## otherwise we'd have to write to disk, read the log in full, then zip, then remove the old log
        ## depending on the size of logs this may need to be looked at, but hopefully swap will handle
        ## anything really big, zipping is required so this is currently the best solution
        with gzip.open(os.path.join(outputdir, outputfile_updated), "wb") as out_f:
            while i < event_count:
                result_output = bytes()
                try:
                    job_results = job.results(output_mode=logtype, count=1000, offset=i)
                except AuthenticationError:
                    logging.info("Splunk Session timed out. Reauthenticating")
                    service = client.connect(host=hostname, port=8089, username=username, password=password)
                    job_results = job.results(output_mode=logtype, count=1000, offset=i)
                for result in job_results:
                    result_output += result
                out_f.write(result_output)
                if not quiet:
                    try:
                        bar.update(i + 1)
                    except ValueError:
                        print ("Progress bar error")
                        logging.warn("Progress bar error")
                i += 1000
        if not quiet:
            bar.finish()
            print("\n File Written to {}".format(outputfile_updated))
        logging.info("File written to {} for input file {} and SID {}".format(outputfile_updated, inputfile, job.sid))
        ## Create Dictionary of currently Set variables
        dict2 ={outputfile: [earliest_time,latest_time, logtype]}
        ## append the above dict to the global dict for reference outside function
        ran_jobs.update(dict2)
        ## if this was a CLI argument run, then no inputfile would be set so just exit
        if inputfile == None:
            sys.exit(0)
        ## If it's a oneshot file it should not be run again but the program should continue running in the background
        ## to achieve that we need to make sure the while loop returns true
        ## unfortunately we also want to move the config file to clean things up
        ## so we set inputfile variable to oneshot and have the has_job_run function return true if it sees inputfile set to that
        ## this is only an issue because by moving the config file means it can't be read to check has_job_run
        if conf_type == "oneshot":
            fullpath_inputfile = conf_dir_name + "/" + inputfile
            fullpath_completeinputfile = ran_dir_name + "/" + inputfile
            shutil.move(fullpath_inputfile, fullpath_completeinputfile)
            logging.info(f"oneshot file moved from {fullpath_inputfile} to {fullpath_completeinputfile}")
            inputfile = "oneshot"

## Code to process the frequency, probably some plugin can do this better but whatever
## basically looks at the current time, does a timedelta by an hour or a day then removes
## the minutes and seconds (and hours if daily) from the time, then returns the beginning
## and end times for the splunk search
def previous_time_window(frequency):
    current_time = datetime.now
    if "hourly" in frequency:
        logging.debug("Processing Hourly Frequency")
        latest_time=hour_rounder(current_time())
        logging.debug(f"Latest Date/Time is {latest_time}")
        earliest_time=hour_rounder(current_time() - timedelta(hours = 1))
        logging.debug(f"Earliest Date/Time is {earliest_time}")
    elif "daily" in frequency:
        latest_time=day_rounder(current_time())
        earliest_time=day_rounder(current_time() - timedelta(hours = 24))
    return latest_time, earliest_time

def hour_rounder(t):
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)).strftime('%Y-%m-%dT%H:00:00')

def day_rounder(t):
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)).strftime('%Y-%m-%dT00:00:00')

## This function reads the folder for config files and then processes them
## its used also to do a syntax check of the config files by parsing all of them then exiting
def folder_reader(outputfile=None, syntax_check=False):
    logging.debug("folder reader accessing folder contents to check for .conf")
    folder_contents = os.listdir(conf_dir_name)
    for x in folder_contents:
        ext = os.path.splitext(x)[-1].lower()
        logging.debug(f"file: {x} found, extension is {ext}")
        if ext == ".conf":
            logging.debug("Checking if {} has run".format(x))
            if syntax_check:
                logging.info(f"performing syntax check on {x}")
                config_parser(inputfile=x)
            else:
                splunkSearchDownload(inputfile=x)
    if syntax_check:
        print("Syntax Check Found No Errors")
        sys.exit(0)

## This is the function which checks if a log download has already been done
## The first two parts account for CLI arguments and oneshot configs
## The rest references ran_jobs dict and looks for a match, it's enclosed in a try block
## which allows for an reference error to just spit out false so the job runs
def has_job_run(inputfile, outputfile):
    if inputfile == None:
        return False
    elif inputfile == "oneshot":
        return True
    outputfile, _, _, earliest_time, latest_time, logtype, _ = config_parser(inputfile)
    try:
        value = all(foo in ran_jobs[outputfile] for foo in [earliest_time,latest_time, logtype])
        logging.debug(f"{outputfile} has run: {value}")
        return value
    except:
        logging.debug(f"{outputfile} has run: False")
        return False

## First checks if there are CLI args and if so runs the main function which takes over
## if no arguments are given then it goes into a while true loop that passes off to folder_reader
## and if you ctrl-c the program it exits gracefully
if __name__ == "__main__":
    if len(sys.argv) > 1:
        logging.info("Arguments given to command, program initiated.. parsing")
        main()
    else:
        logging.info("Arguments not given to command, starting dameon mode initiated")
        try:
            while True:
                time.sleep(5)
                logging.debug("No Suitable conf File Found")
                folder_reader()
        except KeyboardInterrupt:
            print("\n Exiting Program")
            logging.info("Keyboard interrupt, exiting...")
            sys.exit(0)
