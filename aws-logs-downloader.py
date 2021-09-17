# # This is an aws logs downloader. Use: python aws-logs-downloader.py --help to see the help.

import json
import subprocess
import sys
from datetime import datetime, timedelta
import pytz
import argparse

parser = argparse.ArgumentParser(prog='aws-logs-downloader', description=
                                    'Download aws logs for a log group in particular time period. '
                                    'In case there are multiple log streams for that period multiple files will be created. '
                                    'Downloaded files will be stored in current directory, '
                                    'and will be named by the log streams that has a log events in given time period. '
                                    '(If the group name contains forward slashes, they will be replaced by underscores. Each file will be overwritten if it already exists.)'
                                    'Prerequisite: You need to be logged in to your aws profile. '
                                    'The Script itself is going to use on behalf of you the AWS command line APIs: '
                                    '"aws logs describe-log-streams" and "aws logs get-log-events" '
                                    'Usage example: python aws-logs-downloader -g /ecs/my-cluster-test-my-app -t "2021-09-04 05:59:50 +00:00" -i 60'
                                 )

def is_log_stream_in_range(stream_def, from_date_time, till_date_time):
    is_newer_than_from_date = stream_def["lastEventTimestamp"] > datetime.timestamp(from_date_time) * 1000
    is_older_than_till_date = stream_def["firstEventTimestamp"] < datetime.timestamp(till_date_time) * 1000
    return is_newer_than_from_date and is_older_than_till_date


def is_timestamp_in_range(timestamp, from_date_time, till_date_time):
    is_newer_than_from_date = timestamp > datetime.timestamp(from_date_time) * 1000
    is_older_than_till_date = timestamp < datetime.timestamp(till_date_time) * 1000
    return is_newer_than_from_date and is_older_than_till_date


def get_log_stream_names_in_range(from_date_time, till_date_time):
    log_stream_names_result = subprocess.run(
        "aws logs describe-log-streams"
        " --order-by LastEventTime"
        " --descending"
        " --log-group-name " + args.log_group +
        " --output json" +
        MY_AWS_PROFILE_AND_REGION,
        shell=True,
        capture_output=True
    )
    # print("stdout:" + log_stream_names_result.stdout.decode("utf-8"))
    stderr = log_stream_names_result.stderr.decode("utf-8")
    if stderr:
        sys.stderr.write(stderr)
        return None

    stream_names_json_output = json.loads(log_stream_names_result.stdout)
    log_stream_names = []
    for streamDef in stream_names_json_output["logStreams"]:
        if is_log_stream_in_range(streamDef, from_date_time, till_date_time):
            # print(streamDef["logStreamName"])
            # print(streamDef["firstEventTimestamp"])
            # print(streamDef["lastEventTimestamp"])
            log_stream_names.append(streamDef["logStreamName"])

    print("Streams in range from {} till {} are:".format(fromDateTime.strftime(TIME_FORMAT), tillDateTime.strftime(TIME_FORMAT)))
    print(log_stream_names)
    return log_stream_names


def get_next_token_parameter(forward_token):
    if forward_token is None:
        return ""
    else:
        return " --next-token " + forward_token


def download_log_stream_time_range_to_file():
    log_file_name = (stream_name + ".log").replace("/", "_")
    print("\nDownloading logs into file {}".format(log_file_name))
    log_file = open(log_file_name, "w")
    log_file.write("log stream {} \nsince {} till {} :".format(
        stream_name,
        fromDateTime.strftime(TIME_FORMAT),
        tillDateTime.strftime(TIME_FORMAT)))
    forward_token = None
    while True:
        log_events_result = subprocess.run(
            "aws logs get-log-events" +
            " --log-group-name " + args.log_group +
            " --log-stream-name " + stream_name +
            " --start-time {}".format(int(datetime.timestamp(fromDateTime) * 1000)) +
            " --end-time {}".format(int(datetime.timestamp(tillDateTime) * 1000)) +
            " --start-from-head" +
            get_next_token_parameter(forward_token) +
            MY_AWS_PROFILE_AND_REGION,
            shell=True,
            capture_output=True
        )
        # print("stdout:" + log_events_result.stdout.decode("utf-8"))
        stderr = log_events_result.stderr.decode("utf-8")
        if stderr:
            print("stderr:" + stderr)
        log_events_json = json.loads(log_events_result.stdout)
        log_events = log_events_json["events"]
        if len(log_events) == 0:
            break
        forward_token = log_events_json["nextForwardToken"]
        # backward_token = log_events_json["nextBackwardToken"]
        for event in log_events:
            if is_timestamp_in_range(event["timestamp"], fromDateTime, tillDateTime):
                log_file.write("\n")
                log_file.write(event["message"])
        log_file.flush()
        print("Last downloaded event: {}".format(log_events[len(log_events) - 1]["message"]))
    log_file.close()


if __name__ == '__main__':
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0')
    parser.add_argument('-g', '--log-group', required=True, metavar='', help='(required) Log group name for which the log stream events needs to be downloaded')
    parser.add_argument('-t', '--end-time', metavar='', default="", help='(default: now) End date and time of the downloaded logs in format: %%Y-%%m-%%d %%H:%%M:%%S %%z (example: 2021-09-04 05:59:50 +00:00)')
    parser.add_argument('-i', '--interval', metavar='', type=int, default=30, help='(default: 30) Time period in minutes before the end-time. This will be used to calculate the time since which the logs will be downloaded.')
    parser.add_argument('-p', '--profile', metavar='', default='dev', help='(default: dev) The aws profile that is logged in, and on behalf of which the logs will be downloaded.')
    parser.add_argument('-r', '--region', metavar='', default='eu-central-1', help='(default: eu-central-1) The aws region from which the logs will be downloaded.')
    args = parser.parse_args()
    MY_AWS_PROFILE_AND_REGION = " --profile " + args.profile + " --region " + args.region
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S %z"

    # ---- Overwrite the configured values (dev purpose only) ----
    # args.profile = "sandbox"
    # args.region = "eu-central-1"
    # args.end_time = "2021-09-14 12:40:30 +00:00"
    # args.interval = 4 * 60  # in minutes
    # args.log_group = "another-log-group"

    tillDateTime = pytz.utc.localize(datetime.utcnow())
    if args.end_time:
        tillDateTime = datetime.strptime(args.end_time, TIME_FORMAT)
    fromDateTime = tillDateTime - timedelta(days=0, hours=0, minutes=args.interval)

    stream_names_in_time_range = get_log_stream_names_in_range(fromDateTime, tillDateTime)
    if stream_names_in_time_range is None:
        exit(-1)  # see the stderr output
    for stream_name in stream_names_in_time_range:
        download_log_stream_time_range_to_file()
