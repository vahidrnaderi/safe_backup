#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  safe_backup.py
#
#  Copyright 2023 Vahidreza Naderi <vahidrnaderi@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import shutil
import boto3
from botocore.client import ClientError
from pathlib import Path
import redis
import argparse

# from multiprocessing import Pool
import functools
import urllib.parse
import multiprocessing

# Logging levels => NOTSET -> DEBUG -> INFO -> WARNING -> ERROR -> CRITICAL
logging.basicConfig(
    level=logging.DEBUG,
    format=" %(asctime)s -  %(levelname)s -  %(message)s",
)

# Start/Stop logging with commenting/uncommenting next line
# logging.disable(logging.CRITICAL)


def color_log(types, message):
    colors = {
        "HEADER": "\033[95m",
        "OKBLUE": "\033[94m",
        "OKCYAN": "\033[96m",
        "OKGREEN": "\033[92m",
        "WARNING": "\033[93m",
        "FAIL": "\033[91m",
        "ENDC": "\033[0m",
        "BOLD": "\033[1m",
        "UNDERLINE": "\033[4m",
    }
    match types:
        case "header":
            logging.debug(f"{colors['HEADER']}{message}{colors['ENDC']}")
        case "notest":
            logging.notest(f"{colors['OKGREEN']}{message}{colors['ENDC']}")
        case "debug":
            logging.debug(f"{colors['OKCYAN']}{message}{colors['ENDC']}")
        case "info":
            logging.info(f"{colors['OKBLUE']}{message}{colors['ENDC']}")
        case "warning":
            logging.warning(f"{colors['WARNING']}{message}{colors['ENDC']}")
        case "error":
            logging.error(f"{colors['FAIL']}{message}{colors['ENDC']}")
        case "critical":
            logging.critical(f"{colors['UNDERLINE']}{message}{colors['ENDC']}")
        case "bold":
            logging.bold(f"{colors['BOLD']}{message}{colors['ENDC']}")
        case "reset":
            logging.reset(f"{colors['ENDC']}{message}{colors['ENDC']}")


def debug(func):
    """Print the function signature and return value"""

    @functools.wraps(func)
    def wrapper_debug(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)

        # Do something before
        color_log("info", f"------ Calling {func.__name__}({signature})")

        value = func(*args, **kwargs)

        # Do something after
        color_log("info", f"------ End of {func.__name__!r} returned {value!r}")

        return value

    return wrapper_debug


class SafeBackup:
    __region_dest = None

    def db_connect(self):
        REDIS_DECODE_RESPONSE = os.getenv("REDIS_DECODE_RESPONSE", True)

        redis_url = os.getenv("REDIS_URL", "127.0.0.1:6379")

        urllib.parse.uses_netloc.append("redis")
        url = urllib.parse.urlparse(redis_url)

        color_log(
            "debug",
            f"------\n \
            {redis_url = }\n \
            {urllib = }\n \
            {url = }\n \
            {REDIS_DECODE_RESPONSE = } \
        ",
        )

        redis_db = redis.StrictRedis(
            host=url.scheme,
            port=url.path,
            db=0,
            decode_responses=REDIS_DECODE_RESPONSE,
        )
        color_log("debug", f"---- {redis_db =}")

        return redis_db

    @debug
    def __init__(self, args):
        """
        Checking for intruption and continue the last command.
        """
        self.redis_db = self.db_connect()
        color_log("debug", f" *********** args = {args} ######### ")
        redis_keys = self.redis_db.scan(0, "*:marker")[1]
        for key in redis_keys:
            color_log("debug", f" *********** key = {key} ######### ")
            commands = key.split(":")
            command_array = commands[2].split("__")
            color_log("debug", f" *********** commands = {commands} ######### ")
            color_log(
                "debug",
                f" *********** command_array = \
{command_array} ######### ",
            )
            if command_array[0] == "l":
                self.save_files_list_in_redis(
                    "l",
                    command_array[1],
                    command_array[2],
                    intruption=True,
                    first_marker=self.redis_db.get(key),
                )
            elif command_array[0] == "c":
                self.save_files_list_in_redis(
                    "c",
                    command_array[1],
                    command_array[2],
                    commands[2],
                    intruption=True,
                    first_marker=self.redis_db.get(key),
                )
                self.download_files_list_from_redis(
                    "d",
                    f"{commands[0]}:{commands[1]}",
                    command_array[3],
                    command_array[4],
                )

        redis_keys = self.redis_db.scan(0, "*-working1")[1]
        for key in redis_keys:
            color_log("debug", f" *********** {key} #########")
            keys = key.split("-")
            command = keys[1].split("__")
            color_log("debug", f" *********** {keys} + {command} ######### ")
            self.download_files_list_from_redis(
                "d",
                keys[0],
                command[1]
            )

    @debug
    def __s3_connect__(self, destination="source"):
        """
        Connect to a given destination bucket and return a resource.
        """

        if destination == "source":
            AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
            AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
            AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
            AWS_ENDPOINT_URL = os.environ["AWS_ENDPOINT_URL"]
        elif destination == "dest":
            AWS_DEFAULT_REGION = os.getenv(
                "DEST_AWS_DEFAULT_REGION", os.environ["AWS_DEFAULT_REGION"]
            )
            self.__region_dest = AWS_DEFAULT_REGION
            AWS_ACCESS_KEY_ID = os.getenv(
                "DEST_AWS_ACCESS_KEY_ID", os.environ["AWS_ACCESS_KEY_ID"]
            )
            AWS_SECRET_ACCESS_KEY = os.getenv(
                "DEST_AWS_SECRET_ACCESS_KEY",
                os.environ["AWS_SECRET_ACCESS_KEY"],
            )
            AWS_ENDPOINT_URL = os.getenv(
                "DEST_AWS_ENDPOINT_URL", os.environ["AWS_ENDPOINT_URL"]
            )
        else:
            print(f"The s3 destination={destination} is not defined.")
            exit(1)

        session = boto3.session.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=None,
        )

        return session.resource(
            "s3",
            region_name=AWS_DEFAULT_REGION,
            endpoint_url=AWS_ENDPOINT_URL,
            config=boto3.session.Config(signature_version="s3v4"),
            verify=False,
        )

    @debug
    def __create_bucket__(self, s3_client, bucket_name, region=None):
        """
        Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3
        default region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if region is None:
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                location = {"LocationConstraint": region}
                s3_client.create_bucket(
                    Bucket=bucket_name, CreateBucketConfiguration=location
                )
        except ClientError as e:
            logging.error(e)
            return False
        return True

    @debug
    def __make_redis_list_from_pages__(self, args):
        color_log("debug", args[1]["Key"])
        color_log("debug", args)
        self.redis_db.sadd(args[0], args[1]["Key"])

    @debug
    def __multiprocess__(self, redis_key, page_contents):
        processes = []
        for content in page_contents:
            args = [
                [redis_key, content],
            ]
            p = multiprocessing.Process(
                target=self.__make_redis_list_from_pages__, args=args
            )
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    @debug
    def __s3_list_paginator__(
        self,
        bucket,
        command_key,
        page_items=100,
        max_items=None,
        first_marker="",
    ):
        # Create a client
        s3_so = self.__s3_connect__().meta.client

        # Create a reusable Paginator
        paginator = s3_so.get_paginator("list_objects")

        # Create and Customizing page iterators
        page_iterator = paginator.paginate(
            Bucket=bucket.name,
            PaginationConfig={
                "PageSize": page_items,
                "StartingToken": first_marker,
                "MaxItems": max_items,
            },
        )

        redis_key = f"s3:{bucket.name}"

        for page in page_iterator:
            color_log("debug", f" \n*********\n {page =}\n ############ \n")
            color_log("debug", f" **** Marker ************ {page['Marker']}")
            if page["IsTruncated"]:
                color_log(
                    "debug",
                    f" **** NextMarker ******** \
{page['NextMarker']}",
                )
            else:
                color_log("debug", " **** NextMarker ******** ")

            if "Contents" in list(page.keys()):
                self.redis_db.set(
                    f"{redis_key}:{command_key}:marker",
                    page["Marker"],
                )
                self.__multiprocess__(redis_key, page["Contents"])
        else:
            self.redis_db.getdel(f"{redis_key}:{command_key}:marker")

        return redis_key

    @debug
    def bucket_exists(self, bucket_name):
        s3_so = self.__s3_connect__()
        try:
            s3_so.meta.client.head_bucket(Bucket=bucket_name)
            return True, f"<BUCKET_NAME>='{bucket_name}' is exists!"
        except ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                return False, f"<BUCKET_NAME>='{bucket_name}' is not exists!"
            else:
                return (
                    False,
                    f"Something went wrong in s3:<BUCKET_NAME>= \
'{bucket_name}'! Error is \"{e}\"",
                )

    @debug
    def save_files_list_in_redis(
        self,
        option,
        source,
        location,
        command_key="",
        intruption=False,
        first_marker=None,
    ):
        """
        Make a list from source and save it in redis.
        source must be one of 'local' or 's3'

        if source is 'local' then
                        <location> must be <source_directory>
        and if source is 's3' then
                        <location> must be:
                                        <bucket_name> or '*' for all buckets

        """
        files_path = ""
        redis_key = ""
        match source:
            case "s3":
                color_log(
                    "debug",
                    " *** save_files_list_in_redis() \
=> Source is a s3.",
                )
                s3_so = self.__s3_connect__()
                bucket = s3_so.Bucket(location)
                if not intruption:
                    if option == "l":
                        redis_key = self.__s3_list_paginator__(
                            bucket, f"{option}__{source}__{location}"
                        )
                    elif option == "c":
                        redis_key = self.__s3_list_paginator__(
                            bucket,
                            command_key,
                        )
                else:
                    if option == "l":
                        redis_key = self.__s3_list_paginator__(
                            bucket,
                            f"{option}__{source}__{location}",
                            first_marker=first_marker,
                        )
                    elif option == "c":
                        redis_key = self.__s3_list_paginator__(
                            bucket, command_key, first_marker=first_marker
                        )

            case "local":
                color_log(
                    "debug",
                    " *** save_files_list_in_redis() \
=> Source is a local.",
                )
                if Path(location).is_dir() and Path(location).exists:
                    color_log(
                        "debug",
                        " *** save_files_list_in_redis() => \
Location is a directory.",
                    )
                    root_path = location.split(os.sep)[-1]
                    files_path = root_path
                    for folderName, subfolders, filenames in os.walk(location):
                        color_log(
                            "debug",
                            " *** save_files_...() => 1 \
-----------------------------------------------",
                        )
                        color_log(
                            "debug",
                            " *** save_files_...() => \
The folderName folder is "
                            + folderName,
                        )
                        color_log(
                            "debug",
                            " *** save_files_...() => \
The folderName.split(os.sep)[-1] folder is "
                            + folderName.split(os.sep)[-1],
                        )
                        color_log(
                            "debug",
                            " *** save_files_...() => \
The files_path folder is "
                            + files_path,
                        )
                        color_log(
                            "debug",
                            " *** save_files_...() => \
The files_path.split(os.sep)[-1] folder is "
                            + files_path.split(os.sep)[-1],
                        )
                        color_log(
                            "debug",
                            " *** save_files_...() => \
2 ----------------------------------------------------",
                        )
                        if not folderName.split(os.sep)[-1] == files_path:
                            color_log(
                                "debug",
                                " *** save_files_...() => \
The folderName.split(os.sep)[-2] folder is "
                                + folderName.split(os.sep)[-2],
                            )
                            if (
                                folderName.split(os.sep)[-2]
                                == files_path.split(os.sep)[-1]
                            ):
                                parent_path = files_path
                                f = folderName.split(os.sep)[-1]
                                files_path += f"/{f}"
                                color_log(
                                    "debug",
                                    " *** save_files_...() => if : \
The current files_path is "
                                    + files_path,
                                )
                            elif (
                                folderName.split(os.sep)[-2]
                                == files_path.split(os.sep)[-2]
                            ):
                                f = folderName.split(os.sep)[-1]
                                files_path = f"{parent_path}/{f}"
                                color_log(
                                    "debug",
                                    " *** save_files_...() => \
elif 1: The current files_path is "
                                    + files_path,
                                )
                            elif folderName.split(os.sep)[-2] == root_path:
                                f = folderName.split(os.sep)[-1]
                                files_path = f"{root_path}/{f}"
                                color_log(
                                    "debug",
                                    " *** save_files_...() => \
elif 2: The current files_path is "
                                    + files_path,
                                )

                        for filename in filenames:
                            color_log(
                                "debug",
                                " *** save_files_...() => FILE INSIDE "
                                + files_path
                                + ": "
                                + filename,
                            )
                            file_path = f"{files_path}/{filename}"
                            redis_key = f"{source}:{location}"
                            self.redis_db.sadd(redis_key, file_path)
                    color_log(
                        "debug",
                        f" *** save_files_...() \
=> {self.redis_db.keys()}",
                    )
                    print(
                        f"List of files created in '{redis_key}' \
redis key successfuly."
                    )
                else:
                    print("location is not directory or not exist.")
                    exit(1)
            case _:
                print("Source is not valied.")
        return redis_key

    @debug
    def download_files_list_from_redis(
        self,
        option,
        redis_key,
        destination,
        # workers,
    ):
        source = redis_key.split(":")
        color_log("debug", f" *** download_files_...()=> {source =}")
        color_log("debug", f" *** download_files_...()=> {destination =}")
        # redis_key_worker = f"{redis_key}-{option}__{destination}__{workers}"
        redis_key_worker = f"{redis_key}-{option}__{destination}"
        if source[0] == "s3":
            s3_source = self.__s3_connect__().meta.client
        if destination.startswith("s3:"):
            s3_dest = self.__s3_connect__("dest").meta.client
        for member in self.redis_db.sscan(redis_key, 0)[1]:
            self.redis_db.set(f"{redis_key_worker}-working1", member)

            # Backup from local to local
            if source[0] == "local" and not destination.startswith("s3:"):
                color_log(
                    "debug",
                    f"*** if *** download_files_...()=> \
source = {Path(source[1]).parent}/{member} \
--> dest = {destination}/{member}",
                )
                parent = Path(f"{destination}/{member}").parent
                if not os.path.exists(parent):
                    os.makedirs(parent)
                match source[0]:
                    case "local":
                        try:
                            shutil.copy2(
                                f"{Path(source[1]).parent}/{member}",
                                f"{destination}/{member}",
                            )
                        except Exception as e:
                            print(f"There was an error: {e}")
                    case "s3":
                        try:
                            s3_source.download_file(
                                source[1],
                                member,
                                f"{destination}/{member}",
                            )
                        except Exception as e:
                            print(f"There was an error: {e}")

            # Backup from s3 to s3
            elif redis_key.startswith("s3:") and destination.startswith("s3:"):
                s3_dest_bucket = destination.split(":")[1]
                color_log(
                    "debug",
                    f" *** elif *** {member = } --> \
dest = s3:{s3_dest_bucket}",
                )

                color_log(
                    "debug",
                    f" *** elif *** {source[1] = } -> \
./{destination}/{member}",
                )

                # upload to s3 destination
                # Check destination bucket and create it if not exists
                try:
                    color_log(
                        "debug",
                        f" ** elif ** \
{s3_dest.list_buckets()['Buckets'] = }",
                    )
                    s3_dest.head_bucket(Bucket=s3_dest_bucket)
                except ClientError:
                    # The bucket does not exist or you have no access.
                    # Create the destination bucket.
                    if not self.__create_bucket__(
                        s3_dest, s3_dest_bucket, self.__region_dest
                    ):
                        print(
                            "  ######  There was a problem to \
create destination bucket!"
                        )
                        exit(1)

                s3 = boto3.resource("s3")
                copy_source = {"Bucket": source[1], "Key": member}
                try:
                    s3.meta.client.copy(copy_source, s3_dest_bucket, member)
                except ClientError as e:
                    print(f" There was an error: {e}")
                    exit(1)

            # Backup from local to s3
            elif source[0] == "local" and destination.startswith("s3:"):
                s3_dest_bucket = destination.split(":")[1]
                color_log(
                    "debug",
                    f" *** elif-2 *** {member = } --> \
dest = s3:{s3_dest_bucket}",
                )
                color_log(
                    "debug",
                    f" *** elif-2 *** {source[1] = } -> \
./{destination}/{member}",
                )
                # Check destination bucket and create it if not exists
                try:
                    color_log(
                        "debug",
                        f" ** elif-2 ** \
{s3_dest.list_buckets()['Buckets'] = }",
                    )
                    s3_dest.head_bucket(Bucket=s3_dest_bucket)
                except ClientError:
                    # The bucket does not exist or you have no access.
                    # Create the destination bucket.
                    if not self.__create_bucket__(
                        s3_dest, s3_dest_bucket, self.__region_dest
                    ):
                        print(
                            "  ######  There was a problem to \
create destination bucket!"
                        )
                        exit(1)

                color_log(
                    "debug",
                    f" *** elif-2 *** {member = } -> \
{member = }",
                )
                source_path_parent = Path(source[1]).parent
                if os.path.exists(Path(f"./{source_path_parent}/{member}")):
                    try:
                        s3_dest.upload_file(
                            f"./{source_path_parent}/{member}",
                            s3_dest_bucket,
                            member,
                        )
                    except ClientError as e:
                        print(f" There was an error: {e}")
                else:
                    print(
                        f" The file ./{source_path_parent}/{member} \
not exists!"
                    )

            # Backup from s3 to local
            elif source[0] == "s3" and not destination.startswith("s3:"):
                parent = Path(f"./{destination}/{member}").parent
                if not os.path.exists(parent):
                    os.makedirs(parent)
                try:
                    s3_source.download_file(
                        source[1],
                        member,
                        f"{destination}/{member}",
                    )
                except Exception as e:
                    print(f"There was an error: {e}")
            else:
                print(" Something went wrong in download process!")
                exit(2)
            self.redis_db.srem(redis_key, member)
        else:
            self.redis_db.delete(f"{redis_key_worker}-working1")

    @debug
    # def copy_files(self, option, source, location, destination, workers):
    def copy_files(self, option, source, location, destination):
        """
        Make a list of files in redis and then start copying or
        downloading files to the destination.
        """

        o = option
        d = destination
        lo = location
        # command_key = f"{o}__{source}__{lo}__{d}__{workers}"
        command_key = f"{o}__{source}__{lo}__{d}"

        # Make list of source files to Redis
        redis_key = self.save_files_list_in_redis(o, source, lo, command_key)

        # Download or copy source files list that we made before in Redis
        # self.download_files_list_from_redis("d", redis_key, d, workers)
        self.download_files_list_from_redis("d", redis_key, d)


def main():
    parser = argparse.ArgumentParser(
        prog="safe_backup", description="Backup your local or s3 files safely."
    )

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "-l",
        nargs=2,
        metavar=("<SOURCE_TYPE>", "<SOURCE_ADDRESS>"),
        help="get <SOURCE_TYPE> as ['local' | 's3'] and \
[ <SOURCE_DIRECTORY> | <BUCKET_NAME> ] \
to create list of source files in Redis",
    )
    group.add_argument(
        "-c",
        nargs=3,
        metavar=(
            "<SOURCE_TYPE>",
            "<SOURCE_ADDRESS>",
            "<DEST>",
            # "<NUMBER_OF_WORKERS>",
        ),
        help="get <SOURCE_TYPE> as ['local' | 's3'] then <SOURCE_ADDRESS> as \
[ <SOURCE_DIRECTORY> | <BUCKET_NAME> ] and \
get <DEST> as [ <LOCAL_DIRECTORY> | s3:<BUCKET_NAME> ] \
to copy source files to destination",
    )
    group.add_argument(
        "-d",
        nargs=2,
        # metavar=("<REDIS_KEY>", "<DEST>", "<NUMBER_OF_WORKERS>"),
        metavar=("<REDIS_KEY>", "<DEST>"),
        help="read Redis and download source files safety to <DEST> \
which can be a <LOCAL_DIRECTORY> or s3:<BUCKET_NAME>",
    )

    args = parser.parse_args()
    color_log("debug", f"main() *** {args}")
    color_log("debug", f"main() *** {args.l}")
    color_log("debug", f"main() *** {args.c}")
    color_log("debug", f"main() *** {args.d}")

    safe_backup = SafeBackup(args)

    if args.l:
        if not args.l[0] == "local" and not args.l[0] == "s3":
            parser.error("<SOURCE_TYPE> must be one of 'local' or 's3'")
        if args.l[0] == "local" and not Path(args.l[1]).is_dir():
            parser.error(
                f"<SOURCE_ADDRESS>='{args.l[1]}' \
is not directory or not exist!"
            )
        if args.l[0] == "s3":
            result, msg = safe_backup.bucket_exists(args.l[1])
            if not result:
                parser.error(msg)

        "Make list of source files to Redis"
        redis_key = safe_backup.save_files_list_in_redis(
            "l",
            args.l[0],
            args.l[1],
        )
        print(f" {redis_key = } successfully created.")

    elif args.c:
        if not args.c[0] == "local" and not args.c[0] == "s3":
            parser.error("<SOURCE_TYPE> must be one of 'local' or 's3'")
        if args.c[0] == "local" and not Path(args.c[1]).is_dir():
            parser.error(
                f"<SOURCE_ADDRESS>='{args.c[1]}' \
is not directory or not exist!"
            )
        if args.c[0] == "s3":
            result, msg = safe_backup.bucket_exists(args.c[1])
            if not result:
                parser.error(msg)
        if not args.c[2].startswith("s3:"):
            if not Path(args.c[2]).is_dir():
                parser.error(
                    f"<DEST>='{args.c[2]}' \
is not directory or not started with 's3:'!"
                )
        elif not len(args.c[2]) > 3:
            parser.error("You must define the <bucket_name> after 's3:'!")
        # if not args.c[3].isdigit() or int(args.c[3]) <= 0:
            # parser.error(
                # f"<NUMBER_OF_WORKERS>='{args.c[2]}' \
# is not integer or not bigger than 0!"
            # )

        "All copy functions must write down here"
        safe_backup.copy_files("c", args.c[0], args.c[1], args.c[2]) #, args.c[3])
        print(f" Copy to <DEST> = {args.c[2]} successfully completed.")

    elif args.d:
        if not safe_backup.redis_db.exists(args.d[0]) == 1:
            parser.error(f"<REDIS_KEY>='{args.d[0]}' is not exists!")
        if not args.d[1].startswith("s3:"):
            if not Path(args.d[1]).is_dir():
                parser.error(
                    f"<DEST>='{args.d[1]}' \
is not directory or not started with 's3:'!"
                )
        elif not len(args.d[1]) > 3:
            parser.error("You must define the <bucket_name> after 's3:'!")
        # if not args.d[2].isdigit() or int(args.d[2]) <= 0:
            # parser.error(
                # f"<NUMBER_OF_WORKERS>='{args.d[2]}' \
# is not integer or not bigger than 0!"
            # )

        "Download or copy source files list that we made before in Redis"
        safe_backup.download_files_list_from_redis(
            "d",
            args.d[0],
            args.d[1],
            # args.d[2],
        )
        print(f" Download to <DEST> = {args.d[1]} successfully completed.")

    else:
        parser.error(f"Input args='{args}' is not defined!")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
