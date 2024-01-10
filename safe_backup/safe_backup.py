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
import urllib.parse
import multiprocessing

# levels => 10    -> 20   -> 30      -> 40    -> 50
# LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

colors = {
    "HEADER": "\033[95m",
    "INFO": "\033[94m",  # "OKBLUE"
    "DEBUG": "\033[96m",  # "OKCYAN"
    "NOTSET": "\033[92m",  # "OKGREEN"
    "WARNING": "\033[93m",
    "ERROR": "\033[91m",  # "FAIL"
    "RESET": "\033[0m",  # "ENDC"
    "CRITICAL": "\033[1m",  # "BOLD"
    "UNDERLINE": "\033[4m",  # "UNDERLINE"
}


def color_log(loglevel="CRITICAL", message="DEBUG message"):
    numeric_level = getattr(logging, loglevel.upper(), None)
    if isinstance(numeric_level, int):
        msg = f"{colors[loglevel.upper()]}{message}{colors['RESET']}"
        logging.log(
            numeric_level,
            msg,
        )
    else:
        message = f"{loglevel = } args in color_log() function is wrong!"
        msg = f"{colors['ERROR']}{message}{colors['RESET']}"
        logging.log(40, msg)


# Debugging all class methods
def debug_methods(cls):
    for name, value in vars(cls).items():
        if callable(value):
            setattr(cls, name, debug_method(value))
    return cls


def debug_method(func):
    """Print the function signature and return value"""

    def wrapper_debug(*args, **kwargs):
        # Do something before
        color_log(
            "info",
            f"---- Calling {func.__name__}(*args={args} and **kwargs={kwargs})",
        )

        value = func(*args, **kwargs)

        # Do something after
        color_log("info", f"#### End of {func.__name__!r} returned {value!r}")

        return value

    return wrapper_debug


@debug_methods
class DB:
    def db_connect(self):
        DB_DECODE_RESPONSE = os.getenv("SBACKUP_DB_DECODE_RESPONSE", True)

        db_url = os.getenv("SBACKUP_DB_URL", "127.0.0.1:6379")

        urllib.parse.uses_netloc.append("redis")
        url = urllib.parse.urlparse(db_url)

        color_log(
            "debug",
            f"------\n"
            f"\t{db_url = }\n"
            f"\t{urllib = }\n"
            f"\t{url = }\n"
            f"\t{DB_DECODE_RESPONSE = }",
        )

        self.db = redis.StrictRedis(
            host=url.scheme,
            port=url.path,
            db=0,
            decode_responses=DB_DECODE_RESPONSE,
        )
        color_log("debug", f"---- {self.db = }")

    def key_exists(self, key):
        return self.db.exists(key)

    def find(self, cursor, pattern):
        return self.db.scan(cursor, pattern)[1]

    def get_elements(self, key, cursor):
        return self.db.sscan(key, cursor)[1]

    def get_keys(self):
        return self.db.keys()

    def delete(self, key):
        return self.db.delete(key)

    def set(self, key, value):
        return self.db.set(key, value)

    def get(self, key):
        return self.db.get(key)

    def set_add(self, key, value):
        return self.db.sadd(key, value)

    def set_remove(self, key, value):
        return self.db.srem(key, value)


@debug_methods
class SafeBackup:
    __region_dest = None

    def __init__(self, args):
        """
        Connect to db and S3 if needed then check for intruption
        and continue the last command.
        """

        DB.db_connect(self)
        color_log("debug", f" *********** args = {args} ######### ")

        self.__check_if_s3_connection_need(args)

        self.__resume_intrupting()

    def __check_if_s3_connection_need(self, args):
        """
        Check and establish a connection if needed for S3.
        """

        if args.l:
            if args.l[0] == "s3":
                self.s3_source = self.__s3_connect("source")
                self.s3_source_client = self.s3_source.meta.client
        elif args.c:
            if args.c[0] == "s3":
                self.s3_source = self.__s3_connect("source")
                self.s3_source_client = self.s3_source.meta.client

        if args.c:
            if args.c[2].startswith("s3:"):
                self.s3_dest = self.__s3_connect("dest")
                self.s3_dest_client = self.s3_dest.meta.client
        elif args.d:
            if args.d[1].startswith("s3:"):
                self.s3_dest = self.__s3_connect("dest")
                self.s3_dest_client = self.s3_dest.meta.client

    def __resume_intrupting(self):
        """
        Check and continue if any interruption occurred.
        """

        db_keys = DB.find(self, 0, "*:marker_sbackup")
        for key in db_keys:
            color_log("debug", f" *********** key = {key} ######### ")
            commands = key.split("-")
            command_array = commands[2].split("__")
            color_log("debug", f" *********** {commands = } ####### ")
            color_log(
                "debug",
                f" *********** {command_array = } ####### ",
            )
            match command_array[0]:
                case "l":
                    self.save_files_list_in_db(
                        "l",
                        command_array[1],
                        command_array[2],
                        intruption=True,
                        first_marker=DB.get(self, key),
                    )
                case "c":
                    self.save_files_list_in_db(
                        "c",
                        command_array[1],
                        command_array[2],
                        commands[2],
                        intruption=True,
                        first_marker=DB.get(self, key),
                    )
                    self.download_files_list_from_db(
                        "d",
                        f"{commands[0]}:{commands[1]}",
                        command_array[3],
                        command_array[4],
                    )

        db_keys = DB.find(self, 0, "*-work_sbackup")
        for key in db_keys:
            color_log("debug", f" *********** {key} #########")
            keys = key.split("-")
            command = keys[1].split("__")
            color_log("debug", f" *********** {keys} + {command} ######### ")
            self.download_files_list_from_db("d", keys[0], command[1])

    def check_db_key_exists(self, key):
        return DB.key_exists(self, key)

    def __s3_connect(self, destination="source"):
        """
        Connect to a given destination bucket and return a resource.
        """

        if destination == "source":
            AWS_DEFAULT_REGION = os.environ["SBACKUP_AWS_DEFAULT_REGION"]
            AWS_ACCESS_KEY_ID = os.environ["SBACKUP_AWS_ACCESS_KEY_ID"]
            AWS_SECRET_ACCESS_KEY = os.environ["SBACKUP_AWS_SECRET_ACCESS_KEY"]
            AWS_ENDPOINT_URL = os.environ["SBACKUP_AWS_ENDPOINT_URL"]
        elif destination == "dest":
            AWS_DEFAULT_REGION = os.getenv(
                "SBACKUP_DEST_AWS_DEFAULT_REGION",
                os.environ["SBACKUP_AWS_DEFAULT_REGION"],
            )
            self.__region_dest = AWS_DEFAULT_REGION
            AWS_ACCESS_KEY_ID = os.getenv(
                "SBACKUP_DEST_AWS_ACCESS_KEY_ID",
                os.environ["SBACKUP_AWS_ACCESS_KEY_ID"],
            )
            AWS_SECRET_ACCESS_KEY = os.getenv(
                "SBACKUP_DEST_AWS_SECRET_ACCESS_KEY",
                os.environ["SBACKUP_AWS_SECRET_ACCESS_KEY"],
            )
            AWS_ENDPOINT_URL = os.getenv(
                "SBACKUP_DEST_AWS_ENDPOINT_URL",
                os.environ["SBACKUP_AWS_ENDPOINT_URL"],
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

    def __create_bucket(self, s3_client, bucket_name, region=None):
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

    def __make_db_list_from_s3_pages(self, args):
        color_log("debug", args[1]["Key"])
        color_log("debug", args)
        DB.set_add(self, args[0], args[1]["Key"])

    def __multiprocess(self, db_key, page_contents):
        processes = []
        for content in page_contents:
            args = [
                [db_key, content],
            ]
            p = multiprocessing.Process(
                target=self.__make_db_list_from_s3_pages, args=args
            )
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    def __s3_list_paginator(
        self,
        bucket,
        command_key,
        page_items=100,
        max_items=None,
        first_marker="",
    ):
        # Create a reusable Paginator
        paginator = self.s3_source_client.get_paginator("list_objects")

        # Create and Customizing page iterators
        page_iterator = paginator.paginate(
            Bucket=bucket.name,
            PaginationConfig={
                "PageSize": page_items,
                "StartingToken": first_marker,
                "MaxItems": max_items,
            },
        )

        db_key = f"s3:{bucket.name}"

        for page in page_iterator:
            color_log("debug", f" \n*********\n {page = }\n ############ \n")
            color_log("debug", f" **** Marker ************ {page['Marker']}")
            if page["IsTruncated"]:
                color_log(
                    "debug",
                    f" **** NextMarker ******** {page['NextMarker']}",
                )
            else:
                color_log("debug", " **** NextMarker ******** ")

            marker_key = f"{db_key}-{command_key}-marker_sbackup"
            if "Contents" in list(page.keys()):
                DB.set(self, marker_key, page["Marker"])
                self.__multiprocess(db_key, page["Contents"])
        else:
            DB.delete(self, marker_key)

        return db_key

    def bucket_exists(self, bucket_name):
        try:
            self.s3_source_client.head_bucket(Bucket=bucket_name)
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
                    f"Something went wrong in s3:<BUCKET_NAME>="
                    f"'{bucket_name}'! Error is '{e}'",
                )

    def save_files_list_in_db(
        self,
        option,
        source,
        location,
        command_key="",
        intruption=False,
        first_marker=None,
    ):
        """
        Make a list from source and save it in db.
        source must be one of 'local' or 's3'

        if source is 'local' then
                        <location> must be <source_directory>
        and if source is 's3' then
                        <location> must be:
                                        <bucket_name> or '*' for all buckets

        """
        files_path = ""
        db_key = ""
        match source:
            case "s3":
                color_log(
                    "debug",
                    " *** save_files_list_in_db() => Source is a s3.",
                )
                bucket = self.s3_source.Bucket(location)
                if not intruption:
                    if option == "l":
                        db_key = self.__s3_list_paginator(
                            bucket, f"{option}__{source}__{location}"
                        )
                    elif option == "c":
                        db_key = self.__s3_list_paginator(
                            bucket,
                            command_key,
                        )
                else:
                    if option == "l":
                        db_key = self.__s3_list_paginator(
                            bucket,
                            f"{option}__{source}__{location}",
                            first_marker=first_marker,
                        )
                    elif option == "c":
                        db_key = self.__s3_list_paginator(
                            bucket, command_key, first_marker=first_marker
                        )

            case "local":
                color_log(
                    "debug",
                    " *** save_files_list_in_db() => Source is a local.",
                )
                if Path(location).is_dir() and Path(location).exists:
                    color_log(
                        "debug",
                        " *** save_files_...() => Location is a directory.",
                    )
                    root_path = location.split(os.sep)[-1]
                    files_path = root_path
                    for folderName, subfolders, filenames in os.walk(location):
                        fn_split = folderName.split(os.sep)
                        fp_split = files_path.split(os.sep)
                        color_log(
                            "debug",
                            " *** save_files_...() => 1"
                            "-----------------------------------------------",
                        )
                        color_log(
                            "debug",
                            " *** save_files_...() => "
                            "The folderName is " + folderName,
                        )
                        color_log(
                            "debug",
                            " *** save_files_...() => "
                            "The folderName.split(os.sep)[-1] folder is "
                            + fn_split[-1],
                        )
                        color_log(
                            "debug",
                            " *** save_files_...() => "
                            "The files_path folder is " + files_path,
                        )
                        color_log(
                            "debug",
                            " *** save_files_...() => "
                            "The files_path.split(os.sep)[-1] folder is "
                            + fp_split[-1],
                        )
                        color_log(
                            "debug",
                            " *** save_files_...() => "
                            "2 ---------------------------------------------",
                        )
                        if not fn_split[-1] == files_path:
                            color_log(
                                "debug",
                                " *** save_files_...() => "
                                "The folderName.split(os.sep)[-2] folder is "
                                + fn_split[-2],
                            )
                            if fn_split[-2] == fp_split[-1]:
                                parent_path = files_path
                                files_path += f"/{fn_split[-1]}"
                                color_log(
                                    "debug",
                                    " *** save_files_...() => if : "
                                    "The current files_path is " + files_path,
                                )
                            elif fn_split[-2] == fp_split[-2]:
                                files_path = f"{parent_path}/{fn_split[-1]}"
                                color_log(
                                    "debug",
                                    " *** save_files_...() => elif 1: "
                                    "The current files_path is " + files_path,
                                )
                            elif fn_split[-2] == root_path:
                                files_path = f"{root_path}/{fn_split[-1]}"
                                color_log(
                                    "debug",
                                    " *** save_files_...() => elif 2: "
                                    "The current files_path is " + files_path,
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
                            db_key = f"{source}:{location}"
                            DB.set_add(self, db_key, file_path)
                    color_log(
                        "debug",
                        f" *** save_files_...() => {DB.get_keys(self)}",
                    )
                    print(f"Files list created in '{db_key = }' successfuly.")
                else:
                    print("location is not directory or not exist.")
                    exit(1)
            case _:
                print("Source is not valied.")
        return db_key

    def download_files_list_from_db(
        self,
        option,
        db_key,
        destination,
    ):
        source = db_key.split(":")
        color_log(
            "debug",
            f" *** download_files_...()=> from {source = } to {destination = }",
        )

        db_key_worker = f"{db_key}-{option}__{destination}"
        for member in DB.get_elements(self, db_key, 0):
            DB.set(self, f"{db_key_worker}-work_sbackup", member)

            # Backup from local to local
            if source[0] == "local" and not destination.startswith("s3:"):
                color_log(
                    "debug",
                    f"*** <local to local> *** download_files_...()=> "
                    f"source = {Path(source[1]).parent}/{member} "
                    f"--> dest = {destination}/{member}",
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
                            self.s3_source_client.download_file(
                                source[1],
                                member,
                                f"{destination}/{member}",
                            )
                        except Exception as e:
                            print(f"There was an error: {e}")

            # Backup from s3 to s3
            elif db_key.startswith("s3:") and destination.startswith("s3:"):
                s3_dest_bucket = destination.split(":")[1]
                color_log(
                    "debug",
                    f" *** <s3 to s3> *** {member = } --> "
                    f"dest = s3:{s3_dest_bucket}",
                )

                color_log(
                    "debug",
                    f" *** <s3 to s3> *** {source[1] = } -> "
                    f"./{destination}/{member}",
                )

                # upload to s3 destination
                # Check destination bucket and create it if not exists
                try:
                    color_log(
                        "debug",
                        f" ** <s3 to s3> ** "
                        f"{self.s3_dest_client.list_buckets()['Buckets'] = }",
                    )
                    self.s3_dest_client.head_bucket(Bucket=s3_dest_bucket)
                except ClientError:
                    # The bucket does not exist or you have no access.
                    # Create the destination bucket.
                    if not self.__create_bucket(
                        self.s3_dest_client,
                        s3_dest_bucket,
                        self.__region_dest,
                    ):
                        print(
                            "  ######  There was a problem to "
                            "create destination bucket!"
                        )
                        exit(1)

                source_copy = {"Bucket": source[1], "Key": member}
                try:
                    self.s3_dest_client.copy(
                        source_copy,
                        s3_dest_bucket,
                        member,
                    )
                except ClientError as e:
                    print(f" There was an error: {e}")
                    exit(1)

            # Backup from local to s3
            elif source[0] == "local" and destination.startswith("s3:"):
                s3_dest_bucket = destination.split(":")[1]
                color_log(
                    "debug",
                    f" *** <local to s3> *** {member = } --> "
                    f"dest = s3:{s3_dest_bucket}",
                )
                color_log(
                    "debug",
                    f" *** <local to s3> *** {source[1] = } -> "
                    f"./{destination}/{member}",
                )
                # Check destination bucket and create it if not exists
                try:
                    color_log(
                        "debug",
                        f" ** <local to s3> ** "
                        f"{self.s3_dest_client.list_buckets()['Buckets'] = }",
                    )
                    self.s3_dest_client.head_bucket(Bucket=s3_dest_bucket)
                except ClientError:
                    # The bucket does not exist or you have no access.
                    # Create the destination bucket.
                    if not self.__create_bucket(
                        self.s3_dest_client, s3_dest_bucket, self.__region_dest
                    ):
                        print(
                            "  ######  There was a problem to "
                            "create destination bucket!"
                        )
                        exit(1)

                color_log(
                    "debug",
                    f" *** elif-2 *** {member = } -> " f"{member = }",
                )
                source_path_parent = Path(source[1]).parent
                if os.path.exists(Path(f"./{source_path_parent}/{member}")):
                    try:
                        self.s3_dest_client.upload_file(
                            f"./{source_path_parent}/{member}",
                            s3_dest_bucket,
                            member,
                        )
                    except ClientError as e:
                        print(f" There was an error: {e}")
                else:
                    spp = source_path_parent
                    print(f" The file ./{spp}/{member} not exists!")

            # Backup from s3 to local
            elif source[0] == "s3" and not destination.startswith("s3:"):
                parent = Path(f"./{destination}/{member}").parent
                if not os.path.exists(parent):
                    os.makedirs(parent)
                try:
                    self.s3_source_client.download_file(
                        source[1],
                        member,
                        f"{destination}/{member}",
                    )
                except Exception as e:
                    print(f"There was an error: {e}")
            else:
                print(" Something went wrong in download process!")
                exit(2)
            DB.set_remove(self, db_key, member)
        else:
            DB.delete(self, f"{db_key_worker}-work_sbackup")

    def copy_files(self, option, source, location, destination):
        """
        Make a list of files in db and then start copying or
        downloading files to the destination.
        """

        o = option
        d = destination
        lo = location
        command_key = f"{o}__{source}__{lo}__{d}"

        # Make list of source files to db
        db_key = self.save_files_list_in_db(o, source, lo, command_key)

        # Download or copy source files list that we made before in db
        self.download_files_list_from_db("d", db_key, d)


def main():
    parser = argparse.ArgumentParser(
        prog="sbackup", description="Backup your local or s3 files safely."
    )

    parser.add_argument(
        "-L",
        nargs=1,
        metavar=("<LOG_LEVEL>"),
        help="Get <LOG_LEVEL> (NOTSET, DEBUG, INFO, WARNING, "
        "ERROR, CRITICAL) and Activate logging level",
    )

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "-l",
        nargs=2,
        metavar=("<SOURCE_TYPE>", "<SOURCE_ADDRESS>"),
        help="get <SOURCE_TYPE> as ['local' | 's3'] and "
        "[<SOURCE_DIRECTORY> | <BUCKET_NAME>] "
        "to create list of source files in db",
    )
    group.add_argument(
        "-c",
        nargs=3,
        metavar=(
            "<SOURCE_TYPE>",
            "<SOURCE_ADDRESS>",
            "<DEST>",
        ),
        help="get <SOURCE_TYPE> as ['local' | 's3'] then <SOURCE_ADDRESS> as "
        "[<SOURCE_DIRECTORY> | <BUCKET_NAME>] and "
        "get <DEST> as [<LOCAL_DIRECTORY> | s3:<BUCKET_NAME>] "
        "to copy source files to destination",
    )
    group.add_argument(
        "-d",
        nargs=2,
        metavar=("<DB_KEY>", "<DEST>"),
        help="read db and download source files safety to <DEST> "
        "which can be a <LOCAL_DIRECTORY> or s3:<BUCKET_NAME>",
    )

    args = parser.parse_args()

    # disable logging
    if not args.L or args.L[0].upper() == "NOTSET":
        logging.disable()
    # activate logging level
    else:
        loglevel = args.L[0].upper()
        numeric_level = getattr(logging, loglevel, None)
        if isinstance(numeric_level, int):
            logging.basicConfig(
                level=numeric_level,
                format=FORMAT,
            )
            color_log(loglevel, f"Start logging at {loglevel} level ")
        else:
            parser.error(f"<LOG_LEVEL>='{args.L[0]}' is not defined!")

    color_log("debug", f"main() *** {args = }")
    color_log("debug", f"main() *** {args.l = }")
    color_log("debug", f"main() *** {args.c = }")
    color_log("debug", f"main() *** {args.d = }")

    safe_backup = SafeBackup(args)

    if args.l:
        if not args.l[0] == "local" and not args.l[0] == "s3":
            parser.error("<SOURCE_TYPE> must be one of 'local' or 's3'")
        if args.l[0] == "local" and not Path(args.l[1]).is_dir():
            parser.error(
                f"<SOURCE_ADDRESS>='{args.l[1]}' is not directory or not exist!"
            )
        if args.l[0] == "s3":
            result, msg = safe_backup.bucket_exists(args.l[1])
            if not result:
                parser.error(msg)

        "Make list of source files to db"
        db_key = safe_backup.save_files_list_in_db(
            "l",
            args.l[0],
            args.l[1],
        )
        print(f" {db_key = } successfully created.")

    elif args.c:
        if not args.c[0] == "local" and not args.c[0] == "s3":
            parser.error("<SOURCE_TYPE> must be one of 'local' or 's3'")
        if args.c[0] == "local" and not Path(args.c[1]).is_dir():
            parser.error(
                f"<SOURCE_ADDRESS>='{args.c[1]}' is not directory or not exist!"
            )
        if args.c[0] == "s3":
            result, msg = safe_backup.bucket_exists(args.c[1])
            if not result:
                parser.error(msg)
        if not args.c[2].startswith("s3:"):
            if not Path(args.c[2]).is_dir():
                parser.error(
                    f"<DEST>='{args.c[2]}'"
                    " is not directory or not started with 's3:'!"
                )
        elif not len(args.c[2]) > 3:
            parser.error("You must define the <bucket_name> after 's3:'!")

        # Copy files
        safe_backup.copy_files("c", args.c[0], args.c[1], args.c[2])
        print(f" Copy to <DEST> = {args.c[2]} successfully completed.")

    elif args.d:
        if not safe_backup.check_db_key_exists(args.d[0]) == 1:
            parser.error(f"<DB_KEY>='{args.d[0]}' is not exists!")
        if not args.d[1].startswith("s3:"):
            if not Path(args.d[1]).is_dir():
                parser.error(
                    f"<DEST>='{args.d[1]}' "
                    "is not directory or not started with 's3:'!"
                )
        elif not len(args.d[1]) > 3:
            parser.error("You must define the <bucket_name> after 's3:'!")

        "Download or copy source files list that we made before in db"
        safe_backup.download_files_list_from_db(
            "d",
            args.d[0],
            args.d[1],
        )
        print(f" Download to <DEST> = {args.d[1]} successfully completed.")

    else:
        parser.error(f"Input args='{args}' is not defined!")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
