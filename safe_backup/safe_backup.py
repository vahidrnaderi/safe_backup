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
from multiprocessing import Pool
import functools

# import time


logging.basicConfig(
    level=logging.DEBUG,
    format=" %(asctime)s -  %(levelname)s -  %(message)s",
)


class SafeBackup:
    redis_db = redis.StrictRedis(
        host="localhost", port=6379, db=0, decode_responses=True
    )
    __region_dest = None

    def debug(func):
        """Print the function signature and return value"""

        @functools.wraps(func)
        def wrapper_debug(*args, **kwargs):
            args_repr = [repr(a) for a in args]  # 1
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]  # 2
            signature = ", ".join(args_repr + kwargs_repr)  # 3

            # Do something before
            logging.debug(f"------ Calling {func.__name__}({signature})")

            value = func(*args, **kwargs)

            # Do something after
            logging.debug(f"------ End of {func.__name__!r} \
                returned {value!r}")  # 4

            return value

        return wrapper_debug

    @debug
    def __init__(self, args):
        """
        Checking for intruption and continue the last command.
        """
        logging.debug(f" *********** args = {args} ######### ")
        redis_keys = self.redis_db.scan(0, "*:marker")[1]
        for key in redis_keys:
            logging.debug(
                # f" *********** key = {key.decode('UTF-8')} ######### "
                f" *********** key = {key} ######### "
            )
            # commands = key.decode("UTF-8").split(":")
            commands = key.split(":")
            command_array = commands[2].split("__")
            logging.debug(f" *********** commands = {commands} ######### ")
            logging.debug(f" *********** command_array = \
                {command_array} ######### ")
            if command_array[0] == "l":
                self.save_files_list_in_redis(
                    "l",
                    command_array[1],
                    command_array[2],
                    intruption=True,
                    first_marker=self.redis_db.get(key),
                )
            # first_marker=self.redis_db.get(key).decode("UTF-8"),
            elif command_array[0] == "c":
                self.save_files_list_in_redis(
                    "c",
                    command_array[1],
                    command_array[2],
                    commands[2],
                    intruption=True,
                    first_marker=self.redis_db.get(key),
                )
                # first_marker=self.redis_db.get(key).decode("UTF-8"),
                self.download_files_list_from_redis(
                    "d",
                    f"{commands[0]}:{commands[1]}",
                    command_array[3],
                    command_array[4],
                )

        redis_keys = self.redis_db.scan(0, "*-working1")[1]
        for key in redis_keys:
            # logging.debug(f" *********** {key.decode('UTF-8')} #########")
            logging.debug(f" *********** {key} #########")
            # keys = key.decode("UTF-8").split("-")
            keys = key.split("-")
            command = keys[1].split("__")
            logging.debug(f" *********** {keys} + {command} ######### ")
            self.download_files_list_from_redis(
                "d",
                keys[0],
                command[1],
                command[2]
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
                os.environ["AWS_SECRET_ACCESS_KEY"]
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
        logging.debug(args[1][0]["Key"])
        self.redis_db.sadd(args[0], args[1][0]["Key"])

    @debug
    def __pooled__(self, redis_key, page_contents):
        with Pool() as pool:
            args = [
                [redis_key, page_contents],
            ]
            pool.map(self.__make_redis_list_from_pages__, args)

    @debug
    def __s3_list_paginator__(
        self,
        bucket,
        command_key,
        page_items=1,
        max_items=None,
        first_marker=""
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
            logging.debug(f" ****************\n {page}\n ############ \n")
            logging.debug(f" **** Marker ************ {page['Marker']}")
            if page["IsTruncated"]:
                logging.debug(f" **** NextMarker ******** \
                {page['NextMarker']}")
            else:
                logging.debug(" **** NextMarker ******** ")

            if "Contents" in list(page.keys()):
                self.redis_db.set(
                    f"{redis_key}:{command_key}:marker", page["Marker"])
                self.__pooled__(redis_key, page["Contents"])
                # time.sleep(5)
        else:
            self.redis_db.getdel(f"{redis_key}:{command_key}:marker")

        return redis_key

    @debug
    def bucket_exists(self, bucket_name):
        s3_so = self.__s3_connect__()
        # bucket = s3_so.Bucket(bucket_name)
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
                logging.debug(" *** save_files_list_in_redis() \
                => Source is a s3.")
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
                            command_key)
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
                logging.debug(" *** save_files_list_in_redis() \
                    => Source is a local.")
                if Path(location).is_dir() and Path(location).exists:
                    logging.debug(
                        " *** save_files_list_in_redis() => \
                        Location is a directory."
                    )
                    root_path = location.split(os.sep)[-1]
                    files_path = root_path
                    for folderName, subfolders, filenames in os.walk(location):
                        logging.debug(
                            " *** save_files_...() => 1 \
                            -----------------------------------------------"
                        )
                        logging.debug(
                            " *** save_files_...() => \
                            The folderName folder is "
                            + folderName
                        )
                        logging.debug(
                            " *** save_files_...() => \
                            The folderName.split(os.sep)[-1] folder is "
                            + folderName.split(os.sep)[-1]
                        )
                        logging.debug(
                            " *** save_files_...() => \
                            The files_path folder is "
                            + files_path
                        )
                        logging.debug(
                            " *** save_files_...() => \
                            The files_path.split(os.sep)[-1] folder is "
                            + files_path.split(os.sep)[-1]
                        )
                        logging.debug(
                            " *** save_files_...() => \
                            2 ---------------------------\
                            -------------------------"
                        )
                        if not folderName.split(os.sep)[-1] == files_path:
                            logging.debug(
                                " *** save_files_...() => \
                                The folderName.split(os.sep)[-2] folder is "
                                + folderName.split(os.sep)[-2]
                            )
                            if (
                                folderName.split(os.sep)[-2]
                                == files_path.split(os.sep)[-1]
                            ):
                                parent_path = files_path
                                f = folderName.split(os.sep)[-1]
                                files_path += f"/{f}"
                                logging.debug(
                                    " *** save_files_...() => if : \
                                    The current files_path is "
                                    + files_path
                                )
                            elif (
                                folderName.split(os.sep)[-2]
                                == files_path.split(os.sep)[-2]
                            ):
                                f = folderName.split(os.sep)[-1]
                                files_path = (
                                    f"{parent_path}/{f}"
                                    )
                                logging.debug(
                                    " *** save_files_...() => \
                                    elif 1: The current files_path is "
                                    + files_path
                                )
                            elif folderName.split(os.sep)[-2] == root_path:
                                f = folderName.split(os.sep)[-1]
                                files_path = (
                                    f"{root_path}/{f}"
                                )
                                logging.debug(
                                    " *** save_files_...() => \
                                    elif 2: The current files_path is "
                                    + files_path
                                )

                        for filename in filenames:
                            logging.debug(
                                " *** save_files_...() => FILE INSIDE "
                                + files_path
                                + ": "
                                + filename
                            )
                            file_path = f"{files_path}/{filename}"
                            redis_key = f"{source}:{location}"
                            self.redis_db.sadd(redis_key, file_path)
                        # time.sleep(5)
                    logging.debug(f" *** save_files_...() \
                    => {self.redis_db.keys()}")
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
        workers
    ):
        source = redis_key.split(":")
        logging.debug(f" *** download_files_...()=> {source}")
        for member in self.redis_db.sscan(redis_key, 0)[1]:
            self.redis_db.set(
                f"{redis_key}-{option}__{destination}__{workers}-working1",
                member
            )
            if not destination.startswith("s3:"):
                logging.debug(
                    f"*** download_files_...()=> \
                    {Path(source[1]).parent}/{member} \
                    --> {destination}/{member}"
                )
                parent = Path(f"{destination}/{member}").parent
                if not os.path.exists(parent):
                    os.makedirs(parent)
                self.redis_db.smove(
                    redis_key,
                    f"{redis_key}-{option}__{destination}__{workers}-working",
                    member,
                )
                match source[0]:
                    case "local":
                        try:
                            shutil.copy2(
                                f"{Path(source[1]).parent}/{member}",
                                f"{destination}/{member}",
                            )
                            r = redis_key
                            o = option
                            d = destination
                            self.redis_db.srem(
                                f"{r}-{o}__{d}__{workers}-working",
                                member,
                            )
                        except Exception as e:
                            print(f"There was an error: {e}")
                            r = redis_key
                            o = option
                            d = destination
                            self.redis_db.smove(
                                f"{r}-{o}__{d}__{workers}-working",
                                redis_key,
                                member,
                            )
                    case "s3":
                        s3_source = self.__s3_connect__().meta.client
                        try:
                            s3_source.download_file(
                                source[1],
                                member,
                                f"{destination}/{member}",
                            )
                            r = redis_key
                            o = option
                            d = destination
                            self.redis_db.srem(
                                f"{r}-{o}__{d}__{workers}-working",
                                member,
                            )
                        except Exception as e:
                            print(f"There was an error: {e}")
                            r = redis_key
                            o = option
                            d = destination
                            self.redis_db.smove(
                                f"{r}-{o}__{d}__{workers}-working",
                                redis_key,
                                member,
                            )

            elif redis_key.startswith("s3:") and destination.startswith("s3:"):
                s3_dest_bucket = destination.split(":")[1]
                logging.debug(
                    f" *** {member} --> \
                    s3:{s3_dest_bucket}"
                )
                parent = Path(f"./{destination}/{member}").parent
                if not os.path.exists(parent):
                    os.makedirs(parent)
                self.redis_db.smove(
                    redis_key,
                    f"{redis_key}-{option}__{destination}__{workers}-working",
                    member,
                )

                # download from s3 source
                s3_source = self.__s3_connect__().meta.client
                logging.debug(f" *** {source[1]} -> ./{destination}/{member}")
                try:
                    s3_source.download_file(
                        source[1],
                        member,
                        f"./{destination}/{member}",
                    )
                except Exception as e:
                    print(f" There was an error: {e}")
                    r = redis_key
                    o = option
                    d = destination
                    self.redis_db.smove(
                        f"{r}-{o}__{d}__{workers}-working",
                        redis_key,
                        member,
                    )

                # upload to s3 destination
                s3_dest = self.__s3_connect__("dest").meta.client
                try:
                    logging.debug(
                        f" *** {s3_dest.list_buckets()['Buckets']}")
                    s3_dest.head_bucket(Bucket=s3_dest_bucket)
                except ClientError:
                    # The bucket does not exist or you have no access.
                    if not self.__create_bucket__(
                        s3_dest, s3_dest_bucket, self.__region_dest
                    ):
                        print(
                            "  ######  There was a problem to \
                            create destination bucket!"
                        )
                        exit(1)
                else:
                    logging.debug(
                        f" *** {member} -> \
                        {member}"
                    )
                    if os.path.exists(Path(f"./{destination}/{member}")):
                        try:
                            s3_dest.upload_file(
                                f"./{destination}/{member}",
                                s3_dest_bucket,
                                member,
                            )
                            r = redis_key
                            o = option
                            d = destination
                            self.redis_db.srem(
                                f"{r}-{o}__{d}__{workers}-working",
                                member,
                            )
                        except ClientError as e:
                            print(f" There was an error: {e}")
                            r = redis_key
                            o = option
                            d = destination
                            self.redis_db.smove(
                                f"{r}-{o}__{d}__{workers}-working",
                                redis_key,
                                member,
                            )
                        else:
                            path = Path(f"./{destination}/{member}")
                            A = os.path.exists(path)
                            if A and not os.path.isfile(path):
                                if not os.listdir(path):
                                    os.rmdir(path.parent)
                            elif os.path.isfile(path):
                                os.unlink(path)
                    else:
                        print(f" The file ./{destination}/{member} \
                        not exists!")
            else:
                print(" There was a problem for copy s3 to s3!")
                exit(2)
            # time.sleep(5)
        else:
            self.redis_db.getdel(
                f"{redis_key}-{option}__{destination}__{workers}-working1"
            )

        if destination.startswith("s3:"):
            shutil.rmtree(Path(f"./{destination}"))

    @debug
    def copy_files(self, option, source, location, destination, workers):
        """
        Make a list of files in redis and then start copying or
        downloading files to the destination.
        """

        o = option
        d = destination
        command_key = f"{o}__{source}__{location}__{d}__{workers}"

        "Make list of source files to Redis"
        redis_key = self.save_files_list_in_redis(
            option,
            source,
            location,
            command_key)

        "Download or copy source files list that we made before in Redis"
        self.download_files_list_from_redis(
            "d",
            redis_key,
            destination,
            workers
            )


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
        nargs=4,
        metavar=(
            "<SOURCE_TYPE>",
            "<SOURCE_ADDRESS>",
            "<DEST_DIRECTORY>",
            "<NUMBER_OF_WORKERS>",
        ),
        help="get <SOURCE_TYPE> as ['local' | 's3'] and \
        [ <SOURCE_DIRECTORY> | <BUCKET_NAME> ] \
        then copy source files to destination",
    )
    group.add_argument(
        "-d",
        nargs=3,
        metavar=("<REDIS_KEY>", "<DEST>", "<NUMBER_OF_WORKERS>"),
        help="read Redis and download source files safety to <DEST> \
        which can be a <Directory> or s3:<bucket_name>",
    )

    args = parser.parse_args()
    logging.debug(f"main() *** {args}")
    logging.debug(f"main() *** {args.l}")
    logging.debug(f"main() *** {args.c}")
    logging.debug(f"main() *** {args.d}")

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
        safe_backup.save_files_list_in_redis("l", args.l[0], args.l[1])

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
        if not Path(args.c[2]).is_dir():
            parser.error(
                f"<DEST_DIRECTORY>='{args.c[2]}' \
                is not directory or not exist!"
            )
        if not args.c[3].isdigit() or int(args.c[3]) <= 0:
            parser.error(
                f"<NUMBER_OF_WORKERS>='{args.c[2]}' \
                is not integer or not bigger than 0!"
            )

        "All copy functions must write down here"
        safe_backup.copy_files("c", args.c[0], args.c[1], args.c[2], args.c[3])

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
        if not args.d[2].isdigit() or int(args.d[2]) <= 0:
            parser.error(
                f"<NUMBER_OF_WORKERS>='{args.d[2]}' \
                is not integer or not bigger than 0!"
            )

        "Download or copy source files list that we made before in Redis"
        safe_backup.download_files_list_from_redis(
            "d",
            args.d[0],
            args.d[1],
            args.d[2])

    else:
        parser.error(f"Input args='{args}' is not defined!")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
