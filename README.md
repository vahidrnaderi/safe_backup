
# Safe Backup

If you need to copy your files from a location on your local drives or download them from an object storage to your local drive safety, without any concerns about losing files, you can achieve this with safe_backup.

This program creates a secure backup of your files from a specified directory or an object storage location.

First, it generates a list of files in Redis and then begins the process of copying or downloading them to the destination while maintaining the same structure.

## Redis:

    $ docker run -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest

## MinIO:

Run MinIo container if you want make your own object storage lab and test the program.

**Full Guide:**  [MinIO Object Storage for Container](https://min.io/docs/minio/container/index.html)

    $ mkdir -p ${HOME}/minio/data

    $ docker run \
       -p 9000:9000 \
       -p 9090:9090 \
       --user $(id -u):$(id -g) \
       --name minio1 \
       -e "MINIO_ROOT_USER=ROOTUSER" \
       -e "MINIO_ROOT_PASSWORD=CHANGEME123" \
       -v ${HOME}/minio/data:/data \
       quay.io/minio/minio server /data --console-address ":9090"

and then open 'http://localhost:9000' in your browser, login and make your access key.

**guide:** [MinIO Object Storage Security and Access](https://min.io/docs/minio/linux/administration/console/security-and-access.html#id1)

## Environment variables:

    $ export SBACKUP_DB_URL                                                      #default "127.0.0.1:6379"
    $ export SBACKUP_DB_DECODE_RESPONSE                                          #default True
    
    $ export SBACKUP_AWS_DEFAULT_REGION = <AWS_DEFAULT_REGION>                   #default None for MinIO
    $ export SBACKUP_AWS_ACCESS_KEY_ID = <AWS_ACCESS_KEY_ID>                     #MinIO/S3 access key
    $ export SBACKUP_AWS_SECRET_ACCESS_KEY = <AWS_SECRET_ACCESS_KEY>             #MinIO/S3 secret key
    $ export SBACKUP_AWS_ENDPOINT_URL = <AWS_ENDPOINT_URL>                       #for MinIO lab set to 'http://localhost:9000'
    
    $ export SBACKUP_DEST_AWS_DEFAULT_REGION = <DEST_AWS_DEFAULT_REGION>         #default None for MinIO
    $ export SBACKUP_DEST_AWS_ACCESS_KEY_ID = <DEST_AWS_ACCESS_KEY_ID>           #MinIO/S3 access key
    $ export SBACKUP_DEST_AWS_SECRET_ACCESS_KEY = <DEST_AWS_SECRET_ACCESS_KEY>   #MinIO/S3 secret key
    $ export SBACKUP_DEST_AWS_ENDPOINT_URL = <DEST_AWS_ENDPOINT_URL>             #for MinIO set to 'http://localhost:9000'

## Install:
    $ pip install safe_backup

## Usage:
    $ sbackup [-h] [-L <LOG_LEVEL>] (-l <SOURCE_TYPE> <SOURCE_ADDRESS> | 
                                     -c <SOURCE_TYPE> <SOURCE_ADDRESS> <DEST> | 
                                     -d <DB_KEY> <DEST>
                                    )

Backup your local or s3 files safety.


**options:**

    -h, --help          show this help message and exit
    -L <LOG_LEVEL>       get <LOG_LEVEL> (NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL) and Activate logging level
    -l <SOURCE_TYPE> <SOURCE_ADDRESS>
                        get <SOURCE_TYPE> as ['local' | 's3'] and [ <SOURCE_DIRECTORY> | <BUCKET_NAME> ] to create list of source files in db
                        
    -c <SOURCE_TYPE> <SOURCE_ADDRESS> <DEST>
                        get <SOURCE_TYPE> as ['local' | 's3'] then <SOURCE_ADDRESS> as [ <SOURCE_DIRECTORY> | <BUCKET_NAME> ] and 
                        get <DEST> as [ <LOCAL_DIRECTORY> | s3:<BUCKET_NAME> ] to copy source files to destination
                        
    -d <DB_KEY> <DEST>
                        read db and download source files safety to <DEST> which can be a <LOCAL_DIRECTORY> or s3:<BUCKET_NAME>

___
