
# Safe Backup

If you need to copy your files from a location on your local drives or download them from an object storage to your local drive safely, without any concerns about losing files, you can achieve this with safe_backup.

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

	$ export AWS_DEFAULT_REGION = <AWS_DEFAULT_REGION>		#default None for MinIO
	$ export AWS_ACCESS_KEY_ID = <AWS_ACCESS_KEY_ID>		#MinIO access key
	$ export AWS_SECRET_ACCESS_KEY = <AWS_SECRET_ACCESS_KEY>	#MinIO secret key
	$ export AWS_ENDPOINT_URL = <AWS_ENDPOINT_URL>			#for MinIO set to 'http://localhost:9000'

## Usage:

	$ python3 safe_backup [-h] [-c <DEST_DIRECTORY>] <SOURCE_TYPE> <SOURCE_ADDRESS>

Backup your local or s3 files safely.

**positional arguments:**

		<SOURCE_TYPE>       get source type ['local' | 's3']
		<SOURCE_ADDRESS>    get  [<SOURCE_DIRECTORY> | <BUCKET_NAME>] to create list of source files in Redis

**options:**

	  -h, --help           show this help message and exit
	  -c <DEST_DIRECTORY>  copy source files to destination
___
