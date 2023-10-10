#Safe Backup

If you need to copy your files from a location on your local drives or download them from an object storage to your local drive safely, without any concerns about losing files, you can achieve this with safe_backup.

This program creates a secure backup of your files from a specified directory or an object storage location.

First, it generates a list of files in Redis and then begins the process of copying or downloading them to the destination while maintaining the same structure.
____

usage: safe_backup [-h] [-c DEST_DIRECTORY] SOURCE_TYPE SOURCE_ADDRESS

Backup your local or s3 files safely.

positional arguments:
  SOURCE_TYPE        get source type ['local' | 's3']
  SOURCE_ADDRESS     [SOURCE_DIRECTORY | BUCKET_NAME] to create list of source files in Redis

options:
  -h, --help         show this help message and exit
  -c DEST_DIRECTORY  copy source files to destination
___


