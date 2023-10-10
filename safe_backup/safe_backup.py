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
#	 http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 

import logging
import os
import boto3
from pathlib import Path
import redis
import argparse

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - \
  %(levelname)s -  %(message)s')

AWS_DEFAULT_REGION = os.environ['AWS_DEFAULT_REGION']
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
AWS_ENDPOINT_URL = os.environ['AWS_ENDPOINT_URL']
	
class SafeBackup:
	redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)

	def get_files_list(self, source, location):
		"""
			source must be:		local 				|	s3
			location must be:	source_directory	|	bucket_name
		"""
		files_path=""
		match source:
			case 's3':
				logging.debug("Source is a s3.")
				session = boto3.session.Session(
					aws_access_key_id=AWS_ACCESS_KEY_ID, 
					aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
					aws_session_token=None
				)

				s3_re = session.resource('s3',
					region_name=AWS_DEFAULT_REGION,
					endpoint_url=AWS_ENDPOINT_URL, 
					config=boto3.session.Config(signature_version='s3v4'), 
					verify=False
				)
				for bucket in s3_re.buckets.all():
					bucket = s3_re.Bucket(bucket.name)
					for obj in bucket.objects.all():
						logging.debug(f"{obj.key} {obj.last_modified}")
						self.redis_db.sadd(f"{source}-{location}", obj.key)

			case 'local':
				logging.debug("Source is a local.")
				if Path(location).is_dir() and Path(location).exists:
					logging.debug("Location is a directory.")
					root_path = location.split(os. sep)[-1]
					files_path = root_path
					for folderName, subfolders, filenames in os.walk(location):	
						logging.debug('1 ----------------------------------------------------')		
						logging.debug('The folderName folder is ' + folderName)
						logging.debug('The folderName.split(os. sep)[-1] folder is ' + folderName.split(os. sep)[-1])	
						logging.debug('The files_path folder is ' + files_path)	
						logging.debug('The files_path.split(os. sep)[-1] folder is ' + files_path.split(os. sep)[-1])
						logging.debug('2 ----------------------------------------------------')		
						if not folderName.split(os. sep)[-1] == files_path: 
							logging.debug('The folderName.split(os. sep)[-2] folder is ' + folderName.split(os. sep)[-2])	
							if folderName.split(os. sep)[-2] == files_path.split(os. sep)[-1]:	
								parent_path = files_path			
								files_path += f"/{folderName.split(os. sep)[-1]}"
								logging.debug('if   : The current files_path is ' + files_path)
							elif folderName.split(os. sep)[-2] == files_path.split(os. sep)[-2]:				
								files_path = f"{parent_path}/{folderName.split(os. sep)[-1]}"
								logging.debug('elif 1: The current files_path is ' + files_path)
							elif folderName.split(os. sep)[-2] == root_path:
								files_path = f"{root_path}/{folderName.split(os. sep)[-1]}"
								logging.debug('elif 2: The current files_path is ' + files_path)						

						for filename in filenames:
							logging.debug('FILE INSIDE ' + files_path + ': '+ filename)
							file_path = f"{files_path}/{filename}"
							# self.redis_db.rpush(root_path, file_path)
							self.redis_db.sadd(f"{source}-{root_path}", file_path)
					logging.debug(self.redis_db.keys())
				else:
					print("location is not directory or not exist.")
					exit(1)
			case _:
				print("Source is not valied.")


def main(args):
	parser = argparse.ArgumentParser(
				prog='safe_backup',
				description='Backup your local or s3 files safely.')

	
	parser.add_argument('source', metavar=('<SOURCE_TYPE>'), 
				help= 'get source type [\'local\' | \'s3\']')
	parser.add_argument('location', metavar=('<SOURCE_ADDRESS>'), 
				help= 'get [ <SOURCE_DIRECTORY> | <BUCKET_NAME> ] to create list of source files in Redis')
	parser.add_argument('-c', nargs=1, metavar=('<DEST_DIRECTORY>'),
				help= 'copy source files to destination')	
	
	args = parser.parse_args()
	logging.debug(args)	
	logging.debug(args.source)
	logging.debug(args.location)
	logging.debug(args.c)
	
	safe_backup = SafeBackup()
	if args.c == None:
		safe_backup.get_files_list(args.source,args.location)
	else:
		pass

	return 0

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
