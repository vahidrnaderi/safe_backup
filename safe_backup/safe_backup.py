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
import os, shutil
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
	
	def __s3_connect__(self):
		session = boto3.session.Session(
			aws_access_key_id=AWS_ACCESS_KEY_ID, 
			aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
			aws_session_token=None
		)

		return session.resource('s3',
			region_name=AWS_DEFAULT_REGION,
			endpoint_url=AWS_ENDPOINT_URL, 
			config=boto3.session.Config(signature_version='s3v4'), 
			verify=False
		)
	
	def save_files_list_in_redis(self, source, location):
		"""
			Make a list from source and save it in redis.
			source must be one of 'local' or 's3'
			
			if source is 'local' then
					<location> must be <source_directory>	
			and if source is 's3' then
					<location> must be <bucket_name>
		"""
		files_path=""
		redis_key = ""
		match source:
			case 's3':
				logging.debug("Source is a s3.")
				s3_re = self.__s3_connect__()
				for bucket in s3_re.buckets.all():
					bucket = s3_re.Bucket(bucket.name)
					for obj in bucket.objects.all():
						logging.debug(f"{obj.key} {obj.last_modified}")
						redis_key = f"{source}:{location}"
						self.redis_db.sadd(redis_key, obj.key)

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
							# redis_key = f"{source}:{root_path}"
							redis_key = f"{source}:{location}"
							self.redis_db.sadd(redis_key, file_path)
					logging.debug(self.redis_db.keys())
				else:
					print("location is not directory or not exist.")
					exit(1)
			case _:
				print("Source is not valied.")
		return redis_key


	def download_files_list_from_redis(self, redis_key, destination, workers):
		source=redis_key.split(':')
		logging.debug(source)
		for member in self.redis_db.sscan(redis_key,0)[1]:
			logging.debug(f"{Path(source[1]).parent}/{member.decode('UTF-8')} --> {destination}/{member.decode('UTF-8')}")
			parent = Path(f"{destination}/{member.decode('UTF-8')}").parent
			if not os.path.exists(parent):
				os.makedirs(parent)
			self.redis_db.smove(redis_key, f"{redis_key}-working", member)
			match source[0]:
				case 'local':				
					try:
						shutil.copy2(f"{Path(source[1]).parent}/{member.decode('UTF-8')}", f"{destination}/{member.decode('UTF-8')}")
						self.redis_db.srem(f"{redis_key}-working", member)
					except Exception as e:
						print(f"There was an error: {e}")
						self.redis_db.smove(f"{redis_key}-working", redis_key, member)
					# if shutil.copy2(f"{Path(source[1]).parent}/{member.decode('UTF-8')}", f"{destination}/{member.decode('UTF-8')}"):
						# self.redis_db.srem(f"{redis_key}-working", member)
					# else:	
						# self.redis_db.smove(f"{redis_key}-working", redis_key, member)
				case 's3':
					s3_dest = self.__s3_connect__().meta.client
					try:
						# shutil.copy2(f"{Path(source[1]).parent}/{member.decode('UTF-8')}", f"{destination}/{member.decode('UTF-8')}")
						s3_dest.download_file(source[1], member.decode('UTF-8'), f"{destination}/{member.decode('UTF-8')}")
						self.redis_db.srem(f"{redis_key}-working", member)
					except Exception as e:
						print(f"There was an error: {e}")
						self.redis_db.smove(f"{redis_key}-working", redis_key, member)
	



def main():
	parser = argparse.ArgumentParser(
				prog='safe_backup',
				description='Backup your local or s3 files safely.')

	group = parser.add_mutually_exclusive_group(required=True)
		
	group.add_argument('-l', nargs=2, metavar=('<SOURCE_TYPE>', '<SOURCE_ADDRESS>'), 
				help= 'get <SOURCE_TYPE> as [\'local\' | \'s3\'] \
				and [ <SOURCE_DIRECTORY> | <BUCKET_NAME> ] \
				to create list of source files in Redis')
	group.add_argument('-c', nargs=5, metavar=('<SOURCE_TYPE>', \
				'<SOURCE_ADDRESS>', '<DEST_DIRECTORY>', '<REDIS_KEY>', 
				'<NUMBER_OF_WORKERS>'),
				help= 'get <SOURCE_TYPE> as [\'local\' | \'s3\'] \
				and [ <SOURCE_DIRECTORY> | <BUCKET_NAME> ] \
				then copy source files to destination')
	# *** write copy from s3 to s3
	group.add_argument('-d', nargs=3, metavar=('<REDIS_KEY>', 
				'<DEST_DIRECTORY>', '<NUMBER_OF_WORKERS>'),
				help= 'read Redis and download source files safely to destination')	
	
	args = parser.parse_args()
	logging.debug(args)	
	logging.debug(args.l)
	logging.debug(args.c)
	logging.debug(args.d)
	
	safe_backup = SafeBackup()
	
	if args.l:
		if not args.l[0]=='local' and not args.l[0]=='s3':
			parser.error(f"<SOURCE_TYPE> must be one of 'local' or 's3'")
		if args.l[0]=='local' and not Path(args.l[1]).is_dir():
			parser.error(f"<SOURCE_ADDRESS>='{args.l[1]}' is not directory or not exist!")
		result = safe_backup.save_files_list_in_redis(args.l[0],args.l[1])
		print(f"List of files created in '{result}' redis key successfuly.")
	elif args.c:
		if not args.c[0]=='local' and not args.c[0]=='s3':
			parser.error(f"<SOURCE_TYPE> must be one of 'local' or 's3'")
		if args.c[0]=='local' and not Path(args.c[1]).is_dir():
			parser.error(f"<SOURCE_ADDRESS>='{args.c[1]}' is not directory or not exist!")
		if not Path(args.c[2]).is_dir():
			parser.error(f"<DEST_DIRECTORY>='{args.c[2]}' is not directory or not exist!")
	elif args.d:
		if not safe_backup.redis_db.exists(args.d[0])==1:
			parser.error(f"<REDIS_KEY>='{args.d[0]}' is not exists!")
		if not Path(args.d[1]).is_dir():
			parser.error(f"<DEST_DIRECTORY>='{args.d[1]}' is not directory or not exist!")
		if not args.d[2].isdigit() or int(args.d[2])<=0:
			parser.error(f"<NUMBER_OF_WORKERS>='{args.d[2]}' is not integer or not bigger than 0!")
		safe_backup.download_files_list_from_redis(args.d[0], args.d[1], args.d[2])
	else:
		parser.error(f"Input args='{args}' is not defined!")
		

	return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
