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
from botocore.client import ClientError
from pathlib import Path
import redis
import argparse

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - \
  %(levelname)s -  %(message)s')


class SafeBackup:
	redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
	__region_dest = None
	
	def __s3_connect__(self, destination='source'):
		if destination=='source':
			AWS_DEFAULT_REGION = os.environ['AWS_DEFAULT_REGION']
			AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
			AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
			AWS_ENDPOINT_URL = os.environ['AWS_ENDPOINT_URL']
		elif destination=='dest':
			AWS_DEFAULT_REGION = os.getenv('DEST_AWS_DEFAULT_REGION',
								os.environ['AWS_DEFAULT_REGION'])
			region_dest = AWS_DEFAULT_REGION
			AWS_ACCESS_KEY_ID = os.getenv('DEST_AWS_ACCESS_KEY_ID',
								os.environ['AWS_ACCESS_KEY_ID'])
			AWS_SECRET_ACCESS_KEY = os.getenv('DEST_AWS_SECRET_ACCESS_KEY',
									os.environ['AWS_SECRET_ACCESS_KEY'])
			AWS_ENDPOINT_URL = os.getenv('DEST_AWS_ENDPOINT_URL',
									os.environ['AWS_ENDPOINT_URL'])
		else:
			print(f"The s3 destination={destination} is not defined.")
			exit(1)
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
		
	def __create_bucket__(self, s3_client, bucket_name, region=None):
		"""Create an S3 bucket in a specified region

		If a region is not specified, the bucket is created in the S3 default
		region (us-east-1).

		:param bucket_name: Bucket to create
		:param region: String region to create bucket in, e.g., 'us-west-2'
		:return: True if bucket created, else False
		"""

		# Create bucket
		try:
			if region is None:
				# s3_client = boto3.client('s3')
				s3_client.create_bucket(Bucket=bucket_name)
			else:
				# s3_client = boto3.client('s3', region_name=region)
				location = {'LocationConstraint': region}
				s3_client.create_bucket(Bucket=bucket_name,
										CreateBucketConfiguration=location)
		except ClientError as e:
			logging.error(e)
			return False
		return True
		
	def __make_redis_list_from_pages__(self, redis_key, page_contetnts):		
		for content in page_contetnts:
			logging.debug(content['Key'])
			self.redis_db.sadd(redis_key, content['Key'])
	
	def __s3_list_paginator__(self, bucket, page_items=1, max_items=None, first_marker=''):	
		# Create a client
		s3_so = self.__s3_connect__().meta.client
		
		# Create a reusable Paginator
		paginator = s3_so.get_paginator('list_objects')
		
		# Create and Customizing page iterators
		page_iterator = paginator.paginate(Bucket = bucket.name,
										   PaginationConfig={'PageSize': page_items,
															 'StartingToken': first_marker,
															 'MaxItems': max_items
														   }                                   
										   )
				
		redis_key = f"s3:{bucket.name}"
		
		for page in page_iterator:
			logging.debug(f" ****************\n {page}\n ############ \n")
			logging.debug(f" **** Marker ************ {page['Marker']}")
			if page['IsTruncated']:
				logging.debug(f" **** NextMarker ******** {page['NextMarker']}")
			else:
				logging.debug(f" **** NextMarker ******** ")
				
			if 'Contents' in list(page.keys()):
				self.__make_redis_list_from_pages__(redis_key, page['Contents'])
	
	def save_files_list_in_redis(self, source, location):
		"""
			Make a list from source and save it in redis.
			source must be one of 'local' or 's3'
			
			if source is 'local' then
					<location> must be <source_directory>	
			and if source is 's3' then
					<location> must be: 
							<bucket_name> or '*' for all buckets
					
		"""
		files_path=""
		redis_key = ""
		match source:
			case 's3':
				logging.debug(" *** save_files_list_in_redis() => Source is a s3.")
				s3_so = self.__s3_connect__()
				if location == '*':
					for bucket in s3_so.buckets.all():
						self.__s3_list_paginator__(bucket)
				else:
					bucket = s3_so.Bucket(location)
					self.__s3_list_paginator__(bucket)

			case 'local':
				logging.debug(" *** save_files_list_in_redis() => Source is a local.")
				if Path(location).is_dir() and Path(location).exists:
					logging.debug(" *** save_files_list_in_redis() => Location is a directory.")
					root_path = location.split(os. sep)[-1]
					files_path = root_path
					for folderName, subfolders, filenames in os.walk(location):	
						logging.debug(' *** save_files_...() => 1 ----------------------------------------------------')		
						logging.debug(' *** save_files_...() => The folderName folder is ' + folderName)
						logging.debug(' *** save_files_...() => The folderName.split(os. sep)[-1] folder is ' + folderName.split(os. sep)[-1])	
						logging.debug(' *** save_files_...() => The files_path folder is ' + files_path)	
						logging.debug(' *** save_files_...() => The files_path.split(os. sep)[-1] folder is ' + files_path.split(os. sep)[-1])
						logging.debug(' *** save_files_...() => 2 ----------------------------------------------------')		
						if not folderName.split(os. sep)[-1] == files_path: 
							logging.debug(' *** save_files_...() => The folderName.split(os. sep)[-2] folder is ' + folderName.split(os. sep)[-2])	
							if folderName.split(os. sep)[-2] == files_path.split(os. sep)[-1]:	
								parent_path = files_path			
								files_path += f"/{folderName.split(os. sep)[-1]}"
								logging.debug(' *** save_files_...() => if   : The current files_path is ' + files_path)
							elif folderName.split(os. sep)[-2] == files_path.split(os. sep)[-2]:				
								files_path = f"{parent_path}/{folderName.split(os. sep)[-1]}"
								logging.debug(' *** save_files_...() => elif 1: The current files_path is ' + files_path)
							elif folderName.split(os. sep)[-2] == root_path:
								files_path = f"{root_path}/{folderName.split(os. sep)[-1]}"
								logging.debug(' *** save_files_...() => elif 2: The current files_path is ' + files_path)						

						for filename in filenames:
							logging.debug(' *** save_files_...() => FILE INSIDE ' + files_path + ': '+ filename)
							file_path = f"{files_path}/{filename}"
							# self.redis_db.rpush(root_path, file_path)
							# redis_key = f"{source}:{root_path}"
							redis_key = f"{source}:{location}"
							self.redis_db.sadd(redis_key, file_path)
					logging.debug(f" *** save_files_...() => {self.redis_db.keys()}")					
					print(f"List of files created in '{redis_key}' redis key successfuly.")
				else:
					print("location is not directory or not exist.")
					exit(1)
			case _:
				print("Source is not valied.")
		return redis_key


	def download_files_list_from_redis(self, redis_key, destination, workers):
		source=redis_key.split(':')
		logging.debug(f" *** download_files_...()=> {source}")
		for member in self.redis_db.sscan(redis_key,0)[1]:
			if not destination.startswith('s3:'):
				logging.debug(f"*** download_files_...()=> {Path(source[1]).parent}/{member.decode('UTF-8')} \
					--> {destination}/{member.decode('UTF-8')}")
				parent = Path(f"{destination}/{member.decode('UTF-8')}").parent
				if not os.path.exists(parent):
					os.makedirs(parent)
				self.redis_db.smove(redis_key, f"{redis_key}-working", member)
				match source[0]:
					case 'local':				
						try:
							shutil.copy2(f"{Path(source[1]).parent}/{member.decode('UTF-8')}", 
								f"{destination}/{member.decode('UTF-8')}")
							self.redis_db.srem(f"{redis_key}-working", member)
						except Exception as e:
							print(f"There was an error: {e}")
							self.redis_db.smove(f"{redis_key}-working",
												redis_key, member)
					case 's3':
						s3_source = self.__s3_connect__().meta.client
						try:
							s3_source.download_file(source[1],
								member.decode('UTF-8'),
								f"{destination}/{member.decode('UTF-8')}")
							self.redis_db.srem(f"{redis_key}-working", member)
						except Exception as e:
							print(f"There was an error: {e}")
							self.redis_db.smove(f"{redis_key}-working",
										redis_key, member)
							
			elif redis_key.startswith('s3:') and destination.startswith('s3:'):
				s3_dest_bucket=destination.split(':')[1]
				logging.debug(f" *** l-188 {member.decode('UTF-8')} --> s3:{s3_dest_bucket}")
				parent = Path(f"./{destination}/{member.decode('UTF-8')}").parent
				if not os.path.exists(parent):
					os.makedirs(parent)
				self.redis_db.smove(redis_key, f"{redis_key}-working", member)
				
				# download from s3 source
				s3_source = self.__s3_connect__().meta.client
				logging.debug(f" *** l-197 {source[1]} -> ./{destination}/{member.decode('UTF-8')}")
				try:
					logging.debug(f" *** l-199")
					s3_source.download_file(source[1],
						member.decode('UTF-8'),
						f"./{destination}/{member.decode('UTF-8')}")
				except Exception as e:
					print(f" There was an error: {e}")
					self.redis_db.smove(f"{redis_key}-working",
						redis_key, member)				
				logging.debug(f" *** l-207")
				
				# upload to s3 destination	
				s3_dest = self.__s3_connect__('dest').meta.client
				try:
					logging.debug(f" *** l-212 {s3_dest.list_buckets()['Buckets']}")
					s3_dest.head_bucket(Bucket=s3_dest_bucket)
				except ClientError:
					# The bucket does not exist or you have no access.
					if not self.__create_bucket__(s3_dest, s3_dest_bucket, self.__region_dest):
						print("  ######  There was a problem to create destination bucket!")	
						exit(1)
				else:					
					logging.debug(f" *** l-220 {member.decode('UTF-8')} -> {member.decode('UTF-8')}")
					if os.path.exists(Path(f"./{destination}/{member.decode('UTF-8')}")):
						try:
							logging.debug(f" *** l-222")
							s3_dest.upload_file(f"./{destination}/{member.decode('UTF-8')}",
								s3_dest_bucket, member.decode('UTF-8'))
							self.redis_db.srem(f"{redis_key}-working", member)
						except ClientError as e:
							print(f" There was an error: {e}")
							self.redis_db.smove(f"{redis_key}-working",
								redis_key, member)
						else:
							path = Path(f"./{destination}/{member.decode('UTF-8')}")
							if os.path.exists(path) and not os.path.isfile(path):
								if not os.listdir(path): 
									os.rmdir(path.parent)
							elif os.path.isfile(path): 
								os.unlink(path)																			
					else:
						print(f" The file ./{destination}/{member.decode('UTF-8')} not exists!")					
					logging.debug(f" *** l-220")	
				
			else:
				print(" There was a problem for copy s3 to s3!")	
				exit(2)	
		
		if destination.startswith('s3:'):	
			shutil.rmtree(Path(f"./{destination}"))			



def main():
	parser = argparse.ArgumentParser(
				prog='safe_backup',
				description='Backup your local or s3 files safely.')

	group = parser.add_mutually_exclusive_group(required=True)
		
	group.add_argument('-l', nargs=2, metavar=('<SOURCE_TYPE>',
				'<SOURCE_ADDRESS>'), 
				help= 'get <SOURCE_TYPE> as [\'local\' | \'s3\'] \
				and [ <SOURCE_DIRECTORY> | [ <BUCKET_NAME> | \'*\' ] ] \
				to create list of source files in Redis')
	group.add_argument('-c', nargs=5, metavar=('<SOURCE_TYPE>',
				'<SOURCE_ADDRESS>', '<DEST_DIRECTORY>', '<REDIS_KEY>', 
				'<NUMBER_OF_WORKERS>'),
				help= 'get <SOURCE_TYPE> as [\'local\' | \'s3\'] \
				and [ <SOURCE_DIRECTORY> | [ <BUCKET_NAME> | \'*\' ] ] \
				then copy source files to destination')
	group.add_argument('-d', nargs=3, metavar=('<REDIS_KEY>', 
				'<DEST>', '<NUMBER_OF_WORKERS>'),
				help= 'read Redis and download source files safety to \
				<DEST> which can be a <Directory> or s3:<bucket_name>')	
	
	args = parser.parse_args()
	logging.debug(f"main() *** {args}")	
	logging.debug(f"main() *** {args.l}")
	logging.debug(f"main() *** {args.c}")
	logging.debug(f"main() *** {args.d}")
	
	safe_backup = SafeBackup()
	
	if args.l:
		if not args.l[0]=='local' and not args.l[0]=='s3':
			parser.error(f"<SOURCE_TYPE> must be one of 'local' or 's3'")
		if args.l[0]=='local' and not Path(args.l[1]).is_dir():
			parser.error(f"<SOURCE_ADDRESS>='{args.l[1]}' is not directory or not exist!")
		safe_backup.save_files_list_in_redis(args.l[0],args.l[1])
	elif args.c:
		if not args.c[0]=='local' and not args.c[0]=='s3':
			parser.error(f"<SOURCE_TYPE> must be one of 'local' or 's3'")
		if args.c[0]=='local' and not Path(args.c[1]).is_dir():
			parser.error(f"<SOURCE_ADDRESS>='{args.c[1]}' is not \
			directory or not exist!")
		if not Path(args.c[2]).is_dir():
			parser.error(f"<DEST_DIRECTORY>='{args.c[2]}' is not \
			directory or not exist!")
	elif args.d:
		if not safe_backup.redis_db.exists(args.d[0])==1:
			parser.error(f"<REDIS_KEY>='{args.d[0]}' is not exists!")
		if not args.d[1].startswith('s3:'):
			if not Path(args.d[1]).is_dir():
				parser.error(f"<DEST>='{args.d[1]}' is not directory \
				or not started with 's3:'!")
		elif not len(args.d[1])>3:
			parser.error(f"You must define the <bucket_name> after 's3:'!")
		if not args.d[2].isdigit() or int(args.d[2])<=0:
			parser.error(f"<NUMBER_OF_WORKERS>='{args.d[2]}' is not \
			integer or not bigger than 0!")
		safe_backup.download_files_list_from_redis(args.d[0], args.d[1], args.d[2])
	else:
		parser.error(f"Input args='{args}' is not defined!")
		

	return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
