#!/usr/bin/env python3

from botocore.exceptions import ClientError
from datetime import date

import argparse
import boto3
import csv
import io

import ntpath
import os
import sys

s3 = boto3.client('s3')

METADATA_HEADERS = [
   'Path',
   'File',
   'Date'
]

class S3File(object):
    def __init__(self, bucket, path):
        super().__init__()
        self.bucket = bucket
        self.path = path

    def __enter__(self):
        self.buffer = io.BytesIO()
        try:
            s3.download_fileobj(self.bucket, self.path, self.buffer)
        except ClientError:
            print(f"File [{self.bucket}]/{self.path} was not found")
        else:
            self.buffer.seek(0, os.SEEK_END)
        return io.TextIOWrapper(self.buffer, encoding='utf-8', write_through=True)

    def __exit__(self, type, value, traceback):
        self.buffer.seek(0)
        s3.upload_fileobj(self.buffer, self.bucket, self.path)
        self.buffer.close()
        print(f"Wrote record to [{self.bucket}]/{self.path}")

def get_filepaths(directory):
    file_paths = []

    for root, directories, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)

    return file_paths

def upload_file(bucket, file, base_path = '', ignore_file_parent_folder = False, ignore_main_local_folder = False):
   path, filename = ntpath.split(file)

   if ignore_file_parent_folder:
      path = "/".join(path.split("/")[:-1])

   if ignore_file_parent_folder:
      path = "/".join(path.split("/")[1:])
   
   if base_path:
      base_path = base_path + "/"
   
   path_in_bucket = base_path + path  
   filename_in_bucket = path_in_bucket + "/" + filename if (path_in_bucket[-1] != "/") else path_in_bucket + filename
   s3 = boto3.client('s3')
   print(f'Uploading file {file}...')
   s3.upload_file(file, bucket, filename_in_bucket)

   return path_in_bucket, filename

def upload_path(bucket, path, base_path, ignore_file_parent_folder, ignore_main_local_folder):
   metadata_info = []

   children_filenames = get_filepaths(path)
   for child_filename in children_filenames:
      path_in_bucket, filename_in_bucket = upload_file(bucket, child_filename, base_path, ignore_file_parent_folder, ignore_main_local_folder)
      metadata_info.append({ 'path': path_in_bucket, 'file': filename_in_bucket })
   
   return metadata_info


if __name__ == '__main__':
   parser = argparse.ArgumentParser(description="Script to upload files to s3 bucket.")
   
   parser.add_argument('--bucket', help='s3 bucket name', required=True)
   parser.add_argument('--target-folder', help='Where in the bucket these files are gonna be uploaded.')
   parser.add_argument('--use-date-paths', help='To organize the files uploaded with the following structure /year=CurrentYear/month=CurrentMonth/day=CurrentDay', action="store_true")
   parser.add_argument('--no-local-file-parent', help='Ignore local file parent. example: local file grand_parent/parent/child.txt will be uploaded as grand_parent/child.txt', action="store_true")
   parser.add_argument('--metadata', help='Generate and upload a metadata file with the path and name for the files uploaded in the folder', action="store_true")
   parser.add_argument('--no-main-local-folder', help='To do not include main local folder in the bucket directory structure. excample: local file main-folder/a/b/file.txt will be uploaded as /a/b/file.txt', action="store_true")
   parser.add_argument('--paths-to-upload', help='local path to be uploaded', nargs='+', action='store', required=True)

   args = parser.parse_args()

   bucket = args.bucket
   main_folder_in_bucket = args.target_folder
   use_date_bucket_paths = args.use_date_paths
   ignore_file_parent_folder = args.no_local_file_parent
   generate_metadata = args.metadata
   ignore_main_local_folder = args.no_main_local_folder

   paths = args.paths_to_upload

   target_bucket_folder = main_folder_in_bucket

   run_date = date.today()

   year = run_date.year
   month = run_date.month
   day = run_date.day

   if use_date_bucket_paths:
      target_bucket_folder = "/".join([main_folder_in_bucket,f'year={year}',f'month={month}',f'day={day}'])

   metadata_info = []

   for path in paths:
      str_path = str(path)
      metadata_info_path = upload_path(bucket, str_path, target_bucket_folder, ignore_file_parent_folder, ignore_main_local_folder)
      metadata_info = metadata_info + metadata_info_path
   
   if generate_metadata:
      metadata_file_name = f'{main_folder_in_bucket}/metadata-{year}-{month}.csv'
      
      with S3File(bucket, metadata_file_name) as buf:
               writer = csv.writer(buf)

               for metadata_path in metadata_info:
                  if buf.tell() == 0:
                     writer.writerow(METADATA_HEADERS)
                  writer.writerow([metadata_path['path'], metadata_path['file'], run_date])