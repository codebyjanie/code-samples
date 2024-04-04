# Upload File to S3

## Run export and send the files to the right place

To make this whole process works we need to run the export as always but in a different folder this time.
First we are going to create the following structure in s3-uploader folder:

` mkdir -p [FILES_FOLDER]`

Where [FOLDER] is the folder in charge to hold all the files that you want to upload to S3. You could use whatever name you want for this folder.

## Send folder to s3

Run the uploader script as shown in the following example:

```
./upload.py --bucket [BUCKET_NAME] --target-folder [TARGET_FOLDER_IN_S3] --use-date-paths --no-local-file-parent --metadata --no-main-local-folder --paths-to-upload [FILES_FOLDER]
```

It has several parameters because it was designed to be flexible. But here are the most important 2 parameters: `bucket` and `paths-to-upload`. The first one is the s3 bucket destination and the latter is the folder you want to upload.