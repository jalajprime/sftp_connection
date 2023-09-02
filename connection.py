import paramiko
import boto3
from botocore.exceptions import NoCredentialsError

# SFTP configuration
sftp_host = 'gfts.gilead.com'
sftp_port = 22
sftp_username = 'ext_gftzsgdprd_svc'
sftp_password = 'ECn3#|7L^k#2'
sftp_remote_path = '/files_from_gilead_to_zs_odm'

# S3 configuration
s3_bucket_name = 'aws-a0217-use1-00-p-s3b-gild-cus-data03'
s3_prefix = 'Temp_folder/'

# Establish SFTP connection
sftp = paramiko.Transport((sftp_host, sftp_port))
sftp.connect(username=sftp_username, password=sftp_password)
sftp_client = paramiko.SFTPClient.from_transport(sftp)

# List remote files
remote_files = sftp_client.listdir(sftp_remote_path)

#Selecting the files with maximum date
pattern = r'_([0-9]{8})\.txt'
file_dates={}
for filename in remote_files:
    match = re.search(pattern, filename)
    if match:
        date = match.group(1)
        file_dates[filename] = date

max_date = max(file_dates.values())
required_files = [filename for filename, date in file_dates.items() if date == max_date]


# S3 setup
s3_client = boto3.client('s3')

# Copy files from SFTP to S3
for remote_file in required_files:
    remote_file_path = sftp_remote_path + remote_file
    s3_object_key = s3_prefix + remote_file

    try:
        with sftp_client.file(remote_file_path, 'r') as sftp_file:
            s3_client.upload_fileobj(sftp_file, s3_bucket_name, s3_object_key)
        print(f'Copied {remote_file} to S3')
    except NoCredentialsError:
        print('No AWS credentials found. Make sure your AWS credentials are configured.')
    except Exception as e:
        print(f'Error copying {remote_file} to S3: {str(e)}')

# Close connections
sftp_client.close()
sftp.close()
