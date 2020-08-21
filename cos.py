from utils import *

class COS():

    def __init__(self):
        """
            Initializes COS class by:
                1. initializing ibm_boto3 COS resource portal
                2. initializing ibm_boto3 COS client portal
                3. initializing ibm_boto3 transfer configuration settings
        """
        self._cos_re = ibm_boto3.resource(
            service_name="s3",
            ibm_api_key_id=COS_API_KEY_ID,
            ibm_service_instance_id=COS_RESOURCE_CRN,
            endpoint_url=COS_ENDPOINT,
            ibm_auth_endpoint=COS_AUTH_ENDPOINT,
            config=Config(signature_version="oauth"))

        self._cos_cli = ibm_boto3.client(
            service_name="s3",
            ibm_api_key_id=COS_API_KEY_ID,
            ibm_service_instance_id=COS_RESOURCE_CRN,
            endpoint_url=COS_ENDPOINT,
            ibm_auth_endpoint=COS_AUTH_ENDPOINT,
            config=Config(signature_version="oauth"))

        self._transfer_config = ibm_boto3.s3.transfer.TransferConfig(
                # set chunksize to 5 MB chunks
                # set max file threshold to 15 MB
                multipart_threshold=1024 * 1024 * 15,
                multipart_chunksize=1024 * 1024 * 5)

    def create_bucket(self, bucket_name, cos_bucket_location = "us-south-smart"):
        """
            Creates smart tier COS bucket

            Parameters:
                bucket_name:         <str>
                                     name of bucket to create

                cos_bucket_location: <str>
                                     regional location of COS bucket
        """
        print(f"Creating new bucket: {bucket_name}")
        try:
            self._cos_re.Bucket(bucket_name).create(CreateBucketConfiguration={"LocationConstraint":cos_bucket_location})
            print(f"Bucket: {bucket_name} created!")
        except ClientError as be:
            print(f"CLIENT ERROR: {be}")
        except Exception as e:
            print(f"Unable to create bucket: {e}")

    def get_buckets(self):
        """
            Retrieves list of avaliable buckets
        """
        print("Retrieving list of buckets")
        try:
            buckets = self._cos_re.buckets.all()
            buckets = [bucket.name for bucket in buckets]
        except ClientError as be:
            print(f"CLIENT ERROR: {be}")
        except Exception as e:
            print(f"Unable to retrieve list buckets: {e}")
        else:
            return buckets

    def get_bucket_contents(self, bucket_name, prefix="", max_keys=100):
        """
            Retrieves list of bucket contents with limit of 100 list entries
            by default unless limit is otherwise specified

            Parameters:
                bucket_name:  <str>
                              name of target bucket

                prefix:       <str>
                              file prefix in order to locate file if in folder structure
                              directory environment

                max_keys:     <int>
                              limit or number of files keys to retrieve

            Response:
                files:        <list>
                              list of file names in target bucket
        """
        print("Retrieving bucket contents from: {0}".format(bucket_name))
        try:
            # create client object
            files = []
            next_token = ""
            more_results = True

            while (more_results):
                if prefix:
                    response = self._cos_cli.list_objects_v2(
                        Prefix=prefix,
                        Bucket=bucket_name, 
                        MaxKeys=max_keys, 
                        ContinuationToken=next_token)
                else:
                    response = self._cos_cli.list_objects_v2(
                        Bucket=bucket_name, 
                        MaxKeys=max_keys, 
                        ContinuationToken=next_token)
                
                batch_files = [file["Key"] for file in response["Contents"]]
                files.extend(batch_files)

                if response["IsTruncated"]:
                    next_token = response["NextContinuationToken"]
                else:
                    next_token = ""
                    more_results = False

        except ClientError as be:
            print("CLIENT ERROR: {0}\n".format(be))
        except Exception as e:
            print("Unable to retrieve bucket contents: {0}".format(e))
        else:
            return files

    def get_item(self, bucket_name, item_name):
        """
            Retrieves target file in target bucket

            Parameters:
                bucket_name:  <str>
                              name of target bucket

                item_name:    <str>
                              name of target file in bucket

            Response:
                item:        <dict>
                             target item contents in dict format 
        """
        print(f"Retrieving item from bucket: {bucket_name}, key: {item_name}")
        try:
            file = self._cos_re.Object(bucket_name, item_name).get()
        except ClientError as be:
            print(f"CLIENT ERROR: {be}")
        except Exception as e:
            print(f"Unable to retrieve file contents: {e}")
        else:
            file = io.BytesIO(file["Body"].read())
            item = jsonify_cos(file, item_name)
            return item

    def upload_file_cos(self, bucket_name, item_name, file):
        """
            Uploads to target bucket

            Parameters:
                bucket_name:  <str>
                              name of target bucket

                item_name:    <str>
                              name of target file in bucket

                file:         <bytes>
                              binarized version of file to be uploaded
        """        
        try:
            print(f"Starting file transfer for {item_name} to bucket: {bucket_name}")
            self._cos_re.Object(bucket_name, item_name).upload_fileobj(
                Fileobj=file,
                Config=self._transfer_config)
            print(f"Transfer for {item_name} Complete!")
        except ClientError as be:
            print(f"CLIENT ERROR: {be}")
        except Exception as e:
            print(f"Unable to complete multi-part upload: {e}")

    def delete_file_cos(self, bucket_name, item_name):
        """
            Delete target file in target bucket

            Parameters:
                bucket_name:  <str>
                              name of target bucket

                item_name:    <str>
                              name of target file in bucket
        """    
        print(f"Deleting item: {item_name}")
        try:
            self._cos_re.Object(bucket_name, item_name).delete()
        except ClientError as be:
            print(f"CLIENT ERROR: {be}")
        except Exception as e:
            print(f"Unable to delete item: {e}")

    # TODO: may need to create a function to empty bucket contents first before deleting
    def delete_bucket(self, bucket_name):
        """
            Delete target bucket

            Parameters:
                bucket_name:  <str>
                              name of target bucket
        """    
        print(f"Creating new bucket: {bucket_name}")
        try:
            self._cos_re.Bucket(bucket_name).delete()
            print(f"Bucket: {bucket_name} delete.")
        except ClientError as e:
            print(f"CLIENT ERROR: {e}")
            raise
        except Exception as e:
            print(f"Unable to delete bucket: {e}")
            raise

if __name__ == '__main__':
    cos = COS()
