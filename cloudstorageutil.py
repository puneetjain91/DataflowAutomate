import google
from google.cloud import storage


# pip3 install google-cloud-storage

def create_bucket(bucket_name):
    """Creates a new bucket."""
    # bucket_name = "your-new-bucket-name"

    storage_client = storage.Client()
    try:

        bucket = storage_client.create_bucket(bucket_name)
        print("Bucket {} created".format(bucket.name))
    except google.api_core.exceptions.Conflict as er:
        print('Please try with different storage name')

def check_bucket_available(bucket_name):
    # storage_client = storage.Client()
    # try:
    #     bucket = storage_client.get_bucket(bucket_name)
    # except google.api_core.exceptions.NotFound as er :
    #     create_bucket(bucket_name)

    """Lists all buckets."""

    notfound = True
    storage_client = storage.Client()
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        if bucket.name == bucket_name:
            notfound = False

    if notfound:
        create_bucket(bucket_name)

    else:
        print('Bucket already exist')
