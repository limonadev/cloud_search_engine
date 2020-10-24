from google.cloud import storage

storage_client = storage.Client()

print('nani')
bucket = storage_client.get_bucket('test_bucket_limonadev')
print(bucket)