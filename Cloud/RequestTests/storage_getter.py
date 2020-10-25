from google.cloud import storage

storage_client = storage.Client()

print('nani')
bucket = storage_client.get_bucket('test_bucket_limonadev')
for blob in bucket.list_blobs():
    if blob.name[:7] == 'output/' and blob.name != 'output/':
        with open(f'Rank/{blob.name}', "wb") as f:
            blob.download_to_file(f)
            print(blob.name + ' ' + '#'*70)