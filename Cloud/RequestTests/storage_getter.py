import os
import requests
from google.cloud import storage

def parse_and_upload_ranks(f):
    pairs = f.readlines()
    body = {}
    for pair in pairs:
        raw = pair.split()
        if len(raw) == 2:
            web, rank = raw[0], raw[1]
            web = web.replace('.', '-')
            print(web, rank)
            body[web] = rank
    if len(body) == 0:
        return
    body = str(body).replace("'",'"')
    os.system("curl -X PATCH -d '" + body + "' 'https://prismatic-vial-174715.firebaseio.com/ranks.json'")

storage_client = storage.Client()

print('nani')
bucket = storage_client.get_bucket('test_bucket_limonadev')
print('lol')
for blob in bucket.list_blobs():
    if blob.name[:7] == 'output/' and blob.name != 'output/':
        with open(f'Rank/{blob.name}', "w+") as f:
            print('downloading')
            content = blob.download_as_string().decode('utf-8')
            print('downloaded')
            f.write(content)
            f.seek(0)
            parse_and_upload_ranks(f)

