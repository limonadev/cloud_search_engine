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

def parse_and_upload_index(f):
    pairs = f.readlines()
    body = {}
    for pair in pairs:
        raw = pair.split()
        if len(raw) == 2:
            word, web = raw[0], raw[1]
            print(word, web)
            if word not in body:
                body[word] = ''
            body[word] += f' {web}'
    for word,web_list in body.items():
        print(word)
        os.system("curl -X POST -d '\"" + web_list + f"\"' 'https://prismatic-vial-174715.firebaseio.com/index/{word}.json'")

storage_client = storage.Client()

print('nani')
bucket = storage_client.get_bucket('test_bucket_limonadev', timeout=(1,10))
print('lol')
for blob in bucket.list_blobs():
    if blob.name[:7] == 'output/' and blob.name != 'output/':
        with open(f'Rank/{blob.name}', "w+") as f:
            print('downloading rank')
            content = blob.download_as_string().decode('utf-8')
            print('downloaded rank')
            f.write(content)
            f.seek(0)
            parse_and_upload_ranks(f)
    elif blob.name[:13] == 'output_index/' and blob.name != 'output_index/':
        with open(f'Index/{blob.name}', 'w+') as f:
            print('downloading index')
            content = blob.download_as_string().decode('utf-8')
            print('downloaded index')
            f.write(content)
            f.seek(0)
            parse_and_upload_index(f)