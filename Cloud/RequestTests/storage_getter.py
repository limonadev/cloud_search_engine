import os
import requests
import ast
from google.cloud import storage
import sys

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

def parse_and_upload_index(f, group_map):
    pairs = f.readlines()
    body = {}
    for pair in pairs:
        raw = pair.split()
        if len(raw) == 2:
            word, web = raw[0], raw[1]
            #print(word, web)

            letter = word[0]
            if letter not in body:
                body[letter] = {}

            if word not in body[letter]:
                body[letter][word] = ''

            body[letter][word] += f' {web}'
    
    for letter, words in body.items():
        if len(words) == 0:
            continue
        
        if letter not in group_map:
            group_map[letter] = []

        print(letter)
        json_body = str(words).replace("'", '"')
        r = requests.post(f'https://prismatic-vial-174715.firebaseio.com/index_test/words/{letter}.json', data=json_body, timeout=10)
        print(r.status_code)
        print(r.text)

        if r.status_code == 200:
            key = ast.literal_eval(r.text)['name']
            print(key)
            group_map[letter].append(key)


def upload_group_map(group_map):
    for letter, groups in group_map.items():
        print(letter)
        r = requests.post(f'https://prismatic-vial-174715.firebaseio.com/index_test/groups/{letter}.json', json=groups, timeout=10)
        print(r.status_code)
        print(r.text)

storage_client = storage.Client()

print('nani')
bucket = storage_client.get_bucket('test_bucket_limonadev', timeout=(1,10))
print('lol')

group_map = {}
for blob in bucket.list_blobs():
    if blob.name[:7] == 'output/' and blob.name != 'output/':
        continue
        with open(f'Rank/{blob.name}', "w+") as f:
            print('downloading rank')
            content = blob.download_as_string(timeout=(1,10)).decode('utf-8')
            print('downloaded rank')
            f.write(content)
            f.seek(0)
            parse_and_upload_ranks(f)
    elif blob.name[:13] == 'output_index/' and blob.name != 'output_index/':
        with open(f'Index/{blob.name}', 'w+') as f:
            print('downloading index')
            content = blob.download_as_string(timeout=(1,10)).decode('utf-8')
            print('downloaded index')
            f.write(content)
            f.seek(0)
            parse_and_upload_index(f, group_map)

print('Uploading groups')
upload_group_map(group_map)