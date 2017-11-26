##### AWS Indexer: creates elastic search index on AWS
##### 1. Connects to AWS elasticsearch service (credential stored in awsconfig.py)
##### 2. Creates index with mapping
##### 3. Creates generator from COCA input file with simple transformation
##### 4. Index documents with streaming.
##### Written in Python 3.6.3

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth
import csv
import glob
import json
import awsconfig
import time

def bulkStream(client,actions, stats_only=False, **kwargs):
    success, failed = 0, 0

    # list of errors to be collected is not stats_only
    errors = []

    for ok, item in helpers.streaming_bulk(client, actions, **kwargs):
        # go through request-reponse pairs and detect failures
        if not ok:
            if not stats_only:
                errors.append(item)
            failed += 1
        else:
            success += 1

    return success, failed if stats_only else errors

config = awsconfig.get_config()
mappings = {'ngram': {
    'properties': {
        'name': {'index': 'not_analyzed', 'type': 'string'},
        'count': {'type': 'integer'},
        'gram': {'type': 'integer'}
    }}}

awsauth = AWS4Auth(config["AWS_ACCESS_KEY"], config['AWS_SECRET_KEY'], config['region'], config['service'])
host = config['host']

es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

es.indices.create(index=config['index'], ignore=400, body={"mappings":mappings})

for fname in glob.glob('../../COCA/*.txt'):
    n = fname.split("/w")[1][0]
    with open(fname, encoding='ISO-8859-1') as f:
        csvreader = csv.reader(f, delimiter='\t')
        print("Processing",fname)
        start = time.time()
        actions = ({"_index" : config['index'], "_type" : "ngram",'name':' '.join(l[1:]),'count':int(l[0]),'gram':int(n)} for l in csvreader)
        print(bulkStream(es,actions))
        print("Elapsed Time",time.time()-start)           