import boto3
import requests
import psycopg2 as pg2
from psycopg2.extras import execute_values
import json
import itertools

config = json.load(open('worker_config.cfg','r'))

API_KEY = config['API_KEY']
API_URL = 'https://api.nytimes.com/svc/archive/v1/{year:}/{month:}.json'
PAYLOAD = {'api-key':API_KEY}
S3_BUCKET = 'bf-nyt-archive'
SLACK_HOOK = config['SLACK_HOOK']

nyt_creds = {'dbname':'nyt_articles',
             'port':5432,
             'user':'dbadmin',
             'password':config['nyt_creds_password'],
             'host':'nyt-articles.ckwrv3en1yz3.us-east-2.rds.amazonaws.com'}

class RequestNotifier:
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def notify(self,payload):
        requests.post(self.endpoint,data=json.dumps(payload))

def initialize():
    ohio_session = boto3.Session(region_name='us-east-2')
    s3 = ohio_session.resource('s3')
    slack_notifier = RequestNotifier(SLACK_HOOK)
    return s3, slack_notifier


def response_elem_to_s3_body(elem):
    return json.dumps(elem).encode('UTF-8')


def put_bytes_in_s3(bytes_,key_name,s3_resource,bucket=S3_BUCKET):
    s3_resource.Object(bucket,key_name).put(Body=bytes_)


def make_nyta_response(year,month):
    r = requests.get(API_URL.format(year=year,month=month),params=PAYLOAD)
    return r.json()


def key_name_for_nyta(year,month):
    return '{}/{:02d}/response.txt'.format(year,month)


def nyta_response_to_s3(year,month,s3_resource):
    response = make_nyta_response(year,month)
    doc_count = response['response']['meta']['hits']
    for ix,doc in enumerate(response['response']['docs']):
        key_name = key_name_for_nyta(year,month,doc)
        s3_body = response_elem_to_s3_body(doc)
        put_bytes_in_s3(s3_body,key_name,s3_resource)

def insert_records_in_rds(records):
    insert_meta = '''INSERT INTO article_meta (article_id,print_page,snippet,lead_paragraph,headline,publication_time)
VALUES %s
ON CONFLICT DO NOTHING'''
    with pg2.connect(**nyt_creds) as nyt_conn:
        cursor = nyt_conn.cursor()
        execute_values(cursor,insert_meta,records)

def nyta_json_to_record(json_content):
    pp = json_content.get('print_page', None)
    lp = json_content.get('lead_paragraph', None)
    sn = json_content.get('snippet', None)
    try:
        hd = json_content['headline'].get('main', None)
    except AttributeError:
        hd = None
    pd = json_content.get('pub_date', None)
    _id = json_content.get('_id', None)
    return (_id, pp, sn, lp, hd, pd)

def response_to_s3_and_rds(year,month,s3_resource):
    response = make_nyta_response(year,month)
    failed_ids = {}
    records = []
    key_name = key_name_for_nyta(year,month)
    s3_body = response_elem_to_s3_body(response['response']['docs'])
    put_bytes_in_s3(s3_body,key_name,s3_resource)
    for doc in response['response']['docs']:
        try:
            record = nyta_json_to_record(doc)
            records.append(record)
        except Exception as e:
            article_identifier = f'{year}-{str(month).zfill(2)}-{doc["_id"]}'
            failed_ids[article_identifier] = e
    assert records, f'Parsing of response failed completely for {year}/{str(month).zfill(2)}'
    insert_records_in_rds(records)
    return failed_ids or f'[nyta-worker]: {year}/{str(month).zfill(2)} receieved, loaded into S3 and inserted into postgres.'


def s3_to_rds(year,month,s3_resource):
    prefix = f'{year}/{str(month).zfill(2)}/'
    bucket = s3_resource.Bucket(name=S3_BUCKET)
    failed_ids = {}
    records = []
    for obj in bucket.objects.filter(Prefix=prefix):
        content_object = s3_resource.Object(S3_BUCKET, obj.key)
        file_content = content_object.get()['Body'].read().decode('UTF-8')
        json_content = json.loads(file_content)
        try:
            record = nyta_json_to_record(json_content)
            records.append(record)
        except Exception as e:
            article_identifier = f'{year}-{str(month).zfill(2)}-{json_content["_id"]}'
            failed_ids[article_identifier] = e
    assert records, f'[nyta-worker]: Nothing in S3 for {year}/{str(month).zfill(2)}'
    insert_records_in_rds(records)
    return failed_ids or f'[nyta-worker]: {year}/{str(month).zfill(2)} inserted into postgres.'


if __name__ == '__main__':
    s3, slack_notifier = initialize()
    print('1) Get articles from NYT\n2) Move articles to RDS')
    choice_menu = {'1':response_to_s3_and_rds,
                   '2':s3_to_rds}
    choice = input('Do which?  ').rstrip()
    action = choice_menu[choice]
    year_start = int(input('Start from which year?  ').rstrip())
    month_start = int(input('Start from which month?  ').rstrip())
    reach_back = int(input('Reach back how many months?  ').rstrip())

    months = reversed(range(1,13))
    years = reversed(range(1851,year_start+1))
    queue = [(y,m) for y,m in itertools.product(years,months)]

    for ix, (y,m) in enumerate(queue):
        if y == year_start and m == month_start:
            to_process = queue[ix:]
            break

    for y,m in to_process:
        try:
            status = action(y,m,s3)
            if type(status) == str:
                slack_notifier.notify({'text':status})
            else:
                if len(status) <= 50:
                    for _id,exception in status.items():
                        slack_notifier.notify({'text':f'[nyta-worker] Article {_id}: {str(exception)}'})
                else:
                    ex = list(status.keys())[0]
                    raise RuntimeError(f'Aborting. {len(status)} articles failed to parse; example:\n{ex} {status[ex]}')
        except AssertionError as e:
            slack_notifier.notify({'text':str(e)})
        except Exception as e:
            slack_notifier.notify({'text':f'[nyta-worker] SOMETHING FUN {str(e)}'})
            raise
