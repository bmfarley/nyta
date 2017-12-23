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
SNS_NOTIFIER = config['SNS_NOTIFIER']

nyt_creds = {'dbname':'nyt_articles',
             'port':5432,
             'user':'dbadmin',
             'password':config['nyt_creds_password'],
             'host':'nyt-articles.ckwrv3en1yz3.us-east-2.rds.amazonaws.com'}

def initialize():
    ohio_session = boto3.Session(region_name='us-east-2')
    s3 = ohio_session.resource('s3')
    sns_client = boto3.client('sns', region_name='us-east-1')
    return s3, sns_client


def response_elem_to_s3_body(elem):
    return json.dumps(elem).encode('UTF-8')


def put_bytes_in_s3(bytes_,key_name,s3_resource,bucket=S3_BUCKET):
    s3_resource.Object(bucket,key_name).put(Body=bytes_)


def make_nyta_response(year,month):
    r = requests.get(API_URL.format(year=year,month=month),params=PAYLOAD)
    return r.json()


def key_name_for_nyta(year,month,response_elem):
    return '{}/{:02d}/{}.txt'.format(year,month,response_elem['_id'])


def nyta_response_to_s3(year,month,sns_client,s3_resource):
    response = make_nyta_response(year,month)
    doc_count = response['response']['meta']['hits']
    for ix,doc in enumerate(response['response']['docs']):
        key_name = key_name_for_nyta(year,month,doc)
        s3_body = response_elem_to_s3_body(doc)
        put_bytes_in_s3(s3_body,key_name,s3_resource)
    sns_client.publish(PhoneNumber=SNS_NOTIFIER,
                   Message='[nyt-archive-worker]: {}/{:02d} successfully published {} docs'.format(year,month,doc_count))

def s3_to_rds(year,month,sns_client,s3_resource):
    prefix = f'{year}/{str(month).zfill(2)}/'
    records = []
    for obj in s3_resource.objects.filter(Prefix=prefix):
        bucket = s3_resource.Bucket(name=S3_BUCKET)
        content_object = bucket.Object(S3_BUCKET, obj.key)
        file_content = content_object.get()['Body'].read().decode('UTF-8')
        json_content = json.loads(file_content)
        pp = json_content.get('print_page', None)
        lp = json_content.get('lead_paragraph', None)
        sn = json_content.get('snippet', None)
        hd = json_content['headline'].get('main', None)
        pd = json_content.get('pub_date', None)
        _id = json_content.get('_id', None)
        records.append((_id, pp, sn, lp, hd, pd))

    insert_meta = '''INSERT INTO article_meta (article_id,print_page,snippet,lead_paragraph,headline,publication_time)
VALUES %s'''
    with pg2.connect(**nyt_creds) as nyt_conn:
        cursor = nyt_conn.cursor()
        execute_values(cursor,insert_meta,records)

    sns_client.publish(PhoneNumber=SNS_NOTIFIER,
                       Message='[nyt-archive-worker]: {}/{} uploaded to postgres'.format(year,str(month).zfill(2)))



if __name__ == '__main__':
    s3, sns_client = initialize()
    print('1) Get articles from NYT\n2) Move articles to RDS')
    choice_menu = {'1':nyta_response_to_s3,
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
            action(y,m,sns_client,s3)
        except Exception as e:
            sns_client.publish(PhoneNumber=SNS_NOTIFIER,
                               Message='[nyt-archive-worker]: FATAL {}'.format(e))
            raise
