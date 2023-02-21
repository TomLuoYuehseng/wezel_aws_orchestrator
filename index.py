from collections import defaultdict
from threading import Thread, Event
from time import sleep
import boto3
from boto3.dynamodb.conditions import Attr
import uuid
import json

stateMachineArn = 'arn:aws:states:us-east-2:736888855894:stateMachine:WebsiteScraperResourcesIDWebpageScraperStateMachine3645894C-QCfBILJznWHG'
#input_sample = '{"sites":{"url": "https://restaurants.fiveguys.com/1-garden-state-plaza","target_id": "0","competitorName": "fiveguys"}}'
session = boto3.Session(profile_name='CDKBuilder')
sfn = session.client('stepfunctions',region_name='us-east-2')

TABLE_NAME = 'WezelAwsStack-SiteMapParserResourcesIdwezelAwsJobTable32AFCBC7-JAVSLLGH43LH'
dynamodb = session.resource('dynamodb', region_name="us-east-2")
table = dynamodb.Table(TABLE_NAME)
dynamodb_client = session.client('dynamodb',region_name='us-east-2')

COMPETITOR_KEY = 'competitor_name'
STATUS_KEY = 'job_status'

def schedule_website_jobs(website_name: str):
    running = table.scan(FilterExpression=Attr(COMPETITOR_KEY).eq(website_name) & Attr(STATUS_KEY).eq('running'), Select = 'COUNT')['Count']

    print(running)
    # e.g. 10 (per minute)

    #max_running = ssm.get(f"max_running_{website_name}")
    max_running = 5
    free_slots = max_running - running
    print(free_slots)

    if free_slots > 0:
        to_run = table.scan(FilterExpression=Attr(COMPETITOR_KEY).eq(website_name) & Attr(STATUS_KEY).eq('pending'))
        print(len(to_run['Items']))
        for i in range(free_slots):
            #run_job(to_run['Items'][i])
            print('running',i)


def run_job(job: dict):
    url = job['sitemap']
    competitor = job[COMPETITOR_KEY]
    target_id = str(uuid.uuid4())
    input_dict = {"sites":{"url": url,"target_id": target_id,"competitorName": competitor }}
    print(json.dumps(input_dict))
    
    response = sfn.start_execution(
    stateMachineArn = stateMachineArn,
    input=json.dumps(input_dict)
    )   
    print(response)
    

def get_websites():
    pass


def create_new_ticker(website_name: str):
    ticker = Ticker(website_name)
    ticker.run()
    return ticker


class Ticker(Thread):

    def __init__(self, website_name: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ready = Event()
        self._canceled: bool = False
        self.website_name = website_name

    def run(self):
        # need a way of shutting this down possibly?
        while True:
            if self._canceled:
                return
            schedule_website_jobs(self.website_name)
            self.wait_for_duration()
            self._ready = True
            print('running')

    def wait_for_duration(self):
        # Grab ssm parameter periodically -> do with aws lambda powertools?
        # wait_time = ssm.get(f"wait_time_{self.website_name}")
        wait_time = (10)
        sleep(wait_time)
        print('sleeping')

    def cancel(self):
        self._canceled = True

    '''
    def ready(self) -> bool:
        if not self._ready:
            return False
        self._ready = False
        return True
    '''

create_new_ticker('fiveguys')