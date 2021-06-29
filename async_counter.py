import os, sys, csv, time, logging
import datetime as dt
from  datetime import datetime
import decimal
from os.path import split, join, isdir, basename, isfile
from pprint import pprint as pp
from cli_layer.utils import timer, get_err
from pathlib import Path
from cli_layer.common import *
from cli_layer.fmt import  pfmt, pfmtv, pfmtd, psql
from cli_layer.pipeline.S3.dir.parquet.row.async_counter_ import usage
#import cli_layer.pipeline.S3.utils as ppl_utils
from cli_layer.pipeline.utils import get_params

e=sys.exit

log = logging.getLogger()


import cli_layer.config.app_config as app_config
apc = app_config.apc


import os, time
import asyncio
from itertools import chain
import json
from typing import List
from json.decoder import WHITESPACE
import logging
from functools import partial
from pprint import pprint as pp

# Third Party
import asyncpool
import aiobotocore.session
import aiobotocore.config

_NUM_WORKERS = 50


def iterload(string_or_fp, cls=json.JSONDecoder, **kwargs):
    # helper for parsing individual jsons from string of jsons (stolen from somewhere)
    string = str(string_or_fp)

    decoder = cls(**kwargs)
    idx = WHITESPACE.match(string, 0).end()
    while idx < len(string):
        obj, end = decoder.raw_decode(string, idx)
        yield obj
        idx = WHITESPACE.match(string, end).end()


async def get_object(s3_client, bucket: str, key: str, client):
    # Get json content from s3 object

    # get object from s3
    if 0:
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        async with response['Body'] as stream:
            content = await stream.read()

        return list(iterload(content.decode()))
    else:
        #print(bucket, key)
        sql_stmt 	= """SELECT count(*) FROM s3object S"""  
        #print(rid)
        colsep=','

        
        req_fact = await client.select_object_content(
            Bucket	= bucket,
            Key		= key,
            ExpressionType	= 'SQL',
            Expression		= sql_stmt,
            InputSerialization={'Parquet': {}},
            OutputSerialization = {'CSV': {
                        'RecordDelimiter': os.linesep,
                        'FieldDelimiter': colsep}},
        ) 
        #print(222, req_fact['Payload']['Records'])

        async for event in req_fact['Payload']:
            
            if 'Records' in event:
                rr=event['Records']['Payload'].decode('utf-8')
                for i, rec in enumerate(rr.split(os.linesep)):
                    if rec:
                        row=rec.split(colsep)
                        if row:
                            print('File line count:', row[0], key)
                            #await counts_queue.put(row)
                            return [int(row[0])]
                                
    return ['N/A']

async def go(bucket: str, prefix: str) -> List[dict]:
    """
    Returns list of dicts of object contents

    :param bucket: s3 bucket
    :param prefix: s3 bucket prefix
    :return: list of dicts of object contents
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    session = aiobotocore.session.AioSession()
    config = aiobotocore.config.AioConfig(max_pool_connections=_NUM_WORKERS)
    contents = []
    async with session.create_client('s3', config=config) as client:
        worker_co = partial(get_object, client, bucket)
        async with asyncpool.AsyncPool(None, _NUM_WORKERS, 's3_work_queue', logger, worker_co,
                                       return_futures=True, raise_on_join=True, log_every_n=10) as work_pool:
            # list s3 objects using paginator
            paginator = client.get_paginator('list_objects')
            async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for c in result.get('Contents', []):
                    contents.append(await work_pool.push(c['Key'], client))

    # retrieve results from futures
    contents = [c.result() for c in contents]
    return list(chain.from_iterable(contents))





def S3_dir_parquet_row_async_counter():
    s = time.perf_counter()
    _loop = asyncio.get_event_loop()
    _result = _loop.run_until_complete(go('vydia-data', 'etl2/stats_raw/apple/analytics_api/bydate/api1x0_vc2x0_ds1x0/20210330/'))
    #print(1111,_result)
    assert not 'N/A' in _result, _result
    print('Total:', sum(_result))
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")

@timer (basename(__file__))
def main(**kwargs):
    """	 
    Location	 : S3\dir\parquet\row\async_counter	
    Params : 
                "s3_loc" - param 0
        "file_filter" - param 1
    Num of params: 2
    Usage: python cli.py -nop 1 -r DEV -p S3\dir\parquet\row\async_counter -pa s3_loc file_filter
    """
    
    usage(**kwargs)
    cp, params=get_params(**kwargs)
    limit	= kwargs['lame_duck']
    s3_loc,file_filter = params
    #apc.setConnName(conn_name)
    loc=locals()
    out=dict()
    for par in 's3_loc,file_filter'.split(','): out[par]=loc[par] 
    pfmtv(out,'',['Parameter', 'Value'])	

    S3_dir_parquet_row_async_counter()



    
