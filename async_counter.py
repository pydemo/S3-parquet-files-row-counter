
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



async def get_line_count(s3_client, bucket: str, key: str, client):
    # Get json content from s3 object

    # get object from s3

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
        worker_co = partial(get_line_count, client, bucket)
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





def main():
    s = time.perf_counter()
    _loop = asyncio.get_event_loop()
    _result = _loop.run_until_complete(go('my-data', 'etl2/20210330/'))
    #print(1111,_result)
    assert not 'N/A' in _result, _result
    print('Total:', sum(_result))
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
