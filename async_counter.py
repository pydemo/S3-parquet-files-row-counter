
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


bucket_name= 'test-data'
bucket_prefix= 'etl2/test0/f__app

async def save_to_file(s3_client, bucket: str, key: str):
        
        response = await s3_client.get_object(Bucket=bucket, Key=key)
        async with response['Body'] as stream:
            content = await stream.read() 
        
        if 1:
            fn =f'out/downloaded/{bucket_name}/{key}'

            dn= os.path.dirname(fn)
            if not isdir(dn):
                os.makedirs(dn,exist_ok=True)
            if 1:
                with open(fn, 'wb') as fh:
                    fh.write(content)
                    print(f'Downloaded to: {fn}')
       
        return [0]

async def go(bucket: str, prefix: str) -> List[dict]:
    """
    Returns list of dicts of object contents

    :param bucket: s3 bucket
    :param prefix: s3 bucket prefix
    :return: list of download statuses
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    session = aiobotocore.session.AioSession()
    config = aiobotocore.config.AioConfig(max_pool_connections=_NUM_WORKERS)
    contents = []
    async with session.create_client('s3', config=config) as client:
        worker_co = partial(save_to_file, client, bucket)
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





def S3_download_bucket_files():
    s = time.perf_counter()
    _loop = asyncio.get_event_loop()
    _result = _loop.run_until_complete(go(bucket_name, bucket_prefix))
    assert sum(_result)==0, _result
    print(_result)
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")





    
