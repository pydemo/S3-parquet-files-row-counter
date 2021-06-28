"""
Get row counts in bucket/dir


"""


import os, sys, csv, gzip
import io, time
import tempfile
import asyncio
import aioboto3

import boto3, shutil, string, random
from pprint import pprint as pp
from os.path import isdir, isfile, join

from os import linesep



try:
    import cStringIO
except ImportError:
    import io as cStringIO

        
e=sys.exit	

colsep=','



def get_key_from_fn(fn):
    
    return fn.split(".")[key_pos_in_fact_fn]
def get_part_from_fn(fn):		
    return fn.split(".")[part_pos_in_fact_fn]

async def get_files_to_scan(objs, queue):
    i=0
    for obj in objs:
        if obj.key:
            path, filename = os.path.split(obj.key)
            if filename: #filter out dir name              
                await queue.put(obj.key)
                i +=1
            
        else:
            raise Exception('obj.key is not set')
    assert i>0, 'Bucket is empty'
    await queue.put(None)
    

async def count_rows_in_file(counts_queue, s3_key):
    if 1: #Fact partitioner

        sql_stmt 	= """SELECT count(*) FROM s3object S"""  
        req_fact =s3.select_object_content(
            Bucket	= bucket_name,
            Key		= s3_key,
            ExpressionType	= 'SQL',
            Expression		= sql_stmt,
            InputSerialization={'Parquet': {}},
            OutputSerialization = {'CSV': {
                        'RecordDelimiter': os.linesep,
                        'FieldDelimiter': colsep}},
            
        )

    for event in req_fact['Payload']:
        if 'Records' in event:
            rr=event['Records']['Payload'].decode('utf-8')
            for i, rec in enumerate(rr.split(linesep)):
                if rec:
                    row=rec.split(colsep)
                    if row:
                        print('File line count:', row[0])
                        await counts_queue.put(row)

    #await counts_queue.put(None)
    

async def count_rows_in_table(queue, counts_queue):
    
    readers=[]
    qz=[]
    while True: 
        row = await queue.get()
        queue.task_done()
        if row:
            s3_key = row
            print('S3 key: ',s3_key)
            if 1:
                readers.append(asyncio.create_task(count_rows_in_file(counts_queue, s3_key)))

        else:
            break
    await asyncio.gather(*readers)
    await counts_queue.put(None)
    
async def merge_counts(counts_queue):
    total = 0
    while True: 
        row = await counts_queue.get()
        counts_queue.task_done()
        if row:
            cnt = int(row[0])
            total +=cnt

        else:
            break
    print('Total rows:', total)
    
async def s3_count_fact():
    queue = asyncio.Queue()
    counts_queue = asyncio.Queue()
    
    
    await asyncio.gather( get_files_to_scan(objs, queue), count_rows_in_table(queue, counts_queue), merge_counts(counts_queue))	
    await queue.join()
    

if __name__=='__main__':
    
    s3			= boto3.client('s3')
    bucket_name	= 'my-data-test'
    bucket_prefix='all_files/'
    s = time.perf_counter()
    s3r		= boto3.resource('s3')
    mybucket = s3r.Bucket(bucket_name)
    objs = mybucket.objects.filter(Prefix = bucket_prefix)
    if 1:
        asyncio.run(s3_count_fact())
        elapsed = time.perf_counter() - s
        print(f"{__file__} executed in {elapsed:0.2f} seconds.")


