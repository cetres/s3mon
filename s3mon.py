#!/usr/bin/env python

import logging

import boto3
import pandas as pd

def load_objects(bucket, prefix=None, region=None, max_iter=0):
    client = boto3.client('s3', region_name=region)
    objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    ct = 0
    while True:
        if df is None:
            df = pd.DataFrame(objects['Contents'])
        else:
            df = pd.concat([df, pd.DataFrame(objects['Contents'])])
        if len(objects['NextContinuationToken']) > 0:
            objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=objects['NextContinuationToken'])
        else:
            break
        ct += 1
        logging.debug('Iter: {}'.format(ct))
        if max_iter > 0 and ct >= max_iter:
            break
    df['LastModified'] = pd.to_datetime(df['LastModified'])
    logging.info('IsTruncated: {}\nQty: {}'.format(objects['IsTruncated'], len(objects['Contents'])))
    return df