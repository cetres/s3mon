#!/usr/bin/env python
"""
check_s3 - Identify older files
"""
__author__ = "Gustavo Oliveira (cetres@gmail.com)"
__version__ = "0.1.0"
__copyright__ = "Copyright (c) 2019 Gustavo Oliveira"
__license__ = "MIT"

import os
import sys
import traceback
import logging
from datetime import datetime, timedelta, timezone
import argparse
import logging
from urllib.parse import urlparse

import boto3
import pandas as pd

UNIDADE = {
    'm': 'minutos',
    'h': 'horas',
    'd': 'dias'
}


def get_file_modification(file_path, region=None, max_iter=0):
    s3client = boto3.client('s3', region_name=region)
    if file_path.startswith('s3'):
        o = urlparse(file_path)
        bucket = o.netloc
        key = o.path.lstrip('/')
    else:
        o = file_path.split("/")
        bucket = o[0]
        key = "/".join(o.path[1:])
    obj = s3client.get_object(Bucket=bucket, Key=key)
    return obj['LastModified']

if __name__ == '__main__':
    description = "Identify older files in s3 bucket"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('file', metavar='FILE', type=str, nargs='+',
                    help='file in s3 to compare')
    parser.add_argument("-c", "--critical_interval", action="store", type=int, default=60*24,
                  help="Critical time interval to alert")
    parser.add_argument("-w", "--warn_interval", action="store", type=int, default=60,
                  help="Warning time interval to alert")
    parser.add_argument("-u", "--unidade", default='m', action="store",
                  help="Unidade de tempo Default: m - minutes")
    parser.add_argument("-r", "--region",
                  help="Region of bucket (Optional)")
    parser.add_argument("-m", "--max_iter", default='100',
                  help="Max iteration times for s3 list")
    parser.add_argument("-d", "--debug", action='store_true',
                  help="Debug output")
    parser.add_argument("-l", "--log_file", 
                  help="Nome do arquivo para armazenar o log")

    args = parser.parse_args()

    LOG_LEVEL = logging.DEBUG if args.debug else logging.WARN
    
    logging.basicConfig(filename=args.log_file, level=LOG_LEVEL, format='%(asctime)-15s %(levelname)s %(message)s')

    try:
        for f in args.file:
            logging.debug('Reading file %s' % f)
            obj_dt = get_file_modification(f, region=args.region, max_iter=args.max_iter)
            if args.unidade == 'm':
                intervalo = int((datetime.now(timezone.utc) - obj_dt).seconds / 60)
            elif args.unidade == 'h':
                intervalo = int((datetime.now(timezone.utc) - obj_dt).seconds / (60 * 60))
            elif args.unidade == 'd':
                intervalo = int((datetime.now(timezone.utc) - obj_dt).seconds / (60 * 60 * 24))
            else:
                parser.error("Unidade dão reconhecida")
            if intervalo > args.critical_interval:
                print("CRITICAL: {} {}".format(intervalo, UNIDADE[args.unidade]))
                sys.exit(2)
            elif intervalo > args.warn_interval:
                print("WARN: {} {}".format(intervalo, UNIDADE[args.unidade]))
                sys.exit(1)
            else:
                print("Ok: {} {}".format(intervalo, UNIDADE[args.unidade]))
    except:
        print('Unknown error found')
        raise
        sys.exit(3)

