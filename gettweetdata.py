from requests_oauthlib import OAuth1Session
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
import json, datetime, time, pytz, re, sys,traceback, pymongo, settings

from pymongo import MongoClient
from collections import defaultdict
import numpy as np


twitter = None
connect = None
db      = None
tweetdata = None
meta    = None

def initialize():
    global twitter, twitter, connect, db, tweetdata, meta
    twitter = OAuth1Session(settings.CONSUMER_KEY, settings.CONSUMER_SECRET, settings.ACCESS_TOKEN, settings.ACCESS_TOKEN_SECRET)
    connect = MongoClient('localhost', 27017)
    db = connect.starbucks
    tweetdata = db.tweetdata
    meta = db.metadata

initialize()