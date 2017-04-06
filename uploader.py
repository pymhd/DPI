import os
from datetime import datetime, timedelta
from pprint import pprint
import json
from log import get_logger
from storage import *
from dpi_api import *
from caching import Cache
import sqlite3
import requests
from requests.utils import urlparse
from  urlparse import ParseResult

os.chdir("/usr/local/comcom/dpi_spy_api")
logger = get_logger(__name__, "uploader")

DATA = [] 
URL = "http://integration_srv_url.2com.net:8080/save_dpi_log"   # Oracle endpoint 

statistic_day = datetime.strftime(datetime.now() - timedelta(days = 1), "%Y-%m-%d") # Yesterday
#week_ago = datetime.strftime(datetime.now() - timedelta(days = 7), "%Y-%m-%d")

conn = sqlite3.connect("STORAGE.db")		# local storage
logger.debug("Sqlite database conn establisged")
cache = Cache(100)				# Better use redis
logger.debug("Cache var initialized for {0} items".format(cache.cacheSize))				# Won't annoy procera with the same queries
storage = Storage(conn)
logger.debug("Abstarct storage initialized")
domains = storage.get_type("domain")            # list of so called domains-serverhostnames (ip addr, as procera stores it)
urls = storage.get_type("url")			# list of URLs 
logger.info(" {0} domains and {1} URLs in storage".format(len(domains), len(urls)))


def replace_id(id, *items):
    global logger
    RESULT = []
    logger.info("Replacing cached item id to {0}".format(id))
    for item in items:
        if item == "NO DATA":
            RESULT.append(item)
            continue
        item["id"] = id
        RESULT.append(item)
    return {"dpi_log": RESULT}

for domain in domains:
    url = domain["url"]
    id = domain["id"]
    parsed_url = urlparse(url)
    procera_vhost = parsed_url.netloc.split("www.")[-1]
    logger.info("Starting main procera func for {0}, id = {1} ".format(procera_vhost, id))
    logger.debug("Searching {0} in cached result".format(procera_vhost)) 
    try:
        cached_query = cache[procera_vhost]
        logger.debug("Found {0} in cache".format(procera_vhost))
        data = replace_id(id, *cached_query["dpi_log"])  
        DATA.append(data)
        logger.debug("Added {0} to upload list".format(procera_vhost))
    except KeyError:
        #print url, procera_vhost, id, statistic_day
        logger.debug("NOT found {0} in cache".format(procera_vhost))
        data = get_domain_statistics(statistic_day, statistic_day, [(procera_vhost, id)])
        cache[procera_vhost] = data
        logger.debug("Added {0} in cache".format(procera_vhost))
        DATA.append(data)
        logger.debug("Added {0} to upload list".format(procera_vhost))



#pprint(DATA)
for item in DATA:
    if item["dpi_log"] ==  ['NO DATA']: continue  # NO MATCH FOR URL, log it and skip
    #pprint(item)
    resp = requests.post(URL, json = item)	  # Upload to Oracle
    