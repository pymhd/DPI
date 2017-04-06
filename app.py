# -*- coding: utf-8 -*-

from flask import Flask, render_template, redirect, request, flash
from Queue import Queue, Empty
import threading
from pprint import pprint
import json
import sqlite3
from dpi_api import *
from storage import *

application = Flask(__name__)      

urls_queue = Queue()
exact_urls_queue = Queue()
add_thread = threading.Thread(target = handler, args = [urls_queue])  # handler defined in dpi_api module
add_exact_thread = threading.Thread(target = exact_handler, args = [exact_urls_queue])  # handler defined in dpi_api module
add_exact_thread.daemon = True
add_thread.daemon = True
add_thread.start()
add_exact_thread.start()


def generate_error(Message):
    return json.dumps({"Status": "Failed", "Data": Message})
    
#########    FLASK Part of Code   ##################
##Default answer for all pathes
#@application.route('/', defaults={'path': ''})
#@application.route('/<path:path>')
#def catch_all(path):
#    return render_template('home.html')
###############################

# {"action": "add_domain",  "url": "http://example.com"}
# {"action": "add_url",  "url": "http://example.com/path/to/url/"}
# {"action": "get_domain_stats", "date_start": "2017-02-24", "date_end": "2017-03-02", "domains":["dom1", "dom2", "dom3"]} query changed, they cant create such item :(
# {"action": "get_url_stats", "date_start": "2017-02-24", "date_end": "2017-03-02", "urls":["url1", "url2", "url3"]}  - query changed, they cant create such item :(
# whatever...

@application.route("/dpi_api", methods = ['GET', 'POST'])
def dpi_api():
    # Get JSON Data and return errors if JSON  is  wrong
    connection = sqlite3.connect("STORAGE.db")
    storage = Storage(connection)
    try: 
        data = request.get_json()
    except: 
        return generate_error("Not JSON structured data")
    if not data: 
        return generate_error("Empy POST Data")
    if not "action" in data: 
        return generate_error("No action specified")
    # Get method from data, get/put/del dpi stats 
    if data["action"].lower() in  ["add_domain", "add_domains", "domain_add", "domains_add"]:
        try: domain = data["url"]
        except: return generate_error("URL not specified")
        if type(domain.encode("utf8")) is not str: 
            return generate_error("Domains Must be a string, not {0}".format(type(domain)))
        if len(domain) < 5: 
            return generate_error("Suspicious. Domain is too short") # use urlparse from dpi_api
        if "id" not in data: 
            return generate_error("No ID was specified")
        id = data["id"]
        storage.add_record("domain", domain.encode("utf8"), id)
        urls_queue.put(domain)   # to handler
        #res = add_domains([domain])
        return json.dumps({'Status': "Ok", "Data": "Items Added to Queue"})
    elif data["action"] in ["add_urls", "add_url", "url_add", "urls_add"]:
        try: url = data["url"]
        except: return generate_error("URL not specified")
        if type(url.encode("utf8")) is not str: 
            return generate_error("URLs must be a string")
        if len(url) < 8: 
            return generate_error("Suspicious. URL is too short")
        if "id" not in data:
            return generate_error("No ID was specified")
        id = data["id"]
        storage.add_record("url", url.encode("utf8"), id)
        exact_urls_queue.put(url) # to exact_handler
        #res = add_urls([url])
        return json.dumps({'Status': "Ok", "Data": "Items Added to Queue"})
    elif data["action"] == "get_url_statistics":
        return generate_error("Not implemented, conception changed")
    elif data["action"] == "get_domain_statistics":
        return generate_error("Not implemented, conception changed")
    else:
        return generate_error(r"Unknown action, ¯\_(ツ)_/¯")

if __name__ == '__main__':
  application.secret_key = 'iedgftrcfbfgr c7rt'
  application.run(host = '127.0.0.1', debug=True)

