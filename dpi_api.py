from pprint import pprint
import packetlogic2
from requests.utils import urlparse
from  urlparse import ParseResult
from Queue import Queue, Empty
from time import sleep
from log import get_logger
from threading import Lock


class _Config:
    ip = "1.2.3.4"
    user = "user"
    pwd = "password"

_logger_upl = get_logger(__name__, "uploader")
_logger_app = get_logger(__name__, "main")
LOCK = Lock()

class _DPI:
    def __init__(self):
        try: self.__pl = packetlogic2.connect(_Config.ip, _Config.user, _Config.pwd)
        except: return False
        
    @staticmethod
    def connect(ip, user, pwd):
        try: 
            pl = packetlogic2.connect(ip, user, pwd)
            ruleset = pl.Ruleset()
            #stats = pl.Statistics(), nobody cares
            return (pl, ruleset)
        except: 
            return (False, False)
    
    @staticmethod
    def commit(ruleset, Msg = "Stats Blank Commit"):
        ruleset.commit(message = Msg)
        return True    
    
    @staticmethod    
    def add_domains_to_propertyobj(ruleset, domainlist):    # [http://mediza.io, "https://www.2kom.ru"]
        obj = ruleset.object_find("/PropertyObjects/Dynamic_Domains")
        for domain in domainlist:
            obj.add("Server Hostname={0}".format(domain))
        return True
        
    @staticmethod            
    def add_urls_to_propertyobj(ruleset, urllist):	# [http://mediza.io/, "http://www.2kom.ru/inet/list/?foo=1&bar=2"]
        obj = ruleset.object_find("/PropertyObjects/Dynamic_URLS")
        for url in urllist:
                obj.add("URL={0}".format(url))
        return True

    @staticmethod            
    def add_exact_urls_to_propertyobj(ruleset, urllist):      # [http://mediza.io/, "http://www.2kom.ru/inet/list/?foo=1&bar=2"]  whatever, exact as user wants
        obj = ruleset.object_find("/PropertyObjects/Dynamic_Exact")
        for url in urllist:
                obj.add("URL={0}".format(url))
        return True

    
    @staticmethod 
    def del_domain_from_propertyobj(ruleset, domainlist):
        obj = ruleset.object_find("/PropertyObjects/Dynamic_Domains")
        for domain in domains:
            try: obj.remove("Server Hostname={0}".format(domain))
            except: return False
        return True

    @staticmethod
    def del_url_from_propertyobj(self, urllist):
        obj = r.object_find("/PropertyObjects/Dynamic_URLS")
        for url in urllist:
            try: obj.remove("URL={0}".format(url))
            except: return False
        return True
        
    def get_domain_kp_cons(self, date_start, date_end, domain): # get DOMAIN kodpacket
        s = self.__pl.Statistics()
        try: items =  s.list(date_start, date_end, "/Oracle_SPY_Domains?Statistics Object/{0}?Remote Vhost".format(domain[0]))
        except packetlogic2.exceptions.PLDBError: return ["NO DATA"]
        return [ {"kodpaket": x["name"].split("_")[2][2:], "id": domain[-1], "count": int(float(x["values"]["connections"])), "dt": date_start, "url": domain[0]} for x in items if x ]
        
    def get_url_kp_cons(self, date_start, date_end, url):	#get URL kodpacket
        s = self.__pl.Statistics()
        parsed_url = urlparse(url)
        procera_vhost = parsed_url.netloc.split("www.")[-1]
        if not url.endswith("/"): url += "/"
        procera_url = url.replace("/", "_")
        #print procera_url, procera_vhost
        #print "/Oracle_SPY_Urls?Statistics Object/{0}?Remote Vhost/URL?Property/{1}?Property".format(procera_vhost, procera_url)
        try: items =  s.list(date_start, date_end, "/Oracle_SPY_Urls?Statistics Object/{0}?Remote Vhost/URL?Property/{1}?Property/".format(procera_vhost, procera_url))
        except packetlogic2.exceptions.PLDBError: return ["NO DATA"]
        return [ {"kodpaket": x["name"].split("_")[2][2:], "count": int(float(x["values"]["connections"])), "date": date_start, "url": url} for x in items if x ] 
    
    def list_urls(self):
        r = self.__pl.Ruleset()
        obj = r.object_find("/PropertyObjects/Dynamic_URLS")
        urllist = obj.items
        return [ url_obj.value for url_obj in urllist ]
    
    def list_domains(self):
        r = self.__pl.Ruleset()
        obj = r.object_find("/PropertyObjects/Dynamic_Domains")
        domainlist = obj.items
        return [ dom_obj.value for dom_obj in domainlist ]
        

def url_parser(url):
    #if conext == "domain"
    parsed_url = urlparse(url)
    scheme = parsed_url.scheme
    #Domain part for Server Hostname Prop Obj
    if scheme == "https":
        procera_vhost = parsed_url.netloc.split("www.")[-1]
        additional_procera_vhost = "www." + procera_vhost
        # May return data, because https
        return {"domains": [procera_vhost, additional_procera_vhost], "urls": False}
    elif scheme == "http":
        #Domain part for Server Hostname Prop Obj
        procera_vhost = parsed_url.netloc.split("www.")[-1]
        additional_procera_vhost = "www." + procera_vhost
        ##Urls part for URL Prop Obj
        original_url = url
        procera_url_exact =  ParseResult(scheme = parsed_url.scheme,
                                   netloc = parsed_url.netloc,
                                   path = parsed_url.path,
                                   params = "",
                                   query = "",
                                   fragment = "").geturl()
        if not procera_url_exact.endswith("/"): procera_url_exact = procera_url_exact + "/"
        procera_url_asterisk = procera_url_exact + "*"
        # Now the same without www
        return {"domains": [procera_vhost, additional_procera_vhost],
                "urls": list(set([original_url, procera_url_exact, procera_url_asterisk,
                         original_url.replace("www.",""), procera_url_exact.replace("www.",""), 
                         procera_url_asterisk.replace("www.","")
                        ]))
               }  # Pizdets kostyli
    else:
        return {"domains": False, "urls": False}    

"""
def add_domains(domains):
    dpi = _DPI(_Config.ip, _Config.user, _Config.pwd)
    if not dpi:
        return False
    resp = dpi.add_domains_to_propertyobj(domains)
    return resp
"""

def add_urls(**kwargs):
    _logger_app.info("Starting 'Procera add items' job")
    if not filter(lambda x: x, kwargs.values()): 
        _logger_app.error("Empty values: not cool bro")
        _logger_app.error(kwargs)
        return False
    _logger_app.info("Trying to connect to DPI...")
    (dpi_con, ruleset) = _DPI.connect(_Config.ip, _Config.user, _Config.pwd)  
    if not dpi_con:
        _logger.critical("Connection Failed, Data Lost, Restore it manualy")
        return False
    _logger_app.info("Connection Established")
    if "domains" in kwargs:
        _logger_app.info("Adding domains: {0}".format(kwargs["domains"]))
        resp = _DPI.add_domains_to_propertyobj(ruleset, kwargs["domains"])
        _logger_app.info("Status: {0}".format(resp))
    if "urls" in kwargs:
        _logger_app.info("Adding urls: {0}".format(kwargs["urls"]))
        resp = _DPI.add_urls_to_propertyobj(ruleset, kwargs["urls"])
        _logger_app.info("Procera status: {0}".format(resp))
    _logger_app.info("Commit...")
    _DPI.commit(ruleset, "SpyCop commit")
    _logger_app.info("Success")
    return True


def add_exact_urls(e_urls):
    _logger_app.info("Starting 'Procera add EXACT URL' job")
    _logger_app.info("Trying to connect to DPI...")
    (dpi_con, ruleset) = _DPI.connect(_Config.ip, _Config.user, _Config.pwd)  
    if not dpi_con:
        _logger.critical("Connection Failed, Data Lost, Restore it manualy")
        return False
    _logger_app.info("Connection Established")
    _logger_app.info("Adding urls: {0}".format(e_urls))
    resp = _DPI.add_exact_urls_to_propertyobj(ruleset, e_urls)
    _logger_app.info("Procera status: {0}".format(resp))
    _logger_app.info("Commit...")
    _DPI.commit(ruleset, "SpyCop commit")
    _logger_app.info("Success")
    return True


    
    
def _get_all_from_queue(q):
    items = []
    while True:
        try: items.append(q.get_nowait())
        except Empty: break
    return items


#Main thread method gonna use it
def handler(q):  # q = Queue obj
    _logger_app.info("Main url type Thread started")
    while True:
        _logger_app.debug("Checking Queue")
        urls = _get_all_from_queue(q)
        _logger_app.debug("Got {0} urls".format(len(urls)))
        if len(urls) > 0:
            procera_urls = filter(lambda x: x, [ u["urls"] for u in map(url_parser, urls) ])  # filter False items
            procera_vhosts = filter(lambda x: x, [ u["domains"] for u in map(url_parser, urls) ]) # filter False items
            _logger_app.info("Got {0} urls to add".format(procera_urls))
            _logger_app.info("Got {0} vhosts to add".format(procera_vhosts))
            #list(set().union(*procera_urls)), list(set().union(*procera_vhosts))
            res = add_urls(urls = list(set().union(*procera_urls)) , domains = list(set().union(*procera_vhosts)))
            if res: 
                _logger_app.info("Successfully added urls and domains to  appropriate lists")
            else:
                _logger_app.error("Could not add items to PropObj")
        _logger_app.debug("Slepping for 5 minutes before next Queue check")
        sleep(300)


def exact_handler(eq):   #get exact queue as arg
    _logger_app.info("Exact Url Handler Thread started")
    while True:
        _logger_app.debug("Checking Exact Queue")
        urls = _get_all_from_queue(eq)
        _logger_app.debug("Got {0} urls".format(len(urls)))
        if len(urls) > 0:
            res = add_exact_urls(urls)
            if res: 
                _logger_app.info("Successfully added urls and domains to  appropriate lists")
            else:
                _logger_app.error("Could not add items to PropObj")
        _logger_app.debug("Slepping for 10 minutes before next Exact Queue check")
        sleep(600)
    



def get_domain_statistics(ds, de, domains):
    dpi = _DPI(_Config.ip, _Config.user, _Config.pwd)
    if not dpi: 
        return False
    #resp = dpi.get_kp_counts(ds, de, url)
    result = []
    inter_result = [ dpi.get_domain_kp_cons(ds, de, domain) for domain in domains ]
    for l in inter_result: result += l
    return {"dpi_log": result}


def get_url_statistics(ds, de, urls):
    dpi = _DPI(_Config.ip, _Config.user, _Config.pwd)
    if not dpi:
        return False
        #resp = dpi.get_kp_counts(ds, de, url)
    result = []
    inter_result = [ dpi.get_url_kp_cons(ds, de, url) for url in urls ]
    for l in inter_result: result += l
    return {"dpi_log": result}
    #  Examples
    #bar:/concurents?Statistics Object/netbynet.ru?Remote Vhost/URL?Property/http:__www.netbynet.ru_internet_tarif_?Property/?splittype=NetObject&datatype=Connections&sortby=Connections
    # s.list("2017-03-14", "2017-03-14",  "/Oracle_SPY_Urls?Statistics Object/ford.ru?Remote Vhost/URL?Property/http:__www.ford.ru_?Property")
    #

if __name__ == "__main__":
    pass
    #pprint(url_parser("https://www.2kom.ru"))
    #pprint(url_parser("http://www.google.com/index.html"))
    #pprint(url_parser("http://www.google.com/path/to/url"))
    #pprint(url_parser("http://www.google.com/path/to/url/"))
    #pprint(url_parser("http://www.google._get_all_from_queuecom/path/to/url/?tr=rr$hg&gf=66#&1"))
    #qi = Queue()
    #qi.put("https://www.motors.ru/")
    #qi.put("http://www.alsaad.ru/path/to/url")
    #handler(qi)
    print _DPI.blabla('Test static ')
    #dpi = _DPI("1.2.3.4", "user", "password")
    #print dpi.list_domains()
    #print dpi.list_urls()
    #if not dpi: print "Connection Error"
    #dpi.add_url("bash.im")
    #dpi.get_statistics("2017-03-02", "2017-03-02", "meduza.io")
    #dpi.get_url_kp_cons("2017-03-15", "2017-03-16", "http://2kom.ru")
    #add_domains(["https://2kom.ru", "https://en.myshows.me/"])
    #add_urls(["http://www.provelo.ru/models-velosipedy/", "http://www.provelo.ru/models-velosipedy"])
    #print(get_domain_statistics("2017-03-26", "2017-03-26", ["meduza.io"]))
    