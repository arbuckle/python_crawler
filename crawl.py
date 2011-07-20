import urllib2, cookielib
import urlparse
from BeautifulSoup import BeautifulSoup
import sqlite3
import time, datetime
import threading, Queue
time.clock() # initializing clock

globalData = {
    'useragent': 'Crawler 0.0',
    'whitelist': ['ifriends.net', 'www.ifriends.net'], #domains to crawl, subdomain.domain.tld.  no wildcards.  will scan the entire web if left blank
    'blacklist': ['showcam', 'ireqfeed', 'showclub'], #if target URL contains string match from this list, the URL will not be crawled.
    'startURL': 'http://www.ifriends.net/',
    'threadLimit': 5,
    'queue': [] # not a queue object, a collection of response data objects to sequentially process
}


class Crawl:
    def __init__(self):
        print 'Crawl | init called'
        self.cookie = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(self.cookie))
        urllib2.install_opener(opener)
        self.db = DBOps()
    def getURL(self):
        print 'Crawl | getURL called'
        #check for start URL
        if globalData['startURL']:
            url = [(0, globalData['startURL'])]
            globalData['startURL'] = None
        else:
            url = self.db.getURLFromQueue(globalData['threadLimit'])
        return url

    def requestURL(self, data):
        print 'Crawl | requestURL called'
        url = data['full_url']
        request = urllib2.Request(url)
        self.cookie.add_cookie_header(request)
        visitedtime = datetime.datetime.utcnow()
        try:
            start = time.clock()
            response = urllib2.urlopen(request)
            response = response.read()
            end = time.clock()
            loadtime = end - start
        except (urllib2.HTTPError, urllib2.URLError), e:
            response = None
            data.update({'error': e})
            loadtime = 0
        data.update({'source': response, 'loadtime': loadtime, 'request_url': url, 'visitedtime': visitedtime})
        return data

class DBOps:
    def __init__(self):
        print 'DBOps | init called'
        self.connection = sqlite3.connect('crawl.db')
        self.c = self.connection.cursor ()
    def create(self):
        print 'DBOps | create called'
        self.c.execute('CREATE TABLE IF NOT EXISTS queue (url_id INTEGER PRIMARY KEY, url VARCHAR(1024))')
        self.c.execute('CREATE TABLE IF NOT EXISTS url_canonical (url_id INTEGER PRIMARY KEY, url VARCHAR(1024), times_visited INTEGER, times_referenced INTEGER)')
        self.c.execute('CREATE TABLE IF NOT EXISTS page_rel (link_src INTEGER, link_dest INTEGER, visit_id INTEGER)')
        self.c.execute('CREATE TABLE IF NOT EXISTS visit_metadata (visit_id INTEGER PRIMARY KEY, visited DATETIME, full_url VARCHAR(1024), scheme VARCHAR(64), netloc VARCHAR(256), path VARCHAR(256), params VARCHAR(256), query VARCHAR(256), fragment VARCHAR(256), \
            size INTEGER, loadtime FLOAT, num_links INTEGER, links_internal INTEGER, links_external INTEGER, error VARCHAR(512))')
    def getURLFromQueue(self, limit):
        print 'DBOps | getURLFromQueue called'
        self.c.execute('SELECT * FROM queue LIMIT ?', [str(limit)])
        return self.c.fetchall()
    def updateCanonical(self, data):
        print 'DBOps | updateCanonical called'
        # checks each link for references in url_canonical, updates or adds records accordingly
        url_canonical = self.c.execute('SELECT * FROM url_canonical') # possibly inefficient...
        url_canonical = url_canonical.fetchall()
        for link in data['all_links']:
            match = False
            for record in url_canonical:
                if link == record[1]:
                    # tick times_referenced
                    times_referenced = record[3] + 1
                    self.c.execute('UPDATE url_canonical SET times_referenced = ? WHERE url = ?', [times_referenced, link])
                    self.connection.commit()
                    match = True
                    break
            if not match:
                self.c.execute('INSERT INTO url_canonical VALUES (?, ?, ?, ?)', [None, link, 0, 0])
                self.connection.commit()
        return data
    def addToQueue(self, data):
        print 'DBOps | addToQueue called'
        # compares eligible links against url_canonical and adds them to the queue if they're unique
        for link in data['queue_links']:
            self.c.execute('SELECT * FROM url_canonical WHERE url = ?', [link])
            if not self.c.fetchall():
                self.c.execute('INSERT INTO queue VALUES (?, ?)', [None, link])
            self.connection.commit()
        return data
    def addVisitData(self, data):
        print 'DBOps | addVisitData called'
        # updates visit metadata, condition indicates whether error occurred.  Also ticks times_visited in url_Canonical
        if data['source']:
            self.c.execute('INSERT INTO visit_metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [None, data['visitedtime'], data['request_url'], data['scheme'], data['netloc'],
                 data['path'], data['params'], data['query'], data['fragment'], data['page_size'],
                 data['loadtime'], data['count_all_links'], data['count_external_links'],
                 data['count_internal_links'], None])
        else:
            self.c.execute('INSERT INTO visit_metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                [None, data['visitedtime'], data['request_url'], None, None, None, None, None, None, None, None, None, None, None, str(data['error'])])
        self.connection.commit()

        times_visited = self.c.execute('SELECT times_visited FROM url_canonical WHERE url = ?', [data['request_url']])
        times_visited = times_visited.fetchall()
        if times_visited:
            times_visited = times_visited[0][0]
            times_visited += 1
            self.c.execute('UPDATE url_canonical SET times_visited = ? WHERE url = ?', [times_visited, data['request_url']])
        else:
            self.c.execute('INSERT INTO url_canonical VALUES (?, ?, ?, ?)', [None, data['request_url'], 1, 0])
        self.connection.commit()
        return data
    def updatePageRel(self, data):
        print 'DBOps | updatePageRel called'
        # adds a relationships between the visit url_id and all the url_ids for the urls on the page
        link_src = self.c.execute('SELECT url_id FROM url_canonical WHERE url = ?', [data['request_url']])
        link_src = link_src.fetchall()[0][0]

        visit_id = self.c.execute('SELECT visit_id FROM visit_metadata WHERE full_url = ? ORDER BY visited DESC', [data['request_url']])
        visit_id = visit_id.fetchall()[0][0]
        #link_src INTEGER, link_dest INTEGER, visit_id INTEGER

        for link in data['all_links']:
            # n+1 queries!  TODO: should I just grab the whole table and iterate over it in memory?
            link_dest = self.c.execute('SELECT url_id FROM url_canonical WHERE url = ?', [link])
            link_dest = link_dest.fetchall()[0][0]
            self.c.execute('INSERT INTO page_rel VALUES (?, ?, ?)', [link_src, visit_id, link_dest])
            self.connection.commit()
        return data
    def removeURLFromQueue(self, data):
        print 'DBOps | removeURLFromQueue called'
        # removes the URL from the queue, if the ID is not 0 (0 indicating start url)
        url_id = data['url_id']
        if url_id:
            self.c.execute('DELETE FROM queue WHERE url_id = ?', [url_id])
            self.connection.commit()
        return data

class ParseResponse:
    def __init__(self):
        pass
    def getLinks(self, data):
        print 'ParseResponse | getLinks called'
        # parses response source and pulls all valid hrefs out of the page source
        soup = BeautifulSoup(data['source'])
        soup = soup.findAll('a')
        links = []
        for link in soup:
            try:
                if 'http' in link['href']: #this is not the best... TODO:  make this less brittle
                    links.append(link['href'])
            except KeyError:
                continue
        data.update({'all_links': links})
        return data
    def parseEligibleLinks(self, data):
        print 'ParseResponse | parseEligibleLinks called'
        # returns a list of links meeting the eligibility requirements set by the global white/black lists
        output = []
        #iterate through links and check against blacklist
        for link in data['all_links']:
            netloc = urlparse.urlparse(link).netloc
            skip = False # indicates that link is eligible to crawl
            for rule in globalData['whitelist']:
                if netloc <> rule:
                    skip = True
                else:
                    skip = False
                    break
            for rule in globalData['blacklist']:
                if rule in link:
                    skip = True
                    break
            if not skip:
                output.append(link)
        data.update({'queue_links': output})
        return data
    def parseLinkTarget(self, data):
        print 'ParseResponse | parseLinkTarget called'
        # returns a list of links targeting the same netloc/domain
        internal = []
        external = []
        url = urlparse.urlparse(data['request_url'])
        for link in data['all_links']:
            netloc = urlparse.urlparse(link).netloc
            if url.netloc == netloc: # strict! subdomains count as external!
                internal.append(link)
            else:
                external.append(link)
        data.update({'internal_links': internal, 'external_links': external})
        return data
    def main(self, data):
        print 'ParseResponse | main called'
        url = data['request_url']
        # break the URL down into all the constituent components
        parsedURL = urlparse.urlparse(url)
        data.update({'scheme': parsedURL.scheme, 'netloc': parsedURL.netloc, 'path': parsedURL.path,
                       'params': parsedURL.params, 'query': parsedURL.query, 'fragment': parsedURL.fragment})

        # get all the links from the page
        data = self.getLinks(data)
        data = self.parseEligibleLinks(data) # queue_links item
        data = self.parseLinkTarget(data) # internal_links, external_links item

        # count links, external, internal
        count_internal = len(data['internal_links'])
        count_external = len(data['external_links'])
        count_all = len(data['all_links'])
        data.update({'count_internal_links': count_internal, 'count_external_links': count_external, 'count_all_links': count_all})

        # calculate size of page
        page_size = len(data['source'])
        data.update({'page_size': page_size})

        # call extensible "customParse" class, offering additional options
        #data = customParse(data)
        return data

class RequestThreading(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.in_queue = queue # queue.put is used to populate the input object with target URL tuples
    def run(self):
        while True:
            target = self.in_queue.get() # gets the next object from the queue?
            data = Crawl().requestURL(target) # target is passed to getURL
            globalData['queue'].append(data)
            self.in_queue.task_done()


def main():
    #Initialize class instances
    crawl = Crawl()
    db = DBOps()
    parse = ParseResponse()
    queue = Queue.Queue()
    numUrls = 0

    #create database
    db.create()

    #start crawl
    url = crawl.getURL()
    while url:
        print ' 000 BEGIN '
        time.sleep(1)
        if not globalData['queue']:
            print 'sreegs', url
            for url in url:
                data = {'url_id': url[0], 'full_url': url[1]}
                queue.put(data)
                numUrls += 1

            for iter in range(globalData['threadLimit']):
                thr = RequestThreading(queue)
                thr.setDaemon(True)
                thr.start()
        else:
            print 'waiting...', len(globalData['queue'])
        print ' 001 DATA ADDED TO globalData QUEUE', len(globalData['queue'])
        # from this point forward, the data object will contain all the information the application uses
        # each function will accept the whole data object, manipulate it as needed, and

        if len(globalData['queue']) == numUrls:
            for data in globalData['queue']:
                print ' 002 PROCESSING DATA OBJECT'
                if data['source']:
                    # pass response object to parser
                    data = parse.main(data)

                    db.addToQueue(data) # add all eligible links from the current url response to the queue
                    db.updateCanonical(data) # add all links on the page to the url_canonical table
                    db.addVisitData(data) # log visit data for the current url
                    db.updatePageRel(data) # add relationships between links to the page_rel table
                    db.removeURLFromQueue(data) # remove the current URL from the queue
                else:
                    db.addVisitData(data) # log visit data for the current url
                    db.removeURLFromQueue(data) # remove the current URL from the queue
            print ' 003 RESETTING DATA OBJECT'
            globalData['queue'] = []
            # get next URL
            url = crawl.getURL()
            queue.join()
            numUrls = 0
main()