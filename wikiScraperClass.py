import urllib2
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import json
import sys

# a class for scraping wikipedia-based data sources
class wikiScraper:
    
    def __init__(self, pageName):
        self.urlBase = 'http://en.wikipedia.org'
        self.urlEdits = 'http://en.wikipedia.org/w/index.php?title=' + pageName + '&offset=&limit=500&action=history' 
        self.urlPageViews = 'http://stats.grok.se/json/en/'
        self.urlInfoPage = 'http://en.wikipedia.org/w/index.php?title='+pageName+'&action=info#mw-pageinfo-watchers'
        self.pageName = pageName
        self.grokDate = datetime.datetime(2007, 12, 1)

    @staticmethod
    def extractCounts(soup):
        aux = soup.find("ul", {"id": "pagehistory"})
        editsDict = {}
        editsDict['date'] = []
        editsDict['user'] = []
        for li in aux.findAll('li'):
            dateStr = li.find('a', {'class': 'mw-changeslist-date'}).contents[0]
            dateObj = datetime.datetime.strptime(dateStr.split(',')[1].lstrip(), '%d %B %Y')
            userName = li.find('a', {'class': 'mw-userlink'}).contents[0]
            editsDict['date'].append(dateObj)
            editsDict['user'].append(userName)
        return pd.DataFrame(editsDict)
        
    # extract edit counts of last 'count' number of recent edits from history page itself
    def pullEdits(self):
        print 'Start data pull of ' + self.pageName + ' Wikipedia Edits'
        flag = True
        allDF = []
        
        # start at the base page
        urlEditsIndex = self.urlEdits
        
        # pull all the edits
        while(flag):
            print 'Querying ' + urlEditsIndex
            page = urllib2.urlopen(urlEditsIndex)
            soup = BeautifulSoup(page.read())
        
            # parse the existing soup now
            auxDF = wikiScraper.extractCounts(soup)
            allDF.append(auxDF)

            # try to find additional pages (if they exist)
            newLink = soup.find('a', {'class': 'mw-nextlink'}, href=True)
            
            # this will crawl to the next page
            if newLink is not None:
                urlEditsIndex = self.urlBase + newLink['href']
                print 'Found new page, continuing to crawl...'
            else:
                print 'No more pages, halting'
                flag = False
            
        editsDFRaw = pd.concat(allDF)
        return editsDFRaw
    
    # static method that generates date range beginning from a datetime object until *now*
    # in the format of the stats.grok.se api YYYYMM
    @staticmethod
    def genDateRange(startDate):
        today = datetime.datetime.now()
        startYear = startDate.year
        startMonth = startDate.month
        if startMonth < 10:
            toRet = [str(startYear) + '0' + str(startMonth)]
        else:
            toRet = [str(startYear) + str(startMonth)]
        flag = True
        while(flag):
            tmpAdd = startMonth + 1
            if tmpAdd > 12:
                startYear += 1
                startMonth = 1
            else:
                startMonth = tmpAdd
            if datetime.datetime(startYear, startMonth, 1) > today:
                flag = False
            else:
                if startMonth < 10:
                    toRet.append(str(startYear) + '0' + str(startMonth))
                else:
                    toRet.append(str(startYear) + str(startMonth))
        return toRet

    # sanity check of static method genDateRange
    # print genDateRange(datetime.datetime(2014, 9, 1))
    
    # extract view counts from json service (stats.grok.se)
    def pullViews(self):
        print 'Start data pull of ' + self.pageName + ' Wikipedia views'
        # hit followers page to get the date of creation
        page1 = urllib2.urlopen(self.urlInfoPage)
        soup1 = BeautifulSoup(page1.read())
        dateStr = [x for x in soup1.find('tr', {'id': 'mw-pageinfo-firsttime'}).stripped_strings][1].split(', ')[1]
        
        # cap the history at the grokDate
        startDate = max(self.grokDate, datetime.datetime.strptime(dateStr, '%d %B %Y'))
        
        # generate all of the dates to query
        allMonthQueries = wikiScraper.genDateRange(startDate)
        
        # query all the dates
        allMonths = []
        for amq in allMonthQueries:
            print 'Querying ' + amq
            q = self.urlPageViews + amq + '/' + self.pageName
            page2 = urllib2.urlopen(q)
            datDict = json.load(page2)
            if datDict['daily_views'] != {}:
                qDates = []
                # handle observed data fidelity issue
                for x in datDict['daily_views'].keys():
                    try:
                        if datDict['daily_views'][x] > 0:
                            qDates.append((datetime.datetime.strptime(x, '%Y-%m-%d'), datDict['daily_views'][x]))
                    except:
                        print 'Data error found, skipping ' + x
                allMonths.append(qDates)
            else:
                # do nothing here
                qDates = []

        # concat all the dates together
        finalDates = [x for sublist in allMonths for x in sublist]
        
        # generate view counts per (non-zero dates) + cleanup
        viewDF = pd.DataFrame(finalDates)
        viewDF.columns = ['date', 'wikiViews']
        viewDF.sort('date', inplace=True)
        
        return viewDF

# usage: python wikiScraperClass.py pageName dataDir
# pageName = name of wikipedia page to pull stats from
# dataDir = directory location to store this rawData
def main():
    if len(sys.argv) < 2:
        sys.stderr.write('Error: Must input at least wiki page name! e.g., "Puppy"')
        sys.exit()

    today = datetime.datetime.now()
    todayString = today.strftime('%Y-%m-%d')
    pageName = sys.argv[1] # "Puppy"

    fileNameEdits = pageName + '_' + todayString + '_rawEdits.csv'
    fileNameViews = pageName + '_' + todayString + '_rawViews.csv'
    
    if len(sys.argv) > 2:
        fileNameEdits = sys.argv[2] + '/' + fileNameEdits
        fileNameViews = sys.argv[2] + '/' + fileNameViews

    ws = wikiScraper(pageName)
    rawEdits = ws.pullEdits()
    rawEdits.to_csv(fileNameEdits, index=False, encoding='utf-8')

    rawViews = ws.pullViews()
    rawViews.to_csv(fileNameViews, index=False, encoding='utf-8')
   
if __name__=='__main__':
    main()