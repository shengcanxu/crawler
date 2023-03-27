from mongoengine import connect

from futu.news import refreshCrawl, createRefreshCrawlJob

if __name__ == "__main__":
    connect(db="futu", alias="futu", username="canoxu", password="4401821211", authentication_source='admin')
    connect(db="stock", alias="stock", username="canoxu", password="4401821211", authentication_source='admin')

    # createRefreshCrawlJob()
    refreshCrawl()