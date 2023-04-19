import datetime
import hashlib
import json
import math
import random
import time
from queue import Queue

import dateutil
import requests
from mongoengine import connect, Document, BooleanField, StringField, DateTimeField, ListField, IntField, FloatField
from requests import Response
from requests_html import HTMLSession
from utils.Job import createJob, createRefreshJob, failJob, finishJob
from utils.PageBackup import PageBackup
from utils.logger import FileLogger

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
    "Referer": "https://www.futunn.com/",
    "futu-x-csrf-token": "", # 需要从股票首页的META中获得
}
COOKIES = {}
session = HTMLSession()

# https://tushare.pro/document/2?doc_id=25, 更新采用cninfo的数据
class StockInfo(Document):
    code = StringField(required=True)
    symbol = StringField(required=True)
    name = StringField(required=True)
    area = StringField()
    industry = StringField()
    fullname = StringField()
    enname = StringField()
    market = StringField()
    exchange = StringField()
    curr_type = StringField()
    list_status = StringField(required=True) #上市状态 L上市 D退市 P暂停上市
    list_date = StringField(required=True)
    delist_date = StringField()
    is_hs = StringField()
    company_type = IntField(required=True)
    # 从Stockdaily或者Stock中copy而来的指标， 方便聚合展示
    PE = FloatField() # PE_ttm
    marketValue = FloatField()
    close = FloatField()
    ROE_latest = FloatField()
    ROE_year = FloatField()
    meta = {
        "strict": True,
        'collection': 'stocklist',
        'db_alias': 'stock',
    }

class FutuJob(Document):
    category = StringField(required=True)  # job分型
    name = StringField(required=True)  # job名称，一般是标识出不同的job，起job_id作用
    finished = BooleanField(requests=True, default=False)  # 是否已经完成
    createDate = DateTimeField(required=True)  # 创建时间
    tryDate = DateTimeField(required=False)  # 尝试运行时间
    param = ListField(require=False)  # 参数
    lastUpdateDate = DateTimeField(required=False)  # 最后一次更新时间，主要用于需要周期更新的任务
    daySpan = IntField(required=False)  # 每次更新的间隔，主要用于需要周期更新的任务
    meta = {
        "strict": True,
        "collection": "job",
        "db_alias": "futu"
    }

class FutuNews(Document):
    newsId = StringField(required=True) #new id
    url = StringField(required=True)
    createDate = StringField()
    title = StringField()
    content = StringField()
    author = StringField()
    authorUrl = StringField()
    relatedStocks = ListField()
    meta = {
        "strict": True,
        "collection": "news",
        "db_alias": "futu"
    }

# 如果最后两个news已经爬取过了，返回false， 否则返回true
def parseNewsList(newsList:list):
    exists = []
    for news in newsList:
        id = news.get("id", 0)
        url = news.get("url", None)
        if url:
            job = createJob(FutuJob, category="news", name=url, param=[id])
            seconds = (datetime.datetime.now() - job.createDate).seconds
            if seconds > 100:
                exists.append(True)
            else:
                exists.append(False)

    if len(exists) < 2 or (exists[-1] is True and exists[-2] is True):
        return False
    else:
        return True

# 爬取新闻列表
def crawlNewsList(url:str):
    global COOKIES, HEADERS, session

    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES, timeout=10)
        if response is None or response.status_code != 200:
            return False
        COOKIES = session.cookies

        # get futu-x-csrf-token
        metaElem = response.html.xpath("//meta[@name='csrf-token']", first=True)
        if metaElem is not None:
            HEADERS["futu-x-csrf-token"] = metaElem.attrs["content"]
            HEADERS["Referer"] = url
        else:
            return False

        # news list
        strResult = response.html.search('__INITIAL_STATE__{}window._params')
        if strResult is None: return False
        data = strResult[0].strip()
        if data[-1] == ',':
            data = data[:-1]
        if data[0] == '=':
            data = data[1:]
        jsonData = json.loads(data)
        prefetch = jsonData.get("prefetch",{})
        newsList = prefetch.get("newsList", {}).get("list", [])
        stockId = prefetch.get("stockInfo", {}).get("stock_id", 0)
        marketType = prefetch.get("stockInfo", {}).get("market_type", 0)
        newsType = prefetch.get("newsType", 0)
        newsSubType = prefetch.get("newsSubType", 0)
        allNew = parseNewsList(newsList)

        # get seq_mark
        seqMarkMatch = response.html.search('"seq_mark":{},')
        seqMark = seqMarkMatch[0].replace('"', "").strip() if seqMarkMatch is not None else None

        hasNext = True
        while hasNext and allNew and seqMark:
            url = "https://www.futunn.com/quote-api/get-news-list?stock_id=%s&seq_mark=%s&market_type=%s&type=%s&subType=%s" % \
                  (str(stockId), seqMark, str(marketType), str(newsType), str(newsSubType))
            print(url)
            response = session.get(url, headers=HEADERS, cookies=COOKIES, timeout=10)
            jsonData = response.json().get("data", {})
            hasNext = jsonData.get("has_more", False)
            seqMark = jsonData.get("seq_mark", False)
            newsList = jsonData.get("list", [])
            allNew = parseNewsList(newsList)

            time.sleep(0.5)
    except Exception as ex:
        FileLogger.error(ex)
        return False

    return True

# 爬取新闻内容
def crawlNews(url:str, newsId:str, job):
    global session
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES, timeout=10)
        if response is None or response.status_code != 200:
            return False

        news = FutuNews.objects(newsId=newsId).first()
        if news is None:
            news = FutuNews(newsId=newsId)
        news.url = url

        if response.url.find("futunn.com") >= 0:
            crawlFutuNews(newsId, news, response, session)
        elif response.url.find("qq.com") >= 0:
            FileLogger.info("skip QQ crawler")
            job.param.append("QQ")
            # crawlQQNews(newsId, news, response, session)
            return False
        else:
            FileLogger.error("this url is not catched in code, add it")
            return False

        news.save()
        return True
    except Exception as ex:
        FileLogger.error(ex)
        return False

# save to backup file in domain:futunn.com
def crawlQQNews(newsId:str, news:FutuNews, response:Response, session:HTMLSession):
    response.html.render()

    timeElem = response.html.find("#news_time", first=True)
    dateStr = timeElem.text if timeElem is not None else "1990/01/01"
    dateObj = dateutil.parser.parse(dateStr)
    backup = PageBackup(dateObj)
    backup.writeHtml(newsId + ".html", response.text)

    imageElems = response.html.find("#news-text .news-text img")
    for image in imageElems:
        if "src" not in image.attrs: continue
        imageUrl = image.attrs["src"]
        imageRes = session.get(imageUrl, headers=HEADERS, cookies=COOKIES, timeout=10)
        if imageRes is None or imageRes.status_code != 200:
            continue
        backup.writeImage(imageRes.url, imageRes.content)
        time.sleep(0.1)

    # parse content
    news.createDate = dateObj.strftime("%Y-%m-%d")
    titleElem = response.html.find("#news_title span", first=True)
    if titleElem is not None:
        news.title = titleElem.text
    authorElem = response.html.find("#news_source", first=True)
    if authorElem is not None:
        news.author = authorElem.text
        news.authorUrl = authorElem.attrs["href"] if "href" in authorElem.attrs else None
    contentElem = response.html.find("#news-text .news-text", first=True)
    if contentElem is not None:
        news.content = contentElem.text
    news.relatedStocks = []

# save to backup file in domain:futunn.com
def crawlFutuNews(newsId:str, news:FutuNews, response:Response, session:HTMLSession):
    timeElem = response.html.find("div.info-bar .publicTime", first=True)
    dateStr = timeElem.text if timeElem is not None else "1990/01/01"
    dateObj = dateutil.parser.parse(dateStr)
    backup = PageBackup(dateObj)
    backup.writeHtml(newsId + ".html", response.text)

    imageElems = response.html.find("#content img")
    for image in imageElems:
        if "src" not in image.attrs: continue
        imageUrl = image.attrs["src"]
        imageRes = session.get(imageUrl, headers=HEADERS, cookies=COOKIES, timeout=10)
        if imageRes is None or imageRes.status_code != 200:
            continue
        backup.writeImage(imageRes.url, imageRes.content)
        time.sleep(0.1)

    # parse content
    news.createDate = dateObj.strftime("%Y-%m-%d")
    titleElem = response.html.find("#newsDetail h1.title", first=True)
    if titleElem is not None:
        news.title = titleElem.text
    authorElem = response.html.find("div.info-bar a", first=True)
    if authorElem is not None:
        news.author = authorElem.text
        news.authorUrl = authorElem.attrs["href"] if "href" in authorElem.attrs else None
    contentElem = response.html.find("#content", first=True)
    if contentElem is not None:
        news.content = contentElem.text
    relatedStocks = []
    stocksElems = response.html.find("#relatedStockWeb a")
    for atag in stocksElems:
        nameElem = atag.find(".stock-name", first=True)
        if nameElem is not None:
            relatedStocks.append({
                "name": nameElem.text,
                "url": atag.attrs["href"]
            })
    news.relatedStocks = relatedStocks

def refreshCrawl():
    while True:
        count = 0
        for job in FutuJob.objects(finished=False).order_by("tryDate").limit(100):
            count += 1

            url = job.name
            category = job.category
            FileLogger.info(f"working on {url} of {category}")
            succ = False
            try:
                if category == "stocknews":
                    succ = crawlNewsList(url)
                elif category == "news":
                    id = str(job.param[0])
                    succ = crawlNews(url, id, job)

                if succ:
                    finishJob(job)
                    FileLogger.warning(f"success on {url} of {category}")
                else:
                    failJob(job)
                    FileLogger.error(f"fail on {url} of {category}")

            except Exception as ex:
                FileLogger(ex)
                FileLogger(f"error on {url} of {category}")

            time.sleep(0.1)
        if count == 0: break

def createRefreshCrawlJob():
    FileLogger.info("creating refresh jobs")
    for stock in StockInfo.objects(list_status="L"):
        code = stock.code
        code = code[2:] + "-" + code[0:2]
        url = "https://www.futunn.com/stock/%s" % code
        createRefreshJob(FutuJob, category="stocknews", name=url)


# 爬取富图的新闻
if __name__ == "__main__":
    connect(db="futu", alias="futu", username="canoxu", password="4401821211", authentication_source='admin')
    connect(db="stock", alias="stock", username="canoxu", password="4401821211", authentication_source='admin')

    # createRefreshCrawlJob()
    refreshCrawl()

    # succ = crawlNewsList("https://www.futunn.com/stock/600519-SH")
    # crawlNews("https://news.futunn.com/post/21827639?src=43&ns_stock_id=33333243184257&report_type=stock&report_id=29312004", "32198898")


