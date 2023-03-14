import time
from threading import Thread, Lock
from utils.Job import createJob, finishJob, createOrUpdateJob
import datetime
from mongoengine import connect, Document, StringField, IntField, ListField, DateTimeField, BooleanField
from requests_html import HTMLSession
from utils.logger import FileLogger
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6"
}

class XSJob(Document):
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
        "db_alias": "xiaoshuo"
    }

class XiaoShuoBen(Document):
    url = StringField(required=True)
    type = StringField()
    title = StringField()
    author = StringField()
    description = StringField()
    image = StringField()
    date = DateTimeField()
    articleList = ListField()
    lastArticleId = IntField()
    lastCrawledArticleId = IntField()
    updateDate = StringField()
    meta = {
        "strict": True,
        "collection": "xiaoshuoben",
        "db_alias": "xiaoshuo"
    }

class XiaoShuoArticle(Document):
    url = StringField()
    benTitle = StringField()
    title = StringField()
    content = StringField()
    date = DateTimeField()
    meta = {
        "strict": True,
        "collection": "xiaoshuoarticle",
        "db_alias": "xiaoshuo"
    }

def createXiaoShuoBenJobs():
    for benId in range(1, 69318):
        createJob(XSJob, category="ben", name=str(benId))
        print(benId)

def createXiaoShuoArticleJobs():
    titles = XiaoShuoBen.objects().distinct("title")
    for title in titles:
        createJob(XSJob, category="article", name=title)
        print(title)

def refreshNullXiaoShuoBen():
    nullBens = XiaoShuoBen.objects(__raw__ = {'articleList': {'$size': 0}})
    for nullBen in nullBens:
        benId = nullBen.url[25:len(nullBen.url)-1]
        createOrUpdateJob(XSJob, category="ben", name=str(benId))
        print(nullBen.url)

#  https://www.1biqug.net
def crawlXiaoShuoBen():
    for job in XSJob.objects(category="ben", finished=False):
        benId = int(job.name)
        url = "https://www.1biqug.net/9/%d/" % benId
        succ = _crawlXiaoShuoBen(url)
        if succ:
            finishJob(job)

def _crawlXiaoShuoBen(url:str):
    session = HTMLSession()
    try:
        response = session.get(url, headers=HEADERS)
        info = response.html.find("#info", first=True)
        title = info.find("h1", first=True)
        if len(title.text) == 0:
            FileLogger.error(f"no xiaoshuo in benId {url}")
            return True

        titleText = title.text.strip()
        ben = XiaoShuoBen.objects(title=titleText).first()
        if ben is None:
            ben = XiaoShuoBen(title=titleText)
            ben.lastArticleId = 0
            ben.lastCrawledArticleId = 0

        author = info.find("p", first=True)
        description = response.html.find("#intro", first=True)
        image = response.html.find("#fmimg>img", first=True)
        listDl = response.html.find("#list>dl", first=True)
        list = listDl.find("dl>*")

        articleList = ben.articleList if ben.articleList else []
        lastArticleId = ben.lastArticleId if ben.lastArticleId is not None else 0
        hasChange = False
        for item in list:
            if item.tag == "dt": continue
            aTag = item.find("dd>a", first=True)
            if aTag and len(aTag.attrs["href"]) > 0:
                urlStr = aTag.attrs["href"]
                page = urlStr.split("/")[3]
                id = int(page.split(".")[0])
                if lastArticleId < id:
                    lastArticleId = id
                    articleList.append({"url": urlStr, "name": item.text})
                    hasChange = True

        # save to DB
        if hasChange:
            ben.url = url
            ben.date = datetime.datetime.now()
            ben.author = author.text.split("：")[-1] if author is not None else ""
            ben.description = description.text if description is not None else ""
            ben.image = image.attrs["src"] if image is not None else ""
            ben.articleList = articleList
            ben.lastArticleId = lastArticleId
            ben.updateDate = datetime.date.today().strftime("%Y-%m-%d")
            ben.save()
            FileLogger.warning(f"success crawl {titleText} on id: {url}")
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        return False

def _getDomainFromUrl(url:str):
    domain = re.match(r"(http[s]+://www[^/]*/).*", url)
    if len(domain.groups()) == 0:
        FileLogger.error(f"no domain in {url}")
        return None
    domain = domain.groups()[0]
    domain = domain[0:len(domain) - 1] if domain.endswith("/") else domain
    return domain

ArticleList = []
accessLock = Lock()
GlobalSession = HTMLSession()
# python多线程教程：https://www.cnblogs.com/yuanwt93/p/15886333.html
def crawlArticleMultiThread():
    global ArticleList, accessLock
    threadList = []
    for i in range(20):
        thread = Thread(target=crawlArticle, args=[i])
        threadList.append(thread)
        thread.start()
    # [t.join() for t in threadList]

    workingJobs = []
    for job in XSJob.objects(category="article", finished=False):
        title = job.name
        ben = XiaoShuoBen.objects(title=title).first()
        if ben is None:
            FileLogger.error(f"no such ben in {title}")
            finishJob(job)
            continue
        if ben.articleList is None or len(ben.articleList) == 0:
            FileLogger.error(f"article list is empty in {title}")
            continue

        domain = _getDomainFromUrl(ben.url)
        if domain is None: continue

        while len(ArticleList) >= 500:
            time.sleep(1)
        accessLock.acquire()
        while len(workingJobs) >= 5:
            workingJob = workingJobs.pop(0)
            FileLogger.warning(f"finish on {workingJob.name}")
            finishJob(workingJob)
        for article in ben.articleList:
            article["fullUrl"] = domain + article["url"]
            article["benTitle"] = ben.title
            ArticleList.append(article)
        workingJobs.append(job)
        accessLock.release()

def crawlArticle(threadId:int):
    global ArticleList, accessLock

    errorCount = 0
    sleepSeconds = 0
    while True:
        accessLock.acquire()
        articleItem = ArticleList.pop() if len(ArticleList) > 0 else None
        accessLock.release()
        if articleItem is None:
            time.sleep(1)
            sleepSeconds += 1
            if sleepSeconds >= 60:
                break
            else:
                print(f"thread {threadId} sleeps {sleepSeconds} seconds")
                continue
        sleepSeconds = 0

        url = articleItem.get('fullUrl', None)
        title = articleItem.get("name", None)
        benTitle = articleItem.get("benTitle")
        if url is None or title is None:
            FileLogger.error(f"thread {threadId} errors, no such url or title")
            continue

        succ = _crawlArticle(threadId, url, benTitle, title)
        if not succ:
            errorCount += 1
            if errorCount >= 5:
                time.sleep(30)
                errorCount = 0
            else:
                time.sleep(1)

def _crawlArticle(threadId:int, url:str, benTitle:str, title:str):
    global GlobalSession

    article = XiaoShuoArticle.objects(benTitle=benTitle, title=title).first()
    if article is not None:
        FileLogger.warning(f"thread {threadId}: {url} already exists!")
        return

    # crawl from internet
    try:
        response = GlobalSession.get(url, headers=HEADERS, timeout=10)
        content = response.html.find("#content", first=True)
        text = content.text

        article = XiaoShuoArticle(benTitle=benTitle, title=title)
        article.url = url
        article.date = datetime.datetime.now()
        article.content = text
        article.save()
        FileLogger.warning(f"thread {threadId} success on {url}")
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"thread {threadId} error on {url}")
        createJob(XSJob, category="article_error", name=url, param=[url, benTitle, title])
        return False

# 重新爬取之前错误的articles
def reCrawlErrorArticlesMultiThread():
    global ArticleList
    session = HTMLSession()
    domain = "https://www.1biqug.net"

    for job in XSJob.objects(category="article_error", finished=False):
        url = job.param[0]
        benTitle = job.param[1]
        title = job.param[2]
        ArticleList.append({
            "url": url[22:],
            "fullUrl": url,
            "name": title,
            "benTitle": benTitle
        })

    threadList = []
    for i in range(20):
        thread = Thread(target=crawlArticle, args=[i])
        threadList.append(thread)
        thread.start()
    [t.join() for t in threadList]

    for job in XSJob.objects(category="article_error", finished=False):
        url = job.param[0]
        article = XiaoShuoArticle.objects(url=url).first()
        if article is not None:
            finishJob(job)


def refreshAndCreateXiaoshuoBenJob():
    session = HTMLSession()
    url = "https://www.1biqug.net/"
    try:
        response = session.get(url, headers=HEADERS)
        newBenList = response.html.find("#newscontent>div.r>ul>li")
        for li in newBenList:
            linkObj = li.find("span.s2>a", first=True)
            benId = linkObj.attrs['href'].split("/")[2]
            title = linkObj.text.strip()
            createJob(XSJob, category="ben", name=str(benId))
            createJob(XSJob, category="article", name=title)
            print(f"crawl and get new ben: {benId}")

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on refresh ben !")

# refresh every ben to get the update article (id > lastArticleId)
def refreshAllBenArticle():
    titles = XiaoShuoBen.objects().distinct("title")
    count = 0
    for title in titles:
        ben = XiaoShuoBen.objects(title=title).first()
        if ben.lastCrawledArticleId is None: ben.lastCrawledArticleId = 0
        if ben and len(ben.articleList) > 0 and ben.lastCrawledArticleId < ben.lastArticleId:
            domain = _getDomainFromUrl(ben.url)
            if domain is None: continue

            for article in ben.articleList:
                url = article["url"]
                page = url.split("/")[3]
                id = int(page.split(".")[0])
                if ben.lastCrawledArticleId < id <= ben.lastArticleId:
                    _crawlArticle(0, domain+article["url"], ben.title, article["name"])
            ben.lastCrawledArticleId = ben.lastArticleId
            ben.save()

        count += 1
        print(f"finished refresh ben article {count} {title}")

def refreshXiaoShuoBen():
    today = datetime.date.today()
    afterDate = (today - datetime.timedelta(days=365)).strftime("%Y-%m-%d")
    titles = XiaoShuoBen.objects().distinct("title")
    count = 0
    for title in titles:
        ben = XiaoShuoBen.objects(title=title).first()
        if ben.updateDate >= afterDate:
            _crawlXiaoShuoBen(ben.url)

        count += 1
        print(f"finished refresh ben {count} {title}")

def refreshXiaoshuo():
    refreshAndCreateXiaoshuoBenJob()
    crawlXiaoShuoBen()
    refreshXiaoShuoBen()
    refreshAllBenArticle()

if __name__ == "__main__":
    connect(db="xiaoshuo", alias="xiaoshuo", username="canoxu", password="4401821211", authentication_source='admin')

    # createXiaoShuoBenJobs()
    # crawlXiaoShuoBen()
    # refreshNullXiaoShuoBen()
    # createXiaoShuoArticleJobs()

    # print(f"crawl xiaoshuo article!!!!!!")
    # crawlArticleMultiThread()
    # reCrawlErrorArticlesMultiThread()


    # 这个refresh还是有问题。需要debug
    refreshXiaoshuo()

