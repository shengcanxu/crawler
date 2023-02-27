##
# 从雪球爬取专栏内容
# 链接： https://xueqiu.com/query/v1/symbol/search/status.json?count=10&comment=0&symbol=SZ002511&hl=0&source=all&sort=alpha&page=7&q=&type=11
##
import json
import time
from enum import Enum
import re
import datetime
from mongoengine import *
from mongoengine import Document
from requests_html import HTMLSession

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
    "Content-Type": "text/html; charset=utf-8",
    "Referer": "https://xueqiu.com/",
    "sec-ch-ua": '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": '1',
}
COOKIES = {
    "device_id": "14361501bb5ad879f853195114489f65",
    "s": "ch11f0aaec",
    "bid": "deedac848c890be06868ef62034afc25_lahn50mk",
    "Hm_lvt_1db88642e346389874251b5a1eded6e3": "1675431605",
    "acw_tc": "2760779816773797051691797e58ccde3b1e6226c517f2b346d3a8135aa403",
    "xq_a_token": "7da3658c0a79fd9ef135510bc5189429ce0e3035",
    "xqat": "7da3658c0a79fd9ef135510bc5189429ce0e3035",
    "xq_r_token": "c4e290f788b8c24ec35bd4b893dc8fa427e1f229",
    "xq_id_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTY3OTk2MjkxMiwiY3RtIjoxNjc3Mzc5Njc5MDg0LCJjaWQiOiJkOWQwbjRBWnVwIn0.j6rTru_0PvFP1zxFwgbimoV4tyt7vKFfIaHvW-QO1evKgGgIAHAacqovQ4Q2V4q3hXS-JJeFSlC00hfigwMH8IV179fDp0URlnS8RHPHCxaZi2SvlbYS0BM6iAPrxy2A3trriUYsrTcO_h08Wy-lExrq8v3VLbtSKKLkWFfHgvv8bOOt37hq_Nug7TnPQxqDI2Rcsyt1948aZYvsfEJCNqmXclOsVYmBs5qoXyMyPWdVSgxnXFEZDhqlPC2DR4H93YP2CtakA_mHAQBwucKPP6r35jeFAOJ9nEmRUq05Hg4q0zdLkHyWR9voAQ3HVd62vVQd4oZnIWj_ghyKfloNFg",
    "u": "921677379705248",
    "Hm_lpvt_1db88642e346389874251b5a1eded6e3": "1677379714"
}


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

class Stock(Document):
    code = StringField(required=True)
    name = StringField(required=True)
    meta = {
        "strict": True,
        'collection': 'stock',
        'db_alias': 'xueqiu',
    }

class Author(DynamicDocument):
    userid = LongField(required=True)
    meta = {
        "strict": False,
        'collection': 'author',
        'db_alias': 'xueqiu',
    }

class Article(DynamicDocument):
    articleid = LongField(required=True)
    url = StringField(required=True)
    content = StringField(required=True)
    content_length = LongField()
    view_count = IntField(required=True)
    author = ReferenceField(Author)
    json = StringField()
    linked_stocks = ListField(default=[])
    linked_users = ListField(default=[])
    linked_topics = ListField(default=[])
    pictures = ListField(default=[])
    meta = {
        "strict": False,
        'collection': 'zhuanlan',
        'db_alias': 'xueqiu',
    }

class JobType(Enum):
    EXPAND_STOCK = "expand_stock"
    GET_AUTHOR = "get_author"
    GET_AUTHOR_MORE = "get_author_more"
    GET_LIST = "get_list"
    GET_ARTICLE = "get_article"

class ZhihuJob(Document):
    url = StringField(required=True)
    type = EnumField(JobType)
    priority = IntField(required=True)
    finished = BooleanField(default=False)
    retrys = IntField(required=True, default=0)
    lastUpdate = DateTimeField()
    updateDaySpan = IntField()
    params = ListField()
    meta = {
        "strict": True,
        'collection': 'job',
        'db_alias': 'xueqiu',
    }

# 用于保存统计的全局变量
JOB_CREATED = 0
JOB_CREATED_LAST = 0
JOB_FINISHED = 0
JOB_FINISHED_LAST = 0
JOB_LAST_TIME = datetime.datetime.now()

# 输出新创建和完成的job数量
def printJobStatistic():
    global JOB_CREATED, JOB_FINISHED, JOB_LAST_TIME, JOB_CREATED_LAST, JOB_FINISHED_LAST
    current = datetime.datetime.now()
    if (current - JOB_LAST_TIME).seconds > 60:
        print("-------job statistic-----------")
        print(f"{JOB_CREATED-JOB_CREATED_LAST} jobs are created, {JOB_FINISHED-JOB_FINISHED_LAST} jobs are finished")
        print(f"total {JOB_CREATED} jobs are created, total {JOB_FINISHED} jobs are finished")
        print("-------------------------------")
        JOB_LAST_TIME = current
        JOB_CREATED_LAST = JOB_CREATED
        JOB_FINISHED_LAST = JOB_FINISHED

def createJob(url:str, jobType:JobType, params=None, updateWhenExists=False):
    priority = 10
    if jobType == JobType.EXPAND_STOCK:
        priority = 10
    elif jobType == JobType.GET_AUTHOR:
        priority = 20
    elif jobType == JobType.GET_AUTHOR_MORE:
        priority = 5
    elif jobType == JobType.GET_LIST:
        priority = 30
    elif jobType == JobType.GET_ARTICLE:
        priority = 40

    if params is None:
        params = []
    job = ZhihuJob.objects(url=url, type=jobType).first()
    if job is not None:
        if not updateWhenExists: return False
        else:
            daySpan = job.updateDaySpan
            lastUpdateDate = job.lastUpdate
            if daySpan is not None and (datetime.datetime.now() - lastUpdateDate).days <= daySpan:
                return False
    else:
        job = ZhihuJob(url=url, type = jobType)
    job.priority = priority
    job.finished = False
    job.lastUpdate = datetime.datetime.now()
    job.updateDaySpan = 1
    job.params = params
    job.save()

    global JOB_CREATED
    JOB_CREATED += 1
    return True

def finishJob(job: ZhihuJob):
    global JOB_FINISHED
    printJobStatistic()

    if job:
        job.finished = True
        job.save()
        JOB_FINISHED += 1
        return job
    else:
        print("job is None")
        return None

# https://xueqiu.com/query/v1/symbol/search/status.json?count=10&comment=0&symbol=SZ002511&hl=0&source=all&sort=alpha&page=1&q=&type=11
# 从雪球股票精华评论中筛选出用户列表。 这里是从https://xueqiu.com/S/SZ002511 上拿到页面并生成N个GET_AUTHOR任务。
def getExpandStock(url: str, session):
    response = session.get(url, headers=HEADERS)
    content = json.loads(response.content)
    if content is None or content.get("maxPage", None) is None:
        return False

    maxPage = content.get("maxPage", -1)
    getAuthorCount = 0
    for i in range(1, maxPage+1):
        pos = url.index("page=")
        joburl = url[:pos+5] + str(i) + "&q=&type=11"
        if i > 5:
            created = createJob(joburl, JobType.GET_AUTHOR_MORE)
        else:
            created = createJob(joburl, JobType.GET_AUTHOR)
        if created: getAuthorCount += 1
    print(f"create {getAuthorCount} get_author jobs")
    return True

# https://xueqiu.com/query/v1/symbol/search/status.json?count=10&comment=0&symbol=SZ002511&hl=0&source=all&sort=alpha&page=1&q=&type=11
# # 从雪球股票精华评论中筛选出用户列表。只选择发表专栏的用户。 flags == 171798694912 or flags == 446676601856是专栏
def getZhuanlanAuthor(url:str, session): # return fetch status, fetch count
    response = session.get(url, headers=HEADERS)
    content = json.loads(response.content)
    if content is None: return False, 0

    itemList = content.get("list", None)
    if itemList is None or len(itemList) == 0:
        return False, 0

    getListCount = 0
    meet_flags = 0
    for item in itemList:
        flags = item.get("flags", 0)
        userid = item.get("user_id", 0)
        if userid != 0 and (flags == 171798694912 or flags == 446676601856):
            jobUrl = "https://xueqiu.com/statuses/original/timeline.json?user_id=%s&page=1" % str(userid)
            created = createJob(jobUrl, JobType.GET_LIST,[userid])
            meet_flags += 1
            if created: getListCount += 1

            user = Author.objects(userid=userid).first()
            userJson = item.get("user", None)
            if user is None and userJson is not None:
                user = Author(userid = userid)
                user.json = userJson
                user.save()
    print(f"create {getListCount} get_list jobs, from {meet_flags} zhuanlan, from {len(itemList)} items ")
    return True, getListCount

# https://xueqiu.com/statuses/original/timeline.json?user_id=9481034446&page=1
# 从用户专栏中爬取文章列表。 以上链接就是从https://xueqiu.com/u/9481034446 中获得
def getZhuanlanList(url:str, userid, session, update=False):  # return fetch status, fetch number
    response = session.get(url, headers=HEADERS)
    content = json.loads(response.content)
    if content is None: return False, 0

    itemList = content.get("list", None)
    if itemList is None:
        print(f"item list is None, should be some error!")
        return False, 0
    elif len(itemList) == 0:  # 空列表当成爬取成功
        print(f"{url} returns null list!!!!!!!")
        return True, 0

    getArticleCount = 0
    for item in itemList:
        articleId = item.get("id", 0)
        view_count = item.get("view_count", 0)
        if articleId != 0:
            jobUrl = "https://xueqiu.com/%s/%s" % (str(userid), str(articleId))
            created = createJob(jobUrl, JobType.GET_ARTICLE, [view_count])
            if created: getArticleCount += 1
    print(f"create {getArticleCount} get_article jobs")

    if not update:
        maxPage = content.get("maxPage", -1)
        page = content.get("page", -1)
        getListCount = 0
        if int(page) <= 2:
            for i in range(1, maxPage):
                jobUrl = "https://xueqiu.com/statuses/original/timeline.json?user_id=%s&page=%i" % (userid, i)
                created = createJob(jobUrl, JobType.GET_LIST, [userid])
                if created: getListCount += 1
            print(f"create {getListCount} get_list jobs")
    else:
        page = content.get("page", -1)
        if len(itemList) == getArticleCount and page != -1:  # 如果当前GET_LIST所有的文章都是新的，就再次扫描下一个page
            newPage = page + 1
            jobUrl = "https://xueqiu.com/statuses/original/timeline.json?user_id=%s&page=%i" % (userid, newPage)
            created = createJob(jobUrl, JobType.GET_LIST, [userid], updateWhenExists=True)
            print(f"create a new get-list job for {userid} on page: {newPage}")

    return True, getArticleCount

# https://xueqiu.com/9481034446/210452094
# 爬取文章内容，并从文章的链接中抽取出后续要爬取的股票
def getZhuanlanArticle(url:str, view_count:int, session):
    response = session.get(url, headers=HEADERS)
    article_content = response.html.find("article.article__bd", first=True)
    if article_content is not None:
        html = article_content.html
        article = Article(url=url)
        article.content = html
        article.content_length = len(html)
        article.view_count = view_count
        articleid = url[url.rfind('/')+1:]
        article.articleid = articleid
        url_part = url[0:url.rfind('/')]
        userid = int(url_part[url_part.rfind('/')+1:])
        author = Author.objects(userid=userid).first()
        if author is not None:
            article.author = author

        # 查找股票链接和用户链接
        links = article_content.find("a")
        linked_stocks = []
        linked_users = []
        linked_topics = []
        for link in links:
            href = link.attrs.get("href", None)
            if href is None: continue
            href = href[0:href.find('?')] if href.find('?') >= 0 else href
            text = link.text
            if href.startswith("http://xueqiu.com/S/"):
                linked_stocks.append({"code":href[20:], "name":text})
            elif href.startswith("http://xueqiu.com/n/"):
                linked_users.append({"url":href, "name":text})
            elif href.startswith("http://xueqiu.com/k"):
                linked_topics.append({"url": href, "name": text})

        article.linked_users = linked_users
        article.linked_stocks = linked_stocks
        article.linked_topics = linked_topics

        # 查找图片链接
        pictures = article_content.find("img")
        picList = []
        for picture in pictures:
            picList.append(picture.attrs["src"])
        article.pictures = picList

        #  查找评论数量等关键数据
        text = [script.text for script in response.html.find("script")
                if script.text.find("window.SNOWMAN_STATUS") >= 0]
        if len(text) > 0:
            article.json = text[0]

        article.save()
        print(f"get an article {articleid}")

        # 增加爬取stock的任务
        for stock in linked_stocks:
            code = stock["code"]
            stock_in_db = Stock.objects(code=code).first()
            if stock_in_db is None:
                newStock = Stock(code=code, name=stock["name"])
                newStock.save()
                stockurl = "https://xueqiu.com/query/v1/symbol/search/status.json?count=10&comment=0&symbol=%s&hl=0&source=all&sort=alpha&page=1&q=&type=11" % code
                createJob(stockurl, JobType.EXPAND_STOCK)
    return True

def getZhuanlanMain(update=False):
    working = True
    session = HTMLSession()
    while working:
        count = 0
        for job in ZhihuJob.objects(finished=False).limit(100).order_by("-priority").no_cache():
            print(f"working on: {job.url} {job.type}")
            count += 1

            try:
                url = job.url
                jobType = job.type
                succ = False
                if jobType == JobType.EXPAND_STOCK:
                    succ = getExpandStock(url,session)
                elif jobType == JobType.GET_AUTHOR or jobType == JobType.GET_AUTHOR_MORE:
                    succ, getListCount = getZhuanlanAuthor(url, session)
                    if getListCount > 0:
                        job.updateDaySpan = 1 # 有更新就缩小更新周期到1天, 否则更新周期每次增加2天
                    else:
                        job.updateDaySpan = 7 if job.updateDaySpan < 7 else job.updateDaySpan + 2
                elif jobType == JobType.GET_LIST:
                    succ, getArticleCount = getZhuanlanList(url, job.params[0], session, update)
                    if getArticleCount > 0 :
                        job.updateDaySpan = 1  # 有更新就缩小更新周期到1天, 否则更新周期每次增加2天
                    else:
                        job.updateDaySpan = 7 if job.updateDaySpan < 7 else job.updateDaySpan + 2
                elif jobType == JobType.GET_ARTICLE:
                    succ = getZhuanlanArticle(url, job.params[0], session)

                if succ:
                    finishJob(job)
                else:
                    job.retrys += 1
                    if job.retrys >= 2: finishJob(job)
                    else: job.save()
                time.sleep(2.0)
            except Exception as ex:
                print(ex)
                print(f"error on url: {job.url}")

        if count == 0: working = False
        print("==============re-fetch!===============")

    session.close()

def refreshCookies():
    session = HTMLSession()
    response = session.get("http://www.xueqiu.com")
    for key in response.cookies.keys():
        COOKIES[key] = response.cookies.get(key)

def refreshZhuanlan():
    # 更新cookies
    refreshCookies()

    # 刷新所有股票的最热评论列表（最热也会参考时间做排序），并刷新专栏列表中的第一页
    print(f"refreshing get author jobs")
    stockCodes = [stock.code for stock in StockInfo.objects(list_status='L')]
    for code in stockCodes:
        url = "https://xueqiu.com/query/v1/symbol/search/status.json?count=10&comment=0&symbol=%s&hl=0&source=all&sort=alpha&page=1&q=&type=11" % code
        createJob(url, JobType.GET_AUTHOR, updateWhenExists=True)

    print(f"refreshing get list jobs")
    userIds = [str(user.userid) for user in Author.objects()]
    for userId in userIds:
        url = "https://xueqiu.com/statuses/original/timeline.json?user_id=%s&page=1" % userId
        createJob(url, JobType.GET_LIST, [int(userId)], updateWhenExists=True)

    getZhuanlanMain(update=True)


if __name__ == "__main__":
    connect(db="xueqiu", alias="xueqiu", username="canoxu", password="4401821211", authentication_source='admin')
    connect(db="stock", alias="stock", username="canoxu", password="4401821211", authentication_source='admin')

    # getZhuanlanMain()

    #  重复爬取只需要将所有的jobType = EXPAND_STOCK 和 jobType = GET_LIST 改成finished=False
    refreshZhuanlan()