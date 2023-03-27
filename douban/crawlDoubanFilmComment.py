import re
import time
from queue import Queue

from mongoengine import connect, Document, StringField, BooleanField, ListField, IntField, DateTimeField, EmbeddedDocument, EmbeddedDocumentField
from douban.crawlDoubanFilm import DoubanFilm
from utils.Job import createJob, deleteJob, createOrUpdateJob, finishJob, failJob
from utils.httpProxy import getHTMLSession, startProxy, ProxyMode
from utils.logger import FileLogger
from utils.multiThreadQueue import MultiThreadQueueWorker

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
    "Referer": "https://www.douban.com/"
}
COOKIES = {}

class FilmReviewEmbed(EmbeddedDocument):
    reviewId = StringField()
    author = StringField()  # 作者
    authorUrl = StringField()
    stars = StringField()
    createDate = StringField()
    publisher = StringField()  # 发表review的出版方
    publisherUrl = StringField()
    title = StringField()
    content = StringField()  # 内容
    fullContentUrl = StringField() # 获得完整review的链接
    fullContent = StringField() # 完整review， 一开始由于请求量的问题会先空置
    upVote = IntField()
    downVote = IntField()
    reply = IntField()

class FilmReview(Document):
    filmId = StringField(required=True)
    start = StringField(required=True)
    reviews = ListField(EmbeddedDocumentField(FilmReviewEmbed))
    meta = {
        "strcit": True,
        "collection": "filmreview",
        "db_alias": "douban"
    }

class DoubanFilmCommentJob(Document):
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
        "collection": "filmcmtjob",
        "db_alias": "douban"
    }

def createCrawlJob():
    skip = 0
    while True:
        count = 0
        for film in DoubanFilm.objects().order_by("+filmId").skip(skip).limit(1000):
            count += 1
            filmId = film.filmId
            # 不采集短评， 因为影视的短评太多而且没有营养
            if film.reviewNum is not None and film.reviewNum > 0:
                url = "https://movie.douban.com/subject/%s/reviews?sort=time&start=0" % filmId
                createJob(DoubanFilmCommentJob, category="filmreview", name=url, param=[filmId])

        if count == 0: break
        skip += 1000
        print(skip)

def crawlFilmReview(url:str, filmId:str):
    startNumPart = re.search("start=(\d+)", url)
    if startNumPart is None: return False
    startNum = startNumPart.groups()[0]
    filmReview = FilmReview.objects(filmId=filmId, start=startNum).first()
    if filmReview is None:
        filmReview = FilmReview(filmId=filmId, start=startNum)

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        items = response.html.find("#content div.review-list>div")
        if items:
            reviews = []
            for item in items:
                review = FilmReviewEmbed()
                if "data-cid" not in item.attrs: continue
                reviewId = item.attrs["data-cid"]
                review.reviewId = reviewId
                headerElem = item.find("header.main-hd", first=True)
                if headerElem:
                    aTagElem = headerElem.find("a")
                    review.author = aTagElem[1].text if aTagElem is not None and len(aTagElem) >= 2 else ""
                    review.authorUrl = aTagElem[1].attrs["href"] if aTagElem is not None and len(aTagElem) >= 2 else ""
                    spanTagElem = headerElem.find("span.main-title-rating", first=True)
                    review.stars = spanTagElem.attrs["title"] if spanTagElem is not None else None
                    createElem = headerElem.find("span.main-meta", first=True)
                    review.createDate = createElem.text if createElem is not None else None
                    publisherElem = headerElem.find("span.publisher a", first=True)
                    review.publisher = publisherElem.text if publisherElem is not None else None
                    review.publisherUrl = publisherElem.attrs["href"] if publisherElem is not None else None
                mainElem = item.find("div.main-bd", first=True)
                if mainElem:
                    titleElem = mainElem.find("h2", first=True)
                    review.title = titleElem.text if titleElem is not None else ""
                    contentElem = mainElem.find("div.short-content", first=True)
                    contents = contentElem.xpath("//div/text()")
                    review.content = "".join([c.replace("\xa0(", "").strip() for c in contents if len(c.strip()) > 5])
                    review.fullContentUrl = "https://movie.douban.com/j/review/%s/full" % reviewId
                    review.fullContent = ""
                actionElem = item.find("div.action", first=True)
                if actionElem:
                    usefulElem = actionElem.find("#r-useful_count-%s" % reviewId, first=True)
                    review.upVote = int(usefulElem.text) if usefulElem is not None and len(usefulElem.text.strip()) > 0 else 0
                    uselessElem = actionElem.find("#r-useless_count-%s" % reviewId, first=True)
                    review.downVote = int(uselessElem.text) if uselessElem is not None and len(uselessElem.text.strip()) > 0 else 0
                    replyElem = actionElem.find("a.reply", first=True)
                    review.reply = int(replyElem.text.replace("回应", "")) if replyElem is not None and len(replyElem.text.replace("回应", "")) > 0 else 0
                reviews.append(review)

            filmReview.reviews = reviews
            # FileLogger.warning(f"parsed {len(items)} reviews")
            filmReview.save()

        # 下一页
        paginators = response.html.find("div.paginator a")
        for atag in paginators:
            aTagUrl = "https://movie.douban.com/subject/%s/reviews%s" % (filmId, atag.attrs["href"])
            createJob(DoubanFilmCommentJob, category="filmreview", name=aTagUrl, param=[filmId])  # FileLogger.warning(f"create filmreview job on {aTagUrl}")

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

def crawlDouobanFilmComment():
    def createJobWorker(itemQueue:Queue):
        for job in DoubanFilmCommentJob.objects(finished=False).order_by("+tryDate").limit(500):
            url = job.name
            category = job.category
            filmId = job.param[0]
            itemQueue.put({
                "job":job,
                "url":url,
                "category":category,
                "filmId": filmId
            })

    def crawlWorker(threadId:int, item):
        job = item["job"]
        url = item["url"]
        category = item["category"]
        filmId = item["filmId"]
        # FileLogger.info(f"[{threadId}] working on {url} of {category}")
        succ = False
        if category == "filmreview":
            succ = crawlFilmReview(url, filmId)

        if succ:
            finishJob(job)
            FileLogger.warning(f"[{threadId}] success on {url} of {category}")
        else:
            failJob(job)
            FileLogger.error(f"[{threadId}] fail on {url} of {category}")
        time.sleep(1)
        return succ

    worker = MultiThreadQueueWorker(threadNum=100, minQueueSize=500, crawlFunc=crawlWorker, createJobFunc=createJobWorker)
    worker.start()

if __name__ == "__main__":
    connect(db="douban", alias="douban", username="canoxu", password="4401821211", authentication_source='admin')

    # createCrawlJob()

    startProxy(mode=ProxyMode.PROXY_POOL)
    crawlDouobanFilmComment()

    # crawlFilmReview("https://movie.douban.com/subject/1292720/reviews?sort=time&start=40", "1292720")