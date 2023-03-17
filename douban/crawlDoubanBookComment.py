import re
import time
from mongoengine import connect, StringField, BooleanField, DateTimeField, ListField, IntField, Document, EmbeddedDocument, EmbeddedDocumentField
from douban.crawlDoubanBook import DoubanBook
from utils.Job import failJob, finishJob, createJob, createOrUpdateJob
from utils.httpProxy import getHTMLSession
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

class DoubanCommentJob(Document):
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
        "collection": "cmtjob",
        "db_alias": "douban"
    }

class BookCommentEmbed(EmbeddedDocument):
    author = StringField()  # 作者
    authorUrl = StringField()
    stars = StringField()
    createDate = StringField()
    location = StringField()
    vote = IntField()  # XX有用
    content = StringField()  # 内容

class BookComment(Document):
    bookId = StringField(required=True)
    start = StringField(required=True)
    comments = ListField(EmbeddedDocumentField(BookCommentEmbed))

    meta = {
        "strict": True,
        "collection": "bookcomment",
        "db_alias": "douban"
    }

class BookReviewEmbed(EmbeddedDocument):
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

class BookReview(Document):
    bookId = StringField(required=True)
    start = StringField(required=True)
    reviews = ListField(EmbeddedDocumentField(BookReviewEmbed))
    meta = {
        "strcit": True,
        "collection": "bookreview",
        "db_alias": "douban"
    }

def createCrawlJob():
    bookIds = DoubanBook.objects.distinct("bookId")
    count = 0
    for bookId in bookIds:
        book = DoubanBook.objects(bookId=bookId).first()
        if book.commentNum is not None and book.commentNum > 0:
            url = "https://book.douban.com/subject/%s/comments/?status=P&sort=time&percent_type=&start=0&limit=20" % bookId
            createJob(DoubanCommentJob, category="bookcomment", name=url, param=[bookId])
        if book.reviewNum is not None and book.reviewNum > 0:
            url = "https://book.douban.com/subject/%s/reviews?sort=time&start=0" % bookId
            createJob(DoubanCommentJob, category="bookreview", name=url, param=[bookId])

        count += 1
        if count % 500 == 0: print(count)

def crawlBookComment(url:str, bookId:str):
    startNumPart = re.search("start=(\d+)", url)
    if startNumPart is None: return False
    startNum = startNumPart.groups()[0]
    bookComment = BookComment.objects(bookId=bookId, start=startNum).first()
    if bookComment is None:
        bookComment = BookComment(bookId=bookId, start=startNum)

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        items = response.html.find("#comments li.comment-item")
        if items:
            comments = []
            for item in items:
                comment = BookCommentEmbed()
                infoElem = item.find(".comment .comment-info", first=True)
                if infoElem:
                    aTagElem = infoElem.find("a", first=True)
                    comment.author = aTagElem.text if aTagElem is not None else ""
                    comment.authorUrl = aTagElem.attrs["href"] if aTagElem is not None else ""
                    spanTagElem = infoElem.find("span.user-stars", first=True)
                    comment.stars = spanTagElem.attrs["title"] if spanTagElem is not None else None
                    createElem = infoElem.find(".comment-time", first=True)
                    comment.createDate = createElem.text if createElem is not None else None
                    locationElem = infoElem.find(".comment-location", first=True)
                    comment.location = locationElem.text if locationElem is not None else None
                voteElem = item.find(".comment .comment-vote .vote-count", first=True)
                if voteElem:
                    comment.vote = int(voteElem.text)
                contentElem = item.find(".comment .comment-content span", first=True)
                if contentElem:
                    comment.content = contentElem.text
                comments.append(comment)

            bookComment.comments = comments
            # FileLogger.warning(f"parsed {len(items)} comments")
            bookComment.save()

        # 下一页
        paginators = response.html.find("#paginator a")
        for atag in paginators:
            aTagUrl = "https://book.douban.com/subject/%s/comments/%s" % (bookId, atag.attrs["href"])
            createJob(DoubanCommentJob, category="bookcomment", name=aTagUrl, param=[bookId])
            # FileLogger.warning(f"create bookcomment job on {aTagUrl}")

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

def crawlBookReview(url:str, bookId:str):
    startNumPart = re.search("start=(\d+)", url)
    if startNumPart is None: return False
    startNum = startNumPart.groups()[0]
    bookReview = BookReview.objects(bookId=bookId, start=startNum).first()
    if bookReview is None:
        bookReview = BookReview(bookId=bookId, start=startNum)

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        items = response.html.find("#content div.review-list>div")
        if items:
            reviews = []
            for item in items:
                review = BookReviewEmbed()
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
                mainElem = item.find("div.main-bd",first=True)
                if mainElem:
                    titleElem = mainElem.find("h2", first=True)
                    review.title = titleElem.text if titleElem is not None else ""
                    contentElem = mainElem.find("div.short-content",first=True)
                    contents = contentElem.xpath("//div/text()")
                    review.content = "".join([c.replace("\xa0(", "").strip() for c in contents if len(c.strip()) > 5])
                    review.fullContentUrl = "https://book.douban.com/j/review/%s/full" % reviewId
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

            bookReview.reviews = reviews
            # FileLogger.warning(f"parsed {len(items)} reviews")
            bookReview.save()

        # 下一页
        paginators = response.html.find("div.paginator a")
        for atag in paginators:
            aTagUrl = "https://book.douban.com/subject/%s/reviews%s" % (bookId, atag.attrs["href"])
            createJob(DoubanCommentJob, category="bookreview", name=aTagUrl, param=[bookId])
            # FileLogger.warning(f"create bookreview job on {aTagUrl}")

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

# 爬取豆瓣的短评和书评
def crawlDouobanComment():
    def createJobWorker(itemList:list):
        for job in DoubanCommentJob.objects(finished=False).order_by("+tryDate").limit(500):
            url = job.name
            category = job.category
            bookId = job.param[0]
            itemList.append({
                "job":job,
                "url":url,
                "category":category,
                "bookId": bookId
            })

    def crawlWorker(threadId:int, item):
        job = item["job"]
        url = item["url"]
        category = item["category"]
        bookId = item["bookId"]
        # FileLogger.info(f"[{threadId}] working on {url} of {category}")
        succ = False
        if category == "bookcomment":
            succ = crawlBookComment(url, bookId)
        elif category == "bookreview":
            succ = crawlBookReview(url, bookId)

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

    createCrawlJob()

    # crawlDouobanComment()

    # crawlBookComment("https://book.douban.com/subject/36122667/comments/?start=40&limit=20&status=P&sort=time", "36122667")

    # crawlBookReview("https://book.douban.com/subject/36122667/reviews?sort=time&start=20", "36122667")