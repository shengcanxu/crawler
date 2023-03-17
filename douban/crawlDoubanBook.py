import time
import re
from mongoengine import Document
from mongoengine import connect, Document, EmbeddedDocument, ListField, StringField, IntField, FloatField, ListField, DateTimeField, BooleanField
from requests_html import HTMLSession
from utils.httpProxy import ProxyMode, startProxy, getHTMLSession
from utils.Job import createJob, createOrUpdateJob, finishJob, failJob
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

class DoubanJob(Document):
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
        "db_alias": "douban"
    }

# 豆列
class DouList(Document):
    douListId = StringField(required=True)
    title = StringField()
    follows = IntField()
    bookList = ListField() # 会有重复内容
    meta = {
        "strict": True,
        "collection": "doulist",
        "db_alias": "douban"
    }

class DoubanBook(Document):
    bookId = StringField(required=True)
    url = StringField(required=True)
    title = StringField()
    picture = StringField()
    author = StringField() # 作者
    publisher = StringField() # 出版社
    producer = StringField() # 出品方
    subTitle = StringField() # 副标题
    publishDate = StringField() # 出版年
    pageCount = StringField() # 页数
    price = StringField() # 定价
    padType = StringField() # 装帧
    ISBN = StringField() # ISBN
    orginalName = StringField() # 原作名
    booklistName = StringField() # 丛书

    rateNum = FloatField() # 评分
    ratePeople = IntField() # 评价人数
    star5 = FloatField() # 5星
    star4 = FloatField()  # 4星
    star3 = FloatField()  # 3星
    star2 = FloatField()  # 2星
    star1 = FloatField()  # 1星
    reading = IntField() # 在读
    readComplete = IntField() # 读过
    wantToRead = IntField() # 想读
    contentDes = StringField() # 内容简介
    authorDes = StringField() # 作者简介
    catalog = StringField() # 目录
    commentNum = IntField() # 评论数量
    reviewNum = IntField() # 书评数量
    relatedBooks = ListField()

    meta = {
        "strict": True,
        "collection": "book",
        "db_alias": "douban"
    }

# 爬取豆瓣书本的种类列表, https://book.douban.com/tag/?view=type&icn=index-sorttags-all
def crawlCategoryList():
    url = "https://book.douban.com/tag/"
    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        aTagList = response.html.find("div.article table td a")
        for tag in aTagList:
            tagUrl = "https://book.douban.com%s" % tag.attrs["href"]
            createJob(DoubanJob, category="booklist", name=tagUrl)

        session.markRequestSuccess()

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False


# 爬取某一个种类豆瓣书本的列表 e.g.: https://book.douban.com/tag/%E9%9A%8F%E7%AC%94
def crawlBookList(url:str):
    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        aTagList = response.html.find("#subject_list li div.info h2 a")
        for atag in aTagList:
            aTagUrl = atag.attrs["href"]
            createJob(DoubanJob, category="book", name=aTagUrl)
            # if re.match(r"^https://book.douban.com/subject/\d+/", aTagUrl):
            #     doulistUrl = aTagUrl + "doulists"
            #     createJob(DoubanJob, category="bookdoulist", name=doulistUrl)
        FileLogger.warning(f"parsed {len(aTagList)} books")

        # 下一页
        paginators = response.html.find("#subject_list div.paginator a")
        for atag in paginators:
            aTagUrl = "https://book.douban.com%s" % atag.attrs["href"]
            createJob(DoubanJob, category="booklist", name=aTagUrl)
            FileLogger.warning(f"create booklist job on {aTagUrl}")

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

# 爬取book的豆列 列表
def crawlBookDouList(url:str):
    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        aTagList = response.html.find("#content ul.doulist-list li h3 a")
        if aTagList:
            for atag in aTagList:
                aTagUrl = atag.attrs["href"]
                createJob(DoubanJob, category="doulist", name=aTagUrl)
                # FileLogger.warning(f"create doulist job on {aTagUrl}")

        # 下一页
        paginators = response.html.find("#content div.paginator a")
        for atag in paginators:
            aTagUrl = atag.attrs["href"]
            createJob(DoubanJob, category="bookdoulist", name=aTagUrl)
            # FileLogger.warning(f"create bookdoulist job on {aTagUrl}")

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

# 爬取豆列
def crawlDouList(url:str):
    idPart = re.search("https://www.douban.com/doulist/(\d+)", url)
    if idPart is None: return False
    douListId = idPart.groups()[0]

    doulist = DouList.objects(douListId=douListId).first()
    if doulist is None:
        doulist = DouList(douListId=douListId)

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        if doulist.title is None:
            titleElem = response.html.find("#content h1 span", first=True)
            doulist.title = titleElem.text if titleElem and len(titleElem.text) > 0 else None
        followElem = response.html.find("#content a.doulist-followers-link", first=True)
        doulist.follows = int(followElem.text) if followElem is not None and len(followElem.text.strip()) > 0 else 0

        aTagList = response.html.find("#content div.doulist-item div.title a")
        if aTagList:
            for atag in aTagList:
                aTagUrl = atag.attrs["href"]
                if aTagUrl.find("book.douban.com") < 0: continue
                createJob(DoubanJob, category="book", name=aTagUrl)
                # if re.match(r"^https://book.douban.com/subject/\d+/", aTagUrl):
                #     doulistUrl = aTagUrl + "doulists"
                #     createJob(DoubanJob, category="bookdoulist", name=doulistUrl)

                #TODO： 会有重复的内容
                doulist.bookList.append({"url":aTagUrl, "title":atag.text})
            FileLogger.warning(f"parsed {len(aTagList)} books")
            doulist.save()

        # 下一页
        paginators = response.html.find("#content div.paginator a")
        for atag in paginators:
            aTagUrl = atag.attrs["href"]
            createJob(DoubanJob, category="doulist", name=aTagUrl)
            # FileLogger.warning(f"create doulist job on {aTagUrl}")

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

def crawlBook(url:str):
    idPart = re.search("https://book.douban.com/subject/(\d+)", url)
    if idPart is None: return False
    bookId = idPart.groups()[0]

    book = DoubanBook.objects(bookId=bookId).first()
    if book is None:
        book = DoubanBook(bookId=bookId)
    book.url = url

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        html = response.html.find("#wrapper", first=True)
        if html is None:return False
        titleElem = html.find("h1 span", first=True)
        book.title = titleElem.text if titleElem is not None else ""
        pictureElem = html.find("#mainpic img", first=True)
        book.picture = pictureElem.attrs["src"] if pictureElem is not None else ""
        if len(book.title) == 0 or len(book.picture) == 0:
            session.markRequestFails()
            return False

        # 获得书本基本信息
        infoElem = html.find("#info", first=True)
        if infoElem:
            infos = infoElem.text.split("\n")
            for info in infos:
                textParts = info.split(":")
                if len(textParts) != 2: continue

                textName = textParts[0].strip()
                if textName == "作者":
                    book.author = textParts[1].strip()
                elif textName == "出版社":
                    book.publisher = textParts[1].strip()
                elif textName == "出品方":
                    book.producer = textParts[1].strip()
                elif textName == "副标题":
                    book.subTitle = textParts[1].strip()
                elif textName == "出版年":
                    book.publishDate = textParts[1].strip()
                elif textName == "页数":
                    book.pageCount = textParts[1].strip()
                elif textName == "定价":
                    book.price = textParts[1].strip()
                elif textName == "装帧":
                    book.padType = textParts[1].strip()
                elif textName == "ISBN":
                    book.ISBN = textParts[1].strip()
                elif textName == "原作名":
                    book.orginalName = textParts[1].strip()
                elif textName == "丛书":
                    book.booklistName = textParts[1].strip()

        # 获得评分
        rateElem = html.find("#interest_sectl", first=True)
        if rateElem:
            rateNumElem = rateElem.find("strong.rating_num", first=True)
            book.rateNum = float(rateNumElem.text.strip()) if rateNumElem and len(rateNumElem.text.strip()) > 0 else 0
            ratePeople = rateElem.find("a.rating_people span", first=True)
            book.ratePeople = int(ratePeople.text.strip()) if ratePeople and len(ratePeople.text.strip()) > 0 else 0
            starElemList = rateElem.find("span.rating_per")
            if starElemList and len(starElemList) == 5:
                book.star5 = float(starElemList[0].text[0:-1])
                book.star4 = float(starElemList[1].text[0:-1])
                book.star3 = float(starElemList[2].text[0:-1])
                book.star2 = float(starElemList[3].text[0:-1])
                book.star1 = float(starElemList[4].text[0:-1])

        #在读 读过 想读
        aTagElemList = html.find("#collector p.pl a")
        if aTagElemList:
            for aTagElem in aTagElemList:
                textParts = aTagElem.text.split("人")
                if len(textParts) != 2: continue
                textName = textParts[1].strip()
                if textName == "在读":
                    book.reading = int(textParts[0].strip())
                elif textName == "读过":
                    book.readComplete = int(textParts[0].strip())
                elif textName == "想读":
                    book.wantToRead = int(textParts[0].strip())

        # 简介 目录
        contentDesElemList = html.find("#link-report div.intro")
        if contentDesElemList:
            book.contentDes = contentDesElemList[-1].text
        tags = html.find("#content div.related_info>h2,div.indent")
        for index, tag in enumerate(tags):
            if tag.tag == 'h2' and tag.text.strip().startswith("作者简介") and index < len(tags)-1 and tags[index+1] is not None:
                introElem = tags[index+1].find("div.intro", first=True)
                if introElem:
                    book.authorDes = introElem.text
                    break
        catalogElem = html.find("#dir_%s_full" % bookId, first=True)
        if catalogElem:
            book.catalog = catalogElem.text

        # 短评 书评
        commentElem = html.find("#comments-section h2 span.pl a", first=True)
        if commentElem:
            commentParts = commentElem.text.split(" ")
            if len(commentParts) == 3:
                book.commentNum = int(commentParts[1])
        reviewElem = html.find("#reviews-wrapper h2 span.pl a", first=True)
        if reviewElem:
            reviewParts = reviewElem.text.split(" ")
            if len(reviewParts) == 3:
                book.reviewNum = int(reviewParts[1])

        # 也喜欢部分
        bookElemList = html.find("#db-rec-section div.content dt a")
        relatedBooks = []
        for aTagElem in bookElemList:
            aTagUrl = aTagElem.attrs["href"]
            relatedBooks.append(aTagUrl)
            createJob(DoubanJob, category="book", name=aTagUrl)
            # if re.match(r"^https://book.douban.com/subject/\d+/", aTagUrl):
            #     doulistUrl = aTagUrl + "doulists"
            #     createJob(DoubanJob, category="bookdoulist", name=doulistUrl)
        book.relatedBooks = relatedBooks
        FileLogger.warning(f"parsed {len(bookElemList)} books")

        # 豆列（书单）
        doulistElems = html.find("#db-doulist-section li a")
        for aTagElem in doulistElems:
            aTagUrl = aTagElem.attrs["href"]
            createJob(DoubanJob, category="doulist", name=aTagUrl)

        book.save()
        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

def crawlDoubanBook():
    def createJobWorker(itemList:list):
        for job in DoubanJob.objects(finished=False).order_by("+tryDate").limit(500):
            url = job.name
            category = job.category
            itemList.append({
                "job":job,
                "url":url,
                "category":category
            })

    def crawlWorker(threadId:int, item):
        job = item["job"]
        url = item["url"]
        category = item["category"]
        # FileLogger.info(f"[{threadId}] working on {url} of {category}")
        succ = False
        if category == "booklist":
            succ = crawlBookList(url)
        elif category == "bookdoulist":
            succ = crawlBookDouList(url)
        elif category == "doulist":
            succ = crawlDouList(url)
        elif category == "book":
            succ = crawlBook(url)

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

    # crawlCategoryList()

    startProxy(mode=ProxyMode.PROXY_POOL)
    crawlDoubanBook()

    # crawlBook("https://book.douban.com/subject/34501169/")
    # crawlBook("https://book.douban.com/subject/36161618/?icn=index-latestbook-subject")

    # crawlBook("https://book.douban.com/subject/36161618/?icn=index-latestbook-subject")

    # crawlDouList("https://www.douban.com/doulist/1262364/?start=125&sort=time&playable=0&sub_type=")

    # crawlBookDouList("https://book.douban.com/subject/34501169/doulists")
