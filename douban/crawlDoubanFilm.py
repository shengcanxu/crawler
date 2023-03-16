import re
import time

from mongoengine import connect, Document, StringField, ListField, BooleanField, DateTimeField, IntField, FloatField

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

class FilmJob(Document):
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
        "collection": "filmjob",
        "db_alias": "douban"
    }

# 豆列
class FilmList(Document):
    douListId = StringField(required=True)
    title = StringField()
    follows = IntField()
    filmList = ListField() # 会有重复内容
    meta = {
        "strict": True,
        "collection": "filmlist",
        "db_alias": "douban"
    }

class DoubanFilm(Document):
    filmId = StringField(required=True)
    url = StringField(required=True)
    title = StringField()
    year = StringField() #年份
    picture = StringField()
    director = StringField() # 导演
    scriptWriter = StringField() # 编剧
    actor = StringField() # 主演
    filmType = StringField() # 类型
    website = StringField() # 官方网站
    country = StringField() # 制片国家/地区
    language = StringField() # 语言
    publishDate = StringField() # 上映日期
    length = StringField() # 片长
    otherName = StringField() # 又名
    IMDB = StringField() # IMDb
    firstOutDate = StringField()  # 首播
    seasonNum = StringField()  # 季数
    filmNum = StringField()  # 集数
    lengthEachFilm = StringField()  # 单集片长

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
    celebrityNum = IntField() # 演职员数量
    pictures = ListField() # 相关视频和图片
    discussNum = IntField() # 小组讨论数量
    commentNum = IntField() # 评论数量
    reviewNum = IntField() # 书评数量
    relatedFilms = ListField()

    meta = {
        "strict": True,
        "collection": "film",
        "db_alias": "douban"
    }

# 爬取豆列
def crawlDouList(url:str):
    idPart = re.search("https://www.douban.com/doulist/(\d+)", url)
    if idPart is None: return False
    filmListId = idPart.groups()[0]

    fileList = FilmList.objects(douListId=filmListId).first()
    if fileList is None:
        fileList = FilmList(douListId=filmListId)

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        if fileList.title is None:
            titleElem = response.html.find("#content h1 span", first=True)
            fileList.title = titleElem.text if titleElem and len(titleElem.text) > 0 else None


        aTagList = response.html.find("#content div.doulist-item div.title a")
        if aTagList:
            for atag in aTagList:
                aTagUrl = atag.attrs["href"]
                if aTagUrl.find("movie.douban.com") < 0: continue
                createJob(FilmJob, category="film", name=aTagUrl)

                # TODO： 会有重复的内容
                fileList.filmList.append({"url":aTagUrl, "title":atag.text})
            # FileLogger.warning(f"parsed {len(aTagList)} films")
            fileList.save()

        # 下一页
        paginators = response.html.find("#content div.paginator a")
        for atag in paginators:
            aTagUrl = atag.attrs["href"]
            createJob(FilmJob, category="filmlist", name=aTagUrl)
            # FileLogger.warning(f"create filmlist job on {aTagUrl}")

        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

def crawlFilm(url:str):
    idPart = re.search("https://movie.douban.com/subject/(\d+)", url)
    if idPart is None: return False
    filmId = idPart.groups()[0]

    film = DoubanFilm.objects(filmId=filmId).first()
    if film is None:
        film = DoubanFilm(filmId=filmId)
    film.url = url

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        html = response.html.find("#wrapper", first=True)
        if html is None: return False
        titleElem = html.find("#content h1 span", first=True)
        film.title = titleElem.text if titleElem is not None else ""
        yearElem = html.find("#content h1 span.year", first=True)
        film.year = yearElem.text[1:-1] if yearElem is not None and len(yearElem.text) >=2 else None
        pictureElem = html.find("#mainpic img", first=True)
        film.picture = pictureElem.attrs["src"] if pictureElem is not None else ""
        if len(film.title) == 0 or len(film.picture) == 0:
            session.markRequestFails()
            return False

        # 获得电影基本信息
        infoElem = html.find("#info", first=True)
        if infoElem:
            infos = infoElem.text.split("\n")
            for info in infos:
                textParts = info.split(":")
                if len(textParts) != 2: continue

                textName = textParts[0].strip()
                if textName == "导演":
                    film.director = "".join(textParts[1:])
                elif textName == "编剧":
                    film.scriptWriter = "".join(textParts[1:])
                elif textName == "主演":
                    film.actor = "".join(textParts[1:])
                elif textName == "类型":
                    film.filmType = textParts[1].strip()
                elif textName == "官方网站":
                    film.website = textParts[1].strip()
                elif textName == "制片国家/地区":
                    film.country = textParts[1].strip()
                elif textName == "语言":
                    film.language = textParts[1].strip()
                elif textName == "上映日期":
                    film.publishDate = "".join(textParts[1:])
                elif textName == "片长":
                    film.length = textParts[1].strip()
                elif textName == "又名":
                    film.otherName = "".join(textParts[1:])
                elif textName == "IMDb":
                    film.IMDB = textParts[1].strip()
                elif textName == "首播":
                    film.firstOutDate = textParts[1].strip()
                elif textName == "季数":
                    film.seasonNum = "".join(textParts[1:])
                elif textName == "集数":
                    film.filmNum = "".join(textParts[1:])
                elif textName == "单集片长":
                    film.lengthEachFilm = textParts[1].strip()

        # 获得评分
        rateElem = html.find("#interest_sectl", first=True)
        if rateElem:
            rateNumElem = rateElem.find("strong.rating_num", first=True)
            film.rateNum = float(rateNumElem.text.strip()) if rateNumElem and len(rateNumElem.text.strip()) > 0 else 0
            ratePeople = rateElem.find("a.rating_people span", first=True)
            film.ratePeople = int(ratePeople.text.strip()) if ratePeople and len(ratePeople.text.strip()) > 0 else 0
            starElemList = rateElem.find("span.rating_per")
            if starElemList and len(starElemList) == 5:
                film.star5 = float(starElemList[0].text[0:-1])
                film.star4 = float(starElemList[1].text[0:-1])
                film.star3 = float(starElemList[2].text[0:-1])
                film.star2 = float(starElemList[3].text[0:-1])
                film.star1 = float(starElemList[4].text[0:-1])

        # 在读 读过 想读
        aTagElemList = html.find("div.subject-others-interests-ft a")
        if aTagElemList:
            for aTagElem in aTagElemList:
                textParts = aTagElem.text.split("人")
                if len(textParts) != 2: continue
                textName = textParts[1].strip()
                if textName == "在看":
                    film.reading = int(textParts[0].strip())
                elif textName == "看过":
                    film.readComplete = int(textParts[0].strip())
                elif textName == "想看":
                    film.wantToRead = int(textParts[0].strip())

        # 简介
        contentDesElemList = html.find("#link-report-intra span")
        if contentDesElemList:
            for contentElem in contentDesElemList:
                if len(contentElem.text) >= 10:
                    film.contentDes = contentElem.text
                    break

        # 演职员列表
        # TODO： 从以下链接上获得演员列表：https://movie.douban.com/subject/25848328/celebrities
        celebritiesElem = html.find("#celebrities span.pl a", first=True)
        film.celebrityNum = int(celebritiesElem.text.replace("全部", "").strip()) \
            if celebritiesElem is not None and len(celebritiesElem.text.replace("全部", "").strip()) > 0 else None

        # 相关视频和图片
        picturesElemList = html.find("#related-pic li a")
        if picturesElemList:
            pictures = [picturesElem.attrs["href"] for picturesElem in picturesElemList]
            film.pictures = pictures

        # 小组讨论数量
        discussElem = html.find("div.section-discussion p.pl a", first=True)
        if discussElem:
            textPart = re.search("全部(\d+)条", discussElem.text)
            film.discussNum = int(textPart.groups()[0]) if textPart is not None else None


        # 短评 书评
        commentElem = html.find("#comments-section h2 span.pl a", first=True)
        if commentElem:
            commentParts = commentElem.text.split(" ")
            if len(commentParts) == 3:
                film.commentNum = int(commentParts[1])
        reviewElem = html.find("#reviews-wrapper h2 span.pl a", first=True)
        if reviewElem:
            reviewParts = reviewElem.text.split(" ")
            if len(reviewParts) == 3:
                film.reviewNum = int(reviewParts[1])

        # 也喜欢部分
        aTagList = html.find("#recommendations dl dt a")
        relatedFilms = []
        for aTagElem in aTagList:
            aTagUrl = aTagElem.attrs["href"]
            aTagUrl = aTagUrl.split("?")[0]
            relatedFilms.append(aTagUrl)
            createJob(FilmJob, category="film", name=aTagUrl)  # if re.match(r"^https://book.douban.com/subject/\d+/", aTagUrl):  #     doulistUrl = aTagUrl + "doulists"  #     createJob(DoubanJob, category="bookdoulist", name=doulistUrl)
        film.relatedFilms = relatedFilms
        FileLogger.warning(f"parsed {len(relatedFilms)} books")

        # 豆列（书单）
        doulistElems = html.find("#subject-doulist li a")
        for aTagElem in doulistElems:
            aTagUrl = aTagElem.attrs["href"]
            createJob(FilmJob, category="filmlist", name=aTagUrl)

        film.save()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

def crawlDoubanFilm():
    def createJobWorker(itemList:list):
        for job in FilmJob.objects(finished=False).order_by("+tryDate").limit(500):
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
        if category == "filmlist":
            succ = crawlDouList(url)
        elif category == "film":
            succ = crawlFilm(url)

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

    startProxy(mode=ProxyMode.PROXY_POOL)
    crawlDoubanFilm()

    # crawlFilm("https://movie.douban.com/subject/6893932/?source=2022_annual_movie")

    # crawlDouList("https://www.douban.com/doulist/30299/")