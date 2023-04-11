import re
import time

import execjs
from mongoengine import Document, StringField, BooleanField, DateTimeField, ListField, IntField, connect
from requests_html import HTMLSession

from utils.Job import finishJob, failJob, createJob
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

class KuaikanJob(Document):
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
        "db_alias": "kuaikan"
    }

class Commic(Document):
    url = StringField(required=True)
    commicId = StringField(required=True)
    typeName = StringField(required=True)
    areaName = StringField(required=True)
    imageUrl = StringField() # 头图
    title = StringField() # 标题
    author = StringField() # 作者昵称
    intro = StringField() # 简介
    heat = StringField() # 人气值
    up = IntField() # 点赞数
    topicList = ListField() # 漫画章节列表
    meta = {
        "strict": True,
        "collection": "commic",
        "db_alias": "kuaikan"
    }

class CommicTopic(Document):
    commicId = StringField(required=True)
    url = StringField(required=True)
    title = StringField()
    imageList = ListField() # 漫画图片列表

def crawCommic(url:str, typeName:str, areaName:str):
    session = HTMLSession()
    response = session.get(url, headers=HEADERS, cookies=COOKIES)
    if response is None or response.status_code != 200:
        return False
    try:
        response.html.render()
    except Exception as ex:
        print(ex)
        return False

    idPart = re.search("https://www.kuaikanmanhua.com/web/topic/(\d+)", url)
    if idPart is None: return False
    commicId = idPart.groups()[0]
    commic = Commic.objects(commicId = commicId).first()
    if commic is None:
        commic = Commic(commicId = commicId)
    commic.url = url
    commic.typeName = typeName
    commic.areaName = areaName

    topicElem = response.html.find("div.TopicList", first=True)
    if topicElem is None: return False
    recommendElem = response.html.find("div.recommendBox", first=True)

    imageElem = topicElem.find("div.TopicHeader .left .img", first=True)
    commic.imageUrl = imageElem.attrs["src"] if imageElem is not None else None
    titleElem = topicElem.find("div.TopicHeader .right .title", first=True)
    commic.title = titleElem.text if titleElem is not None else ""
    authorElem = topicElem.find("div.TopicHeader .right .nickname", first=True)
    commic.author = authorElem.text if authorElem is not None else None
    introElem = topicElem.find("div.TopicHeader .right .comicIntro .detailsBox", first=True)
    commic.intro = introElem.text if introElem is not None else ""
    heatElem = topicElem.find("div.TopicHeader .right .btnListRight .heat", first=True)
    commic.heat = heatElem.text if heatElem is not None else 0
    upElem = topicElem.find("div.TopicHeader .right .btnListRight .laud .tipTxt", first=True)
    commic.up = int(upElem.text.split(":")[1].replace(",", "")) if upElem is not None and len(upElem.text.strip()) > 0 else 0

    # commic topic list
    topicElems = topicElem.find("div.TopicItem")
    topicList = []
    for topicElem in topicElems:
        topic = {}
        topicUrlElem = topicElem.find("div.cover > a", first=True)
        topic["url"] = "https://www.kuaikanmanhua.com" + topicUrlElem.attrs["href"] if topicUrlElem is not None and topicUrlElem.attrs["href"].startswith("/web/comic") else None
        imageUrlElem = topicElem.find("div.cover img.img", first=True)
        topic["imageUrl"] = imageUrlElem.attrs["src"] if imageUrlElem is not None else None
        lockElem = topicElem.find("div.cover span.lock", first=True)
        topic["isPay"] = True if lockElem is not None else False
        titleNameElem = topicElem.find("div.title a span", first=True)
        topic["title"] = titleNameElem.text if titleNameElem is not None else ""
        heatNumElem = topicElem.find("div.laud", first=True)
        topic["heat"] = heatNumElem.text.replace("Created with Sketch.", "").strip() if heatNumElem is not None and len(heatNumElem.text.strip()) > 0 else 0
        dateElem = topicElem.find("div.date span", first=True)
        topic["date"] = dateElem.text if dateElem is not None else None
        topicList.append(topic)

        if topic["url"] is not None:
            createJob(KuaikanJob, category="topic", name=topic["url"], param=[commicId, topic["title"]])
    commic.topicList = topicList
    commic.save()

    # 推荐, 看作跟当前一样的typeName 和areaName
    if recommendElem:
        recommendList = recommendElem.find(".recommendItem .imgBox")
        for recommend in recommendList:
            link = "https://www.kuaikanmanhua.com" + recommend.attrs["href"]
            createJob(KuaikanJob, category="commic", name=link, param=[typeName, areaName])

    return True

def crawlCommicList(url:str, typeName:str, areaName:str):
    session = HTMLSession()
    response = session.get(url, headers=HEADERS, cookies=COOKIES)
    if response is None or response.status_code != 200:
        return False

    # parse list
    aTagElems = response.html.find("#__layout .tagContent div.ItemSpecial a")
    for aTag in aTagElems:
        commicUrl = "https://www.kuaikanmanhua.com" + aTag.attrs["href"]
        createJob(KuaikanJob, category="commic", name=commicUrl, param=[typeName, areaName])

    # next page
    aTagElems = response.html.find("#__layout ul.pagination li a")
    baseUrl = url[0:url.rindex("=")]
    for aTag in aTagElems:
        num = aTag.text.strip()
        if num == "..." or len(num) == 0:
            continue
        else:
            newUrl = baseUrl + "=" + num
            createJob(KuaikanJob, category="commiclist", name=newUrl, param=[typeName, areaName])
    return True

def crawlCommicTopic(url:str, commicId:str, title:str):
    session = HTMLSession()
    response = session.get(url, headers=HEADERS, cookies=COOKIES)
    if response is None or response.status_code != 200:
        return False

    topic = CommicTopic.objects(commicId = commicId).first()
    if topic is None:
        topic = CommicTopic(commicId = commicId)
    topic.url = url
    topic.title = title

    imageUrlList = []
    imageElems = response.html.find("div.imgList div.img-box img.img")
    for imageElem in imageElems:
        imageUrl = imageElem.attrs["src"]
        imageUrlList.append(imageUrl)
    topic.imageList = imageUrlList
    topic.save()

    # download and save the html page

    return True

def refreshCrawl():
    while True:
        count = 0
        for job in KuaikanJob.objects(finished=False).order_by("tryDate").limit(100):
            time.sleep(1)
            count += 1

            url = job.name
            category = job.category
            FileLogger.info(f"working on {url} of {category}")
            succ = False
            try:
                if category == "commiclist":
                    succ = crawlCommicList(url, job.param[0], job.param[1])
                elif category == "commic":
                    typeName = job.param[0]
                    areaName = job.param[1]
                    succ = crawCommic(url, typeName, areaName)
                elif category == "topic":
                    commicId = job.param[0]
                    title = job.param[1]
                    succ = crawlCommicTopic(url, commicId, title)

                if succ:
                    finishJob(job)
                    FileLogger.warning(f"success on {url} of {category}")
                else:
                    failJob(job)
                    FileLogger.error(f"fail on {url} of {category}")

            except Exception as ex:
                FileLogger(ex)
                FileLogger(f"error on {url} of {category}")

        if count == 0: break

def createCrawlListJobs():
    types = [(20,"恋爱"), (46,"古风"), (80,"穿越"), (77,"大女主"), (47,"青春"),
        (92,"非人类"), (22,"奇幻"), (48,"都市"), (52,"总裁"), (82,"强剧情"),
        (63,"玄幻"),(86,"系统"),(65,"悬疑"),(91,"末世"),(67,"热血"),
        (62,"萌系"),(71,"搞笑"),(89,"重生"),(68,"异能"),(93,"冒险"),
        (85,"武侠"),(72,"竞技"),(54,"正能量")]
    areas = [(2,"国漫"),(3,"韩漫"),(4,"日漫")]
    for (typeId, typeName) in types:
        for (areaId, areaName) in areas:
            url = "https://www.kuaikanmanhua.com/tag/%s?region=%s&pays=0&state=0&sort=1&page=1" % (str(typeId), str(areaId))
            createJob(KuaikanJob, category="commiclist", name=url, param=[typeName, areaName])

# 爬取快看的动漫
if __name__ == "__main__":
    connect(db="kuaikan", alias="kuaikan", username="canoxu", password="4401821211", authentication_source='admin')
    connect(db="stock", alias="stock", username="canoxu", password="4401821211", authentication_source='admin')

    # createCrawlListJobs()
    # refreshCrawl()

    # crawlCommicList("https://www.kuaikanmanhua.com/tag/20?region=2&pays=0&state=0&sort=1&page=1", "恋爱", "国漫")
    crawCommic("https://www.kuaikanmanhua.com/web/topic/726", "恋爱", "国漫")