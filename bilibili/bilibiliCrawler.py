import json
import time
from queue import Queue
from utils.multiThreadQueue import MultiThreadQueueWorker
from mongoengine import connect, Document, StringField, DictField, BooleanField, DateTimeField, IntField, ListField, LongField

from utils.Job import createJob, finishJob, failJob
from utils.httpProxy import getHTMLSession, startProxy, ProxyMode
from utils.logger import FileLogger
from utils.multiThreadQueue import MultiThreadQueueWorker
from kuwo.js_call import js_context, run_inline_javascript

BASIC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
}
BASIC_COOKIES = {
}
HEADERS = []
COOKIES = []

class BilibiliJob(Document):
    category = StringField(required=True)  # job分型
    name = StringField(required=True)  # job名称，一般是标识出不同的job，起job_id作用
    finished = BooleanField(requests=True, default=False)  # 是否已经完成
    createDate = DateTimeField(required=True)  # 创建时间
    tryDate = DateTimeField(required=False)  # 尝试运行时间
    param = ListField(require=False)  # 参数
    lastUpdateDate = DateTimeField(required=False)  # 最后一次更新时间，主要用于需要周期更新的任务
    daySpan = IntField(required=False)  # 每次更新的间隔，主要用于需要周期更新的任务
    score = LongField(required=True, default=0)  # 用于排序
    meta = {
        "strict": True,
        "collection": "job",
        "db_alias": "bilibili"
    }

class BilibiliUser(Document):
    userid = StringField(required=True)
    name = StringField(required=True)
    follower = LongField(required=False)
    following = LongField(required=False)
    taglist = DictField(required=False)
    videolist = ListField(required=False)
    crawledpages = ListField(required=False)
    meta = {
        "strict": True,
        "collection": "user",
        "db_alias": "bilibili"
    }

def crawl_related_videos(thread_id:int, url:str):
    global AVERAGE_SCORE, VIDEO_COUNT
    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS[thread_id], cookies=COOKIES[thread_id])
        if response is None: return False

        script_elems = response.html.find("script")
        script_elem, dataobj = None, None
        for elem in script_elems:
            if elem.attrs.get("src") is not None: continue
            if elem.text.find("related") < 0: continue
            if elem.text.find("window.__INITIAL_STATE__") >= 0:
                script_elem = elem

        if script_elem is not None:
            text = script_elem.text.replace("window.__INITIAL_STATE__", "var dataobj")
            endpos = text.find("};")
            text = text[:endpos+2] if endpos >= 0 else text
            dataobj = run_inline_javascript(text)

        relateds = dataobj.get("related", []) if dataobj is not None else []
        for related in relateds:
            view = related.get("stat", {}).get("view", 0)
            comment = related.get("stat", {}).get("danmaku", 0)
            duration = related.get("duration", 0)
            userid = related.get("owner", {}).get("mid", None)
            username = related.get("owner", {}).get("name", "Unknown")
            video_id = related.get("bvid", None)

            # 根据观看量，评论量和视频时长计算出分值
            score = int( (view + comment * 100) * (duration / 60) )
            if userid is not None:
                userid = str(userid)
                user = BilibiliUser.objects(userid=userid).first()
                if user is None:
                    user = BilibiliUser(userid=userid, name=username)
                    user.save()
                    relation_url = "https://api.bilibili.com/x/relation/stat?vmid=%s" % userid
                    createJob(BilibiliJob, category="bz_relation", name=relation_url)

                    # use current video to find more related videos
                    if video_id is not None:
                        new_url = "https://www.bilibili.com/video/%s/" % video_id
                        job = createJob(BilibiliJob, category="az_relatedvideo", name=new_url)
                        job.score = score
                        job.save()

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling: {url} !")
        session.markRequestFails()
        return True  # drop job if error on this crawl

def crawl_relation(thread_id:int, url:str, userid:str):
    user = BilibiliUser.objects(userid=userid).first()
    if user is None: return False

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS[thread_id], cookies=COOKIES[thread_id])
        if response is None: return False
        jsonobj = json.loads(response.text)

        follower = jsonobj.get("data", {}).get("follower", 0)
        following = jsonobj.get("data", {}).get("following", 0)
        user.follower = follower
        user.following = following
        user.save()

        # 10W以上关注的爬取视频列表
        if follower >= 100000:
            videolist_url = "https://api.bilibili.com/x/space/wbi/arc/search?mid=%s&ps=30&tid=0&pn=1&keyword=&order=pubdate&platform=web&order_avoided=true" % userid
            createJob(BilibiliJob, category="cz_videolist", name=videolist_url, param=[userid, "1"])

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling: {url} !")
        session.markRequestFails()
        return False

def crawl_videolist(thread_id:int, url:str, userid:str, page_num:int):
    user = BilibiliUser.objects(userid=userid).first()
    if user is None: return False

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS[thread_id], cookies=COOKIES[thread_id])
        if response is None: return False
        jsonobj = json.loads(response.text)
        datalist = jsonobj.get("data", {}).get("list", None)
        if datalist is None: return False

        tag_list = datalist.get("tlist", {})
        video_list = datalist.get("vlist", [])
        if user.taglist is None or len(user.taglist) == 0:
            user.taglist = tag_list
        if page_num not in user.crawledpages:
            user.crawledpages.append(page_num)
            for video in video_list:
                user.videolist.append(video)
        user.save()

        # 判断是否还有更多页面，如果有，创建job
        total = jsonobj.get("data", {}).get("page", {}).get("count", "0")
        total = int(total)
        while total > page_num * 30:
            page_num += 1
            new_url = "https://api.bilibili.com/x/space/wbi/arc/search?mid=%s&ps=30&tid=0&pn=%s&keyword=&order=pubdate&platform=web&order_avoided=true" % (userid, str(page_num))
            createJob(BilibiliJob, category="cz_videolist", name=new_url, param=[userid, str(page_num)])

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling: {url} !")
        session.markRequestFails()
        return False

def crawl_bilibili_job(thread_num:int=1):
    def create_job_worker(item_queue:Queue):
        for job in BilibiliJob.objects(finished=False).order_by("-score").limit(500):
            url = job.name
            category = job.category
            param = job.param
            item_queue.put({
                "job": job,
                "url": url,
                "category": category,
                "param": param
            })

    def crawl_worker(thread_id:int, item:object):
        job = item["job"]
        url = item["url"]
        category = item["category"]
        FileLogger.info(f"[{thread_id}] working on {url} of {category}")
        succ = False
        if category == "az_relatedvideo":
            succ = crawl_related_videos(thread_id, url)
        elif category == "bz_relation":
            userid = item["param"][0]
            succ = crawl_relation(thread_id, url, userid)
        elif category == "cz_videolist":
            userid = item["param"][0]
            page_num = int(item["param"][1])
            succ = crawl_videolist(thread_id, url, userid, page_num)

        if succ:
            finishJob(job)
            FileLogger.warning(f"[{thread_id}] success on {url} of {category}")
        else:
            failJob(job)
            FileLogger.error(f"[{thread_id}] fail on {url} of {category}")
        time.sleep(1)
        return succ

    # copy headers and cookies to make sure each thread has its own header and cookies
    for i in range(thread_num+1):
        HEADERS.append(BASIC_HEADERS.copy())
        COOKIES.append(BASIC_COOKIES.copy())

    worker = MultiThreadQueueWorker(threadNum=thread_num, minQueueSize=400, crawlFunc=crawl_worker, createJobFunc=create_job_worker)
    worker.start()

# 爬取bilibili的用户信息
if __name__ == "__main__":
    connect(host="192.168.0.116", port=27017, db="bilibili", alias="bilibili", username="canoxu", password="4401821211", authentication_source='admin')

    startProxy(mode=ProxyMode.PROXY_POOL)
    crawl_bilibili_job(thread_num=20)