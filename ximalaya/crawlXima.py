import json
import time
from queue import Queue

from mongoengine import Document, LongField, StringField, DictField, ListField, BooleanField, connect, DateTimeField, IntField

from utils.Job import createJob, finishJob, failJob
from utils.httpProxy import getHTMLSession
from utils.logger import FileLogger
from utils.multiThreadQueue import MultiThreadQueueWorker

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
    "Referer": "https://www.ximalaya.com/"
}

COOKIES = {

}

def create_category_jobs():
    category_ids = [2, 3, 4, 5, 6, 8, 9, 12, 13, 15, 1001, 1002, 1005, 1006, 1054, 1061, 1062, 1065]
    for id in category_ids:
        url = f"https://www.ximalaya.com/revision/category/v2/albums?pageNum=1&pageSize=56&sort=1&categoryId={id}"
        createJob(XimaJob, category="az_albums", name=url, param=[id, 1])

class XimaJob(Document):
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
        "db_alias": "xima"
    }

class XimaAlbum(Document):
    albumId = LongField(required=True)
    albumTitle = StringField()
    isPaid = BooleanField()
    info = DictField()  # 只有从分类可以触达的album才有
    detail = DictField()  # 基本上每个album都有，info里面的信息可以从detail里面获取
    trackList = ListField()
    meta = {
        "strict": True,
        "collection": "albums",
        "db_alias": "xima"
    }

class XimaTrack(Document):
    albumId = LongField(required=True)
    trackId = LongField(required=True)
    title = StringField()
    isPaid = BooleanField()
    pageUriInfo = DictField()
    playInfo = DictField()
    statCountInfo = DictField()
    trackInfo = DictField()
    meta = {
        "strict": True,
        "collection": "tracks",
        "db_alias": "xima"
    }

# 爬取喜马拉雅的album列表， 链接类似于：https://www.ximalaya.com/revision/category/v2/albums?pageNum=2&pageSize=56&sort=1&categoryId=4
def crawl_album_list(url:str, category_id:int, page_num:int):
    session = getHTMLSession()

    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False
        jsonobj = json.loads(response.text)
        albums = jsonobj.get("data", None).get("albums", None)
        if albums is None or len(albums) == 0: return False

        # 爬取album列表
        for album in albums:
            album_id = album.get("albumId", None)
            if album_id is None: continue
            xima_album = XimaAlbum.objects(albumId=album_id).first()
            if xima_album is None:
                xima_album = XimaAlbum(albumId = album_id)

            xima_album.albumTitle = album.get("albumTitle", "")
            xima_album.isPaid = album.get("isPaid", False)
            xima_album.info = album
            xima_album.save()

            # 生成爬取album detail的job
            url = f"https://www.ximalaya.com/album/{id}"
            createJob(XimaJob, category="bz_album_detail", name=url, param=[id])

        # 尝试爬取后面5页
        for i in range(1, 6):
            url = f"https://www.ximalaya.com/revision/category/v2/albums?pageNum={page_num + i}&pageSize=56&sort=1&categoryId={category_id}"
            createJob(XimaJob, category="az_albums", name=url, param=[category_id, page_num + i])

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

# 从album的页面中爬取album的详细信息和第一个track的id
def crawl_album_detail(url:str, album_id:int):
    session = getHTMLSession()

    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        # 找到window.__INITIAL_STATE__ json
        json_script = None
        script_len = 0
        scripts = response.html.find("script")
        for script in scripts:
            if len(script.text) > script_len:
                script_len = len(script.text)
                json_script = script.text
        if json_script is None: return False

        start = json_script.find("window.__INITIAL_STATE__ =")
        end = json_script.rfind("};")
        if start == -1 or end == -1: return False
        json_script = json_script[start + 26:end + 1]
        jsonobj = json.loads(json_script)

        # 提取内容
        detail_json = jsonobj.get("store", {}).get("AlbumDetailPage", {})
        anchor_album_list = detail_json.get("anchorAlbumList", [])
        del detail_json["anchorAlbumList"]
        hot_word_albums = detail_json.get("hotWordAlbums", [])
        del detail_json["hotWordAlbums"]
        track_list = jsonobj.get("store", {}).get("AlbumDetailTrackListV2", {}).get("tracksInfo", {}).get("tracks", [])

        # 保存album detail信息
        xima_album = XimaAlbum.objects(albumId=album_id).first()
        if xima_album is None:
            xima_album = XimaAlbum(albumId=album_id)
            xima_album.albumTitle = detail_json.get("albumPageMainInfo", {}).get("albumTitle", "")
            xima_album.isPaid = detail_json.get("albumPageMainInfo", {}).get("isPaid", False)
        xima_album.detail = detail_json
        xima_album.save()

        # 生成爬取更多的album的job
        for album in anchor_album_list:
            id = album.get("id", None)
            if id is None: continue
            url = f"https://www.ximalaya.com/album/{id}"
            createJob(XimaJob, category="bz_album_detail", name=url, param=[id])
        for album in hot_word_albums:
            id = album.get("id", None)
            if id is None: continue
            url = f"https://www.ximalaya.com/album/{id}"
            createJob(XimaJob, category="bz_album_detail", name=url, param=[id])

        # 生成爬取所有track list的job
        first_track_id = None
        last_track_id = None
        for track in track_list:
            album_id = track.get("albumId", None)
            track_id = track.get("trackId", None)
            if album_id is None or track_id is None: continue
            if first_track_id is None:
                first_track_id = track_id
            last_track_id = track_id

        if first_track_id is not None:
            query_track_url = f"https://www.ximalaya.com/m-revision/page/track/queryRelativeTracksById?trackId={first_track_id}&preOffset=0&nextOffset=100&countKeys=play&order=2"
            createJob(XimaJob, category="cz_query_track", name=query_track_url, param=[first_track_id, album_id])
        if last_track_id is not None:
            query_track_url = f"https://www.ximalaya.com/m-revision/page/track/queryRelativeTracksById?trackId={last_track_id}&preOffset=0&nextOffset=100&countKeys=play&order=2"
            createJob(XimaJob, category="cz_query_track", name=query_track_url, param=[last_track_id, album_id])

        session.markRequestSuccess()
        return True
    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

# 查询更多的track list， 链接类似于：https://www.ximalaya.com/m-revision/page/track/queryRelativeTracksById?trackId=123456&preOffset=0&nextOffset=100&countKeys=play&order=2
def crawl_query_track(url:str, track_id:int, album_id:int):
    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        last_track_id = None
        new_track_ids = []
        jsonobj = json.loads(response.text)
        track_list = jsonobj.get("data", [])
        for track in track_list:
            album_id = track.get('trackInfo', {}).get('albumId', None)
            track_id = track.get("id", None)
            if album_id is None or track_id is None: continue
            xima_track = XimaTrack.objects(albumId=album_id, trackId=track_id).first()
            if xima_track is None:
                xima_track = XimaTrack(albumId = album_id, trackId=track_id)

                xima_track.isPaid = track.get('trackInfo', {}).get('isPaid', False)
                xima_track.title = track.get('trackInfo', {}).get('title', "")
                xima_track.pageUriInfo = track.get('pageUriInfo', {})
                xima_track.playInfo = track.get('playInfo', {})
                xima_track.statCountInfo = track.get('statCountInfo', {})
                xima_track.trackInfo = track.get('trackInfo', {})
                xima_track.save()
                new_track_ids.append(track_id)
            last_track_id = track_id

        # 爬取后面的track_Ids
        if last_track_id is not None:
            query_track_url = f"https://www.ximalaya.com/m-revision/page/track/queryRelativeTracksById?trackId={last_track_id}&preOffset=0&nextOffset=100&countKeys=play&order=2"
            createJob(XimaJob, category="cz_query_track", name=query_track_url, param=[last_track_id, album_id])

        # 更新album
        if len(new_track_ids) > 0:
            xima_album = XimaAlbum.objects(albumId=album_id).first()
            if xima_album is not None:
                for id in new_track_ids:
                    xima_album.trackList.append(id)
                xima_album.save()

        session.markRequestSuccess()
        return True
    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

def crawl_xima_job(thread_num:int=1):
    def create_job_worker(item_queue:Queue):
        # joblist = XimaJob.objects(category__in=["az_albums", "bz_album_detail"], finished=False).order_by("+tryDate").limit(500)
        joblist = XimaJob.objects(finished=False, category="cz_query_track").order_by("+tryDate").limit(500)
        for job in joblist:
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
        if category == "az_albums":
            category_id = item["param"][0]
            page_num = int(item["param"][1])
            succ = crawl_album_list(url, category_id, page_num)
        elif category == "bz_album_detail":
            album_id = item["param"][0]
            succ = crawl_album_detail(url, album_id)
        elif category == "cz_query_track":
            last_track_id = item["param"][0]
            album_id = item["param"][1]
            succ = crawl_query_track(url, last_track_id, album_id)

        if succ:
            finishJob(job)
            FileLogger.warning(f"[{thread_id}] success on {url} of {category}")
        else:
            failJob(job)
            FileLogger.error(f"[{thread_id}] fail on {url} of {category}")
        time.sleep(1)
        return succ

    worker = MultiThreadQueueWorker(threadNum=thread_num, minQueueSize=400, crawlFunc=crawl_worker, createJobFunc=create_job_worker)
    worker.start()


if __name__ == "__main__":
    connect(host="192.168.0.101", port=27017, db="xima", alias="xima", username="canoxu", password="4401821211", authentication_source="admin")
    # create_category_jobs()

    # startProxy(mode=ProxyMode.PROXY_POOL)
    crawl_xima_job(thread_num=10)


    # url = "https://www.ximalaya.com/m-revision/page/track/queryRelativeTracksById?trackId=101621810&preOffset=0&nextOffset=100&countKeys=play&order=2"
    # crawl_query_track(url, 101621810, 16411402)
