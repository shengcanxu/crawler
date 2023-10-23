import hashlib
import json
import time
import traceback
import urllib
from queue import Queue
from mongoengine import connect, Document, StringField, DictField, BooleanField, DateTimeField, IntField, ListField
from utils.Job import createJob, finishJob, failJob
from utils.httpProxy import getHTMLSession, startProxy, ProxyMode
from utils.logger import FileLogger
from utils.multiThreadQueue import MultiThreadQueueWorker
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
    "Referer": "https://www.kugou.com/"
}
COOKIES = {
    "kg_mid": "a9ec92f8da70b1cab1692a938d75d5c6",
    "kg_dfid": "0jI33k2UZ5621ZJPSp4GAWxX",
    "Hm_lvt_aedee6983d4cfc62f509129360d6bb3d": "1697809639",
    "KuGooRandom": "66961697809639528",
    "kg_dfid_collect": "d41d8cd98f00b204e9800998ecf8427e",
    "Hm_lpvt_aedee6983d4cfc62f509129360d6bb3d": "1698058164"
}

class KugouJob(Document):
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
        "db_alias": "kugou"
    }

class KugouKeyword(Document):
    keyword = StringField(required=True)
    json = DictField(required=True)
    meta = {
        "strict": True,
        "collection": "keyword",
        "db_alias": "kugou"
    }

class KugouSongList(Document):
    url = StringField(required=True)
    info = DictField(required=True)
    songs = ListField(required=False, default=[])
    crawled = BooleanField(required=True, default=False)
    meta = {
        "strict": True,
        "collection": "songlist",
        "db_alias": "kugou"
    }

# 使用网页中一样的代码来生成search url
def _gen_keyword_search_url(keyword):
    template = "https://complexsearch.kugou.com/v1/search/special?callback=callback123&keyword=%s&page=1&pagesize=30&userid=0&clientver=20000&platform=WebFilter&iscorrection=1&privilege_filter=0&filter=10&appid=1014&srcappid=2919&clienttime=%s&mid=%s&uuid=%s&dfid=-&signature=%s"
    ts = str(int(time.time() * 1000))
    keyword_quoted = urllib.parse.quote(keyword)

    md5_template = 'NVPh5oo715z5DIWAeQlhMDsWXXQV4hwtappid=1014callback=callback123clienttime=%sclientver=20000dfid=-filter=10iscorrection=1keyword=%smid=%spage=1pagesize=30platform=WebFilterprivilege_filter=0srcappid=2919userid=0uuid=%sNVPh5oo715z5DIWAeQlhMDsWXXQV4hwt'
    string = md5_template % (ts, keyword, ts, ts)
    md5 = hashlib.md5()
    md5.update(string.encode())
    md5_str = md5.hexdigest().upper()

    url = template % (keyword_quoted, ts, ts, ts, md5_str)
    return url

# 通过关键字搜索获得歌单信息
def crawl_keyword(url:str, keyword:str):
    session = getHTMLSession()
    try:
        apiurl = _gen_keyword_search_url(keyword)
        response = session.get(apiurl, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        jsontext = response.text.strip()
        if not jsontext.startswith("callback123("): return False
        jsontext = jsontext[12:len(jsontext)-1]
        jsonobj = json.loads(jsontext)

        kugou_keyword = KugouKeyword.objects(keyword=keyword).first()
        if kugou_keyword is None:
            kugou_keyword = KugouKeyword(keyword=keyword)
        kugou_keyword.json = jsonobj
        kugou_keyword.save()

        # 存储所有的歌曲
        info_list = jsonobj.get("data", {}).get("lists", [])
        for info in info_list:
            songlisturl = info.get("egid", None)
            if songlisturl is not None:
                songlisturl = "https://www.kugou.com/songlist/%s/" % songlisturl
                songlist = KugouSongList.objects(url=songlisturl).first()
                if songlist is None:
                    songlist = KugouSongList(url=songlisturl, info=info, crawled=False)
                    songlist.save()

                    # 新建一个爬取songlist job
                    createJob(KugouJob, category="bz_songlist", name=songlisturl)

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        # traceback.print_exc()
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

# 爬取歌单信息
def crawl_songlist(url:str):
    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        song_lis = response.html.find("#songs li a")
        songs = []
        for li in song_lis:
            attrs = li.attrs
            songurl = attrs.get("href", None)
            if songurl is not None:
                songs.append({
                    "title": attrs.get("title", ""),
                    "url": songurl,
                    "data": attrs.get("data", "")
                })

        # save back to KugouSongList object
        songlist = KugouSongList.objects(url=url).first()
        if songlist is not None:
            songlist.songs = songs
            songlist.crawled = True
            songlist.save()

        # split song name to get the new keywords to search
        for song in songs:
            title = song["title"]
            parts = title.split('-')
            strings = parts[0].split('、')
            if len(parts) > 1:
                strings.append(parts[1])

            for string in strings:
                keyword = re.sub(r"[\(（].*[\)）]", "", string)
                keyword = keyword.strip()
                kugou_keyword = KugouKeyword.objects(keyword=keyword).first()
                if kugou_keyword is None:
                    keyword_url = "https://www.kugou.com/yy/html/search.html#searchType=special&searchKeyWord=%s" % keyword
                    createJob(KugouJob, category="az_searchkeyword", name=keyword_url, param=[keyword])

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        # traceback.print_exc()
        session.markRequestFails()
        return False

def crawl_kugou_job():
    def create_job_worker(item_queue:Queue):
        for job in KugouJob.objects(finished=False).order_by("+category +tryDate").limit(500):
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
        if category == "az_searchkeyword":
            keyword = item["param"][0]
            # print("searching " + keyword)
            succ = crawl_keyword(url, keyword)
        if category == "bz_songlist":
            succ = crawl_songlist(url)

        if succ:
            finishJob(job)
            FileLogger.warning(f"[{thread_id}] success on {url} of {category}")
        else:
            failJob(job)
            FileLogger.error(f"[{thread_id}] fail on {url} of {category}")
        time.sleep(1)
        return succ

    worker = MultiThreadQueueWorker(threadNum=10, minQueueSize=500, crawlFunc=crawl_worker, createJobFunc=create_job_worker)
    worker.start()


if __name__ == "__main__":
    connect(db="kugou", alias="kugou", username="canoxu", password="4401821211", authentication_source='admin')

    startProxy(mode=ProxyMode.PROXY_POOL)
    crawl_kugou_job()

