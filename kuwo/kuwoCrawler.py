import os
import random
import re
import sys

from enum import Enum
from threading import Thread

import pinyin
import hashlib
import json
import time
import traceback
import urllib
import math
from queue import Queue
from mongoengine import connect, Document, StringField, DictField, BooleanField, DateTimeField, IntField, ListField, LongField
from requests_html import HTMLSession

from utils.Job import createJob, finishJob, failJob
from utils.httpProxy import getHTMLSession, startProxy, ProxyMode
from utils.logger import FileLogger
from utils.multiThreadQueue import MultiThreadQueueWorker
from kuwo.js_call import js_context, run_inline_javascript

DOWNLOAD_BASE_PATH = "D:/songfiles/"
BASIC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
    "Referer": "https://www.kuwo.cn/",
    "Secret": None
}
BASIC_COOKIES = {
    "Hm_Iuvt_cdb524f42f0cer9b268e4v7y735ewrq2324": None,  # 随着请求经常变化, 一般在请求之前会有set-cookies
    "Hm_lpvt_cdb524f42f0ce19b169a8071123a4797": "1699117858",
    "Hm_lvt_cdb524f42f0ce19b169a8071123a4797": "1699109316"
}
HEADERS = []
COOKIES = []

class KuwoJob(Document):
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
        "db_alias": "kuwo"
    }

class KuwoKeyword(Document):
    keyword = StringField(required=True)
    songlists = ListField(required=True)
    crawledpages = ListField(required=True)
    meta = {
        "strict": True,
        "collection": "keyword",
        "db_alias": "kuwo"
    }

class KuwoSongList(Document):
    identify = StringField(required=True)
    url = StringField(required=True)
    info = DictField(required=True)
    exinfo = DictField(required=False)
    songs = ListField(required=False, default=[])
    crawledpages = ListField(required=False)
    meta = {
        "strict": True,
        "collection": "songlist",
        "db_alias": "kuwo"
    }

class KuwoSong(Document):
    identify = StringField(required=True)
    url = StringField(required=True)
    info = DictField(required=True)
    lyric = ListField(required=False)
    songlists = ListField(required=True)
    isvip = BooleanField(required=True)
    playurl = StringField(required=False)
    updatetime = LongField(required=False)
    crawled = BooleanField(required=True, default=False)
    filepath = StringField(required=False)
    type = StringField(required=True)
    avg_play_count = LongField(default=0)
    max_play_count = LongField(default=0)
    songlistcount = IntField(default=0)
    meta = {
        "strict": True,
        "collection": "songs",
        "db_alias": "kuwo"
    }

class SongType(Enum):
    UnDownload = "undowload"
    LowPlayCount = "lowplaycount"
    ReadyToDownload = "readytodownload"
    VIPSong = "vipsong"
    NotChineseSong = "notchinesesong"
    TooLongName = "toolongname"
    Unknown = "unknown"

def _encode_secret(token):
    secret = js_context.call("encode_secret", token, "Hm_Iuvt_cdb524f42f0cer9b268e4v7y735ewrq2324")
    return secret

def _get_reqid():
    reqid = js_context.call("encode_reqid")
    return reqid

# 使用网页中一样的代码来生成search url
def _gen_keyword_search_url(thread_id:int, keyword:str, page_num:int, hm_value:str):
    secret = _encode_secret(hm_value)
    HEADERS[thread_id]["Secret"] = secret
    template = "https://www.kuwo.cn/api/www/search/searchPlayListBykeyWord?key=%s&pn=%s&rn=30&httpsStatus=1&reqId=%s&plat=web_www&from="
    reqid = _get_reqid()
    url = template % (keyword, str(page_num), reqid)
    return url

# 使用网页中一样的代码来生成songlist url
def _gen_songlist_url(thread_id:int, songlist_id:str, page_num:int, hm_value:str):
    secret = _encode_secret(hm_value)
    HEADERS[thread_id]["Secret"] = secret
    template = "https://www.kuwo.cn/api/www/playlist/playListInfo?pid=%s&pn=%s&rn=20&httpsStatus=1&reqId=%s&plat=web_www"
    reqid = _get_reqid()
    url = template % (songlist_id, str(page_num), reqid)
    return url

def _gen_song_playurl(thread_id:int, song_id:str, hm_value:str):
    secret = _encode_secret(hm_value)
    HEADERS[thread_id]["Secret"] = secret
    template = "https://www.kuwo.cn/api/v1/www/music/playUrl?mid=%s&type=music&httpsStatus=1&reqId=%s&plat=web_www&from="
    reqid = _get_reqid()
    url = template % (song_id, reqid)
    return url

def _check_song_type(song:KuwoSong):
    if song.songlistcount < 20 and song.avg_play_count < 5000 and song.max_play_count < 10000:
        return SongType.LowPlayCount

    artist = song.info.get("artist", "englistartist")
    song_name = song.info.get("name", "englistname")
    if len(artist) > 20 or len(song_name) > 20:
        return SongType.TooLongName
    if not _is_chinese(artist) and not _is_chinese(song_name):
        return SongType.NotChineseSong

    if song.isvip is True:  # vip songs
        return SongType.VIPSong
    else:  # free songs
        return SongType.ReadyToDownload

def _create_or_addto_song(song_id:str, songinfo:dict, songlist:KuwoSongList):
    songurl = "https://www.kuwo.cn/play_detail/%s" % song_id
    song = KuwoSong.objects(identify=song_id).first()
    if song is None:
        song = KuwoSong(identify=song_id, url=songurl, info=songinfo, songlists=[])
        vipstring = songinfo.get("payInfo", {}).get("play", "0000")
        song.isvip = True if vipstring == "1111" else False

        song.type = SongType.UnDownload.value
        song.avg_play_count = 0
        song.max_play_count = 0
        song.songlistcount = 0
    song.songlists.append(songlist.identify)

    info = songlist.exinfo
    total_play = int(info.get("listencnt", 0))
    song_count = int(info.get("total", 0))
    song_play = int(total_play / song_count)

    pre_count = song.songlistcount
    song.songlistcount = pre_count + 1
    song.avg_play_count = int( (song.avg_play_count * pre_count + song_play) / (pre_count + 1) )
    if song_play > song.max_play_count:
        song.max_play_count = song_play

    song_type = _check_song_type(song)
    song.type = song_type.value
    song.crawled = True
    song.save()
    return song

def _get_hm_cookie(thread_id:int):
    if HEADERS[thread_id]["Secret"] is None:
        session = getHTMLSession()
        pageurl = "https://www.kuwo.cn"
        response = session.get(pageurl)
        if response is None: return False
        new_cookie = response.headers["Set-Cookie"]
        new_cookie = re.sub(r";.*", "", new_cookie)
        hm_value = new_cookie.split("=")[1]
        COOKIES[thread_id]["Hm_Iuvt_cdb524f42f0cer9b268e4v7y735ewrq2324"] = hm_value
        return hm_value
    else:
        hm_value = COOKIES[thread_id]["Hm_Iuvt_cdb524f42f0cer9b268e4v7y735ewrq2324"]
        return hm_value

def _set_hm_cookie(thread_id:int, headers):
    new_cookie = headers.get("Set-Cookie", None)
    if new_cookie is not None and len(new_cookie) >= 76:
        new_cookie = re.sub(r";.*", "", new_cookie)
        hm_value = new_cookie.split("=")[1]
        COOKIES[thread_id]["Hm_Iuvt_cdb524f42f0cer9b268e4v7y735ewrq2324"] = hm_value

def _is_chinese(text):
    chinese_regex = re.compile('^[\u4e00-\u9fff]+')
    japanese_regex = re.compile('[\u3040-\u309F]+')
    return chinese_regex.search(text) is not None and japanese_regex.search(text) is None

# 通过关键字搜索获得歌单信息
def crawl_keyword(thread_id:int, url:str, keyword:str, page_num:int):
    session = getHTMLSession()
    hm_value = _get_hm_cookie(thread_id)

    try:
        apiurl = _gen_keyword_search_url(thread_id, keyword, page_num, hm_value)
        response = session.get(apiurl, headers=HEADERS[thread_id], cookies=COOKIES[thread_id])
        if response is None: return False
        _set_hm_cookie(thread_id, response.headers)
        jsonobj = json.loads(response.text)

        songlists = jsonobj.get("data", {}).get("list", [])

        kuwo_keyword = KuwoKeyword.objects(keyword=keyword).first()
        if kuwo_keyword is None:
            kuwo_keyword = KuwoKeyword(keyword=keyword, songlists=[], crawledpages=[])
        if page_num not in kuwo_keyword.crawledpages:
            kuwo_keyword.crawledpages.append(page_num)
            for songlist in songlists:
                kuwo_keyword.songlists.append(songlist)
        kuwo_keyword.save()

        # 存储所有的歌单
        for info in songlists:
            songlist_id = info.get("id", None)
            if songlist_id is not None:
                songlist_id = str(songlist_id)
                songlisturl = "https://www.kuwo.cn/api/www/playlist/playListInfo?pid=%s&pn=%s&rn=20" % (songlist_id, "1")
                songlist = KuwoSongList.objects(identify=songlist_id).first()
                if songlist is None:
                    songlist = KuwoSongList(identify=songlist_id, url=songlisturl, info=info, crawledpages=[])
                    songlist.save()

                    # 新建一个爬取songlist job
                    createJob(KuwoJob, category="bz_songlist", name=songlisturl, param=[songlist_id, "1"])

        # 判断是否还有更多keyword页面，如果有，创建job
        total = jsonobj.get("data", {}).get("total", "0")
        total = int(total)
        while total > page_num * 30:
            page_num += 1
            new_url = "https://www.kuwo.cn/api/www/search/searchPlayListBykeyWord?key=%s&pn=%s&rn=30" % (keyword, str(page_num))
            createJob(KuwoJob, category="az_searchkeyword", name=new_url, param=[keyword, str(page_num)])

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

# 爬取歌单信息
def crawl_songlist(thread_id:int, songlist_id:str, page_num:int):
    session = getHTMLSession()
    hm_value = _get_hm_cookie(thread_id)
    try:
        apiurl = _gen_songlist_url(thread_id, songlist_id, page_num, hm_value)
        response = session.get(apiurl, headers=HEADERS[thread_id], cookies=COOKIES[thread_id])
        if response is None: return False
        _set_hm_cookie(thread_id, response.headers)
        jsonobj = json.loads(response.text)

        # save back to KuwoSongList object
        songlist = KuwoSongList.objects(identify=songlist_id).no_dereference().first()
        if songlist is None: return False

        data = jsonobj.get("data", {})
        musicList = data.get("musicList", [])
        exinfo = data
        del exinfo["musicList"]
        if songlist.exinfo is None or len(songlist.exinfo) == 0:
            songlist.exinfo = exinfo
        if page_num not in songlist.crawledpages:
            songlist.crawledpages.append(page_num)
            for music in musicList:
                songlist.songs.append(music)
        songlist.save()

        # 存储所有的歌曲
        for songinfo in musicList:
            song_id = songinfo.get("rid", None)
            if song_id is not None:
                song_id = str(song_id)
                song = _create_or_addto_song(song_id, songinfo, songlist)

                # 新建一个爬取song job
                songurl = "https://www.kuwo.cn/play_detail/%s" % song_id
                createJob(KuwoJob, category="cz_song", name=songurl, param=[song_id])
                # 如果不是VIP， 创建一个爬取播放url链接的job
                if song.isvip is False and song.type == SongType.ReadyToDownload.value:
                    playurl = "https://www.kuwo.cn/api/v1/www/music/playUrl?mid=%s" % song_id
                    createJob(KuwoJob, category="dz_playurl", name=playurl, param=[song_id])

        # 判断是否还有更多songlist页面，如果有，创建job
        total = data.get("total", "0")
        total = int(total)
        while total > page_num * 20:
            page_num += 1
            new_url = "https://www.kuwo.cn/api/www/playlist/playListInfo?pid=%s&pn=%s&rn=20" % (songlist_id, str(page_num))
            createJob(KuwoJob, category="bz_songlist", name=new_url, param=[songlist_id, str(page_num)])

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling songlist: {songlist_id} !")
        session.markRequestFails()
        return False

# 爬取歌曲信息, 由于歌曲信息在之前已经爬取，这里主要是爬取歌词
def crawl_song(thread_id:int, url:str, song_id:str):
    song = KuwoSong.objects(identify=song_id).first()
    if song is None: return True

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS[thread_id], cookies=COOKIES[thread_id])
        if response is None: return False

        script_elems = response.html.find("script")
        script_elem = None
        for elem in script_elems:
            if elem.attrs.get("src") is not None: continue
            if elem.text.find("window.__NUXT__") >= 0:
                script_elem = elem

        if script_elem is not None:
            text = script_elem.text.replace("window.__NUXT__", "var dataobj")
            dataobj = run_inline_javascript(text)

        datalist = dataobj.get("data", [])
        if len(datalist) == 0: return False
        data = datalist[0]
        lrclist = data.get("lrclist", [])

        song.lyric = lrclist
        song.crawled = True
        song.save()

        # 从歌曲中分离keyword生成job
        artist = song.info.get("artist", None)
        song_name = song.info.get("name", None)
        for string in [artist, song_name]:
            if string is None: continue
            keyword = re.sub(r"[\(（].*[\)）]", "", string)
            keyword = keyword.strip()
            if len(keyword) == 0 or len(keyword) > 10 or not _is_chinese(keyword): continue  # only Chinese keyword accepted

            kugou_keyword = KuwoKeyword.objects(keyword=keyword).first()
            if kugou_keyword is None:
                keyword_url = "https://www.kuwo.cn/api/www/search/searchPlayListBykeyWord?key=%s&pn=%s&rn=30" % (keyword, "1")
                createJob(KuwoJob, category="az_searchkeyword", name=keyword_url, param=[keyword, "1"])

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling song: {song_id} !")
        session.markRequestFails()
        return False

# 获得播放url
def crawl_playurl(thread_id:int, url:str, song_id:str):
    song = KuwoSong.objects(identify=song_id).first()
    if song is None: return True

    session = getHTMLSession()
    hm_value = _get_hm_cookie(thread_id)
    try:
        apiurl = _gen_song_playurl(thread_id, song_id, hm_value)
        HEADERS[thread_id]["Referer"] = "https://www.kuwo.cn/play_detail/%s" % song_id
        response = session.get(apiurl, headers=HEADERS[thread_id], cookies=COOKIES[thread_id])
        if response is None: return False
        _set_hm_cookie(thread_id, response.headers)
        HEADERS[thread_id]["Referer"] = "https://www.kuwo.cn/"
        jsonobj = json.loads(response.text)

        playurl = jsonobj.get("data", {}).get("url", None)
        if playurl is not None:
            song.playurl = playurl
            song.updatetime = int(time.time() * 1000)
            song.crawled = True
            song.save()

            # create download job
            artist = song.info.get("artist", "未知")
            song_name = song.info.get("name", "未知")
            if artist != "未知" or song_name != "未知":
                createJob(KuwoJob, category="ez_download", name=playurl, param=[song_id, artist, song_name])

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling playurl: {song_id} !")
        session.markRequestFails()
        return False

# 下载mp3， 只能下载免费的
def download_song(thread_id:int, url:str, song_id:str, artist:str, song_name:str):
    if DOWNLOAD_BASE_PATH is None:
        FileLogger.error("please set the DOWNLOAD_BASE_PATH !")
        return False
    song = KuwoSong.objects(identify=song_id).first()
    if song is None: return True

    session = HTMLSession()  # direct download, don't use any proxy
    try:
        response = session.get(url, headers=HEADERS[thread_id], cookies=COOKIES[thread_id])
        if response is None: return False

        # find the file path
        name_pinyin = pinyin.get(artist, format="strip")
        first_char = name_pinyin[0].upper() if len(name_pinyin) > 0 else "A"
        filepath = os.path.join(DOWNLOAD_BASE_PATH, first_char)
        if not os.path.exists(filepath):
            os.mkdir(filepath)

        artist = re.sub(r"[\\/:\*\?\"\<\>\|：？“《》]", "", artist)
        song_name = re.sub(r"[\\/:\*\?\"\<\>\|：？“《》]", "", song_name)
        filepath = os.path.join(filepath, artist)
        if not os.path.exists(filepath):
            os.mkdir(filepath)
        filename = artist + "-" + song_name + ".mp3"
        filepath = os.path.join(filepath, filename)

        with open(filepath, "wb") as fp:
            fp.write(response.content)
            song.filepath = filepath
            song.save()

        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling download_song: {song_id} !")
        return False

def download_worker(thread_id:int):
    while True:
        time.sleep(5)
        for job in KuwoJob.objects(category="ez_download", finished=False).limit(50):
            url = job.name
            category = job.category
            param = job.param
            identify = param[0]
            artist = param[1]
            song_name = param[2]

            succ = download_song(thread_id, url, identify, artist, song_name)
            if succ:
                finishJob(job)
                FileLogger.warning(f"[{thread_id}] success on {url} of {category}")
            else:
                failJob(job)
                FileLogger.error(f"[{thread_id}] fail on {url} of {category}")
            time.sleep(0)

def crawl_kuwo_job(thread_num:int=1):
    def create_job_worker(item_queue:Queue):
        # ez_download jobs less than 1000, then just crawl bz_songlist to add more download jobs
        unfinished_download = KuwoJob.objects(category="ez_download", finished=False).count()
        if unfinished_download > 1000:
            joblist = KuwoJob.objects(category__ne="ez_download", finished=False).order_by("-category").limit(500)
        else:
            joblist = KuwoJob.objects(category__in=["az_searchkeyword", "bz_songlist", "dz_playurl"], finished=False).order_by("-category").limit(500)

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
        if category == "az_searchkeyword":
            keyword = item["param"][0]
            page_num = int(item["param"][1])
            succ = crawl_keyword(thread_id, url, keyword, page_num)
        elif category == "bz_songlist":
            identify = item["param"][0]
            page_num = int(item["param"][1])
            succ = crawl_songlist(thread_id, identify, page_num)
        elif category == "cz_song":
            identify = item["param"][0]
            succ = crawl_song(thread_id, url, identify)
        elif category == "dz_playurl":
            identify = item["param"][0]
            succ = crawl_playurl(thread_id, url, identify)

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

    # start a separate thread to downlaod song files
    # thread = Thread(target=download_worker, args=[thread_num])
    # thread.start()

    worker = MultiThreadQueueWorker(threadNum=thread_num, minQueueSize=400, crawlFunc=crawl_worker, createJobFunc=create_job_worker)
    worker.start()

if __name__ == "__main__":
    # connect(db="kuwo", alias="kuwo", username="canoxu", password="4401821211", authentication_source='admin')
    connect(host="192.168.0.116", port=27017, db="kuwo", alias="kuwo", username="canoxu", password="4401821211", authentication_source='admin')

    # DOWNLOAD_BASE_PATH = "D:/test/"
    DOWNLOAD_BASE_PATH = "/home/cano/songfiles/"

    startProxy(mode=ProxyMode.PROXY_POOL)
    crawl_kuwo_job(thread_num=1)

