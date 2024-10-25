import os
import sys
from enum import Enum
import execjs
import pinyin

from kugou.mid_list import MIDS

sys.path.append("D:\\project\\crawler\\")
import hashlib
import json
import time
import traceback
import urllib
from queue import Queue
from mongoengine import connect, Document, StringField, DictField, BooleanField, DateTimeField, IntField, ListField, LongField
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
    "kg_mid_temp": "28cea9ae48aa5e15caa38f15cf6f7732",
    "kg_dfid": "3Mmzyu2UZ53i3TOXFX132990",
    "kg_dfid_collect": "d41d8cd98f00b204e9800998ecf8427e",
    "Hm_lvt_aedee6983d4cfc62f509129360d6bb3d": "1699000126",
    "KuGooRandom": "66641699012271305",
    "Hm_lpvt_aedee6983d4cfc62f509129360d6bb3d": "1699008049"
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

class KugouSong(Document):
    identify = StringField(required=True)
    name = StringField(required=True)
    url = StringField(required=True)
    info = DictField(required=False)
    update_timestamp = LongField(required=False)
    albums = ListField(required=True)
    crawled = BooleanField(required=True, default=False)
    type = StringField(required=True)
    avg_play_count = LongField(default=0)
    max_play_count = LongField(default=0)
    belongto_album_count = IntField(default=0)
    meta = {
        "strict": True,
        "collection": "songs",
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
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

# 爬取歌单信息
def crawl_songlist(url:str):
    def _is_chinese(text):
        chinese_regex = re.compile('^[\u4e00-\u9fff]+')
        japanese_regex = re.compile('[\u3040-\u309F]+')
        return chinese_regex.search(text) is not None and japanese_regex.search(text) is None

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

        # create crawl song job
        for idx, song in enumerate(songs):
            songurl = song["url"]
            name = song["title"]

            left = songurl.rfind("/")
            right = songurl.rfind(".html")
            identify = songurl[left+1:right] if left >= 0 and right >= 0 else None
            if identify is not None:
                createJob(KugouJob, category="cz_song", name=songurl, param=[identify, name, url, idx])

                # create song object or add to song object
                _create_or_addto_song(identify, name, songurl, songlist)

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
                if len(keyword) == 0 or len(keyword) > 10 or not _is_chinese(keyword): continue  # only Chinese keyword accepted

                kugou_keyword = KugouKeyword.objects(keyword=keyword).first()
                if kugou_keyword is None:
                    keyword_url = "https://www.kugou.com/yy/html/search.html#searchType=special&searchKeyWord=%s" % keyword
                    createJob(KugouJob, category="az_searchkeyword", name=keyword_url, param=[keyword])

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

def _create_or_addto_song(identify:str, name:str, url:str, songlist:KugouSongList):
    songlist_url = songlist.url
    song = KugouSong.objects(identify=identify).first()
    if song is None:
        song = KugouSong(identify=identify, name=name, url=url, crawled=False)
        song.type = SongType.UnDownload.value
        song.albums.append(songlist_url)
        song.avg_play_count = 0
        song.max_play_count = 0
        song.belongto_album_count = 0
    else:  #add to song
        song.albums.append(songlist_url)

    # get the play count from songlist and add to song
    info = songlist.info
    total_play = int(info["total_play_count"])
    song_count = int(info["song_count"])
    song_play = int(total_play / song_count)

    pre_count = song.belongto_album_count
    song.belongto_album_count = pre_count + 1
    song.avg_play_count = int( (song.avg_play_count * pre_count + song_play) / (pre_count + 1) )
    if song_play > song.max_play_count:
        song.max_play_count = song_play

    song.save()


class SongType(Enum):
    UnDownload = "undowload"
    LowPlayCount = "lowplaycount"
    ReadyToDownload = "readytodownload"
    VIPSong = "vipsong"
    Unknown = "unknown"

# 爬取歌曲信息
def crawl_song(url:str, identify:str, name:str, thread_id:int):
    song = KugouSong.objects(identify=identify).first()
    if song is None:
        return True

    jsonobj = _crawl_song_url(url, identify, thread_id)
    if jsonobj is None or "data" not in jsonobj: return False
    info = jsonobj["data"]
    # check if get the right response
    if 'play_url' not in info:
        FileLogger.error("no play_url in the response, maybe blocked")
        return False

    song.info = info
    song.update_timestamp = int(time.time() * 1000)

    song_type = _check_song_type(song)
    song.type = song_type.value
    song.crawled = True
    song.save()

    # create download song job
    if song_type == SongType.ReadyToDownload:
        param = [song.name]
        createJob(KugouJob, category="ah_downloadsong", name=song.url, param=param)
    return True

def _check_song_type(song:KugouSong):
    privilege = song.info["privilege"]
    if song.avg_play_count < 100 * 10000 and song.max_play_count < 500 * 10000:
        return SongType.LowPlayCount
    elif privilege == 8:  # free songs
        return SongType.ReadyToDownload
    elif privilege == 10:  # vip songs
        return SongType.VIPSong
    else:
        return SongType.Unknown

def _crawl_song_url_old(url:str, identify:str):
    session = getHTMLSession()
    try:
        template = "https://wwwapi.kugou.com/yy/index.php?r=play/getdata&callback=jQuery19108931500086689674_%s&dfid=0jI33k2UZ5621ZJPSp4GAWxX&appid=1014&mid=a9ec92f8da70b1cab1692a938d75d5c6&platid=4&encode_album_audio_id=%s&_=%s"
        ts = str(int(time.time() * 1000))
        songurl = template % (ts, identify, ts)
        response = session.get(songurl, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        jsontext = response.text.strip()
        if not jsontext.startswith("jQuery19108931500086689674"): return False
        right = jsontext.rfind(')')
        jsontext = jsontext[41:right]
        jsonobj = json.loads(jsontext)

        session.markRequestSuccess()
        return jsonobj

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return None

def _crawl_song_url(url:str, identify:str, thread_id:int):
    session = getHTMLSession()
    try:
        template = "https://wwwapi.kugou.com/play/songinfo?srcappid=2919&clientver=20000&clienttime=%s&mid=%s&uuid=%s&dfid=%s&appid=1014&platid=4&encode_album_audio_id=%s&token=&userid=0&signature=%s"

        ts = str(int(time.time() * 1000))
        mid = MIDS[thread_id]
        uuid = mid
        dfid = COOKIES["kg_dfid"]

        sign_template = "NVPh5oo715z5DIWAeQlhMDsWXXQV4hwtappid=1014clienttime=%sclientver=20000dfid=%sencode_album_audio_id=%smid=%splatid=4srcappid=2919token=userid=0uuid=%sNVPh5oo715z5DIWAeQlhMDsWXXQV4hwt"
        sign_str = sign_template % (ts, dfid, identify, mid, uuid)
        signature = hashlib.md5(sign_str.encode()).hexdigest()

        songurl = template % (ts, mid, uuid, dfid, identify, signature)
        response = session.get(songurl, headers=HEADERS)  # don't use cookies here
        if response is None: return False
        jsontext = response.text.strip()
        jsonobj = json.loads(jsontext)

        session.markRequestSuccess()
        return jsonobj

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return None

# download song
def download_song(url:str, song_name:str):
    def _get_file_path(song_name):
        basepath = "d:\\test\\"
        pinyin_char = pinyin.get(song_name.strip(), format="strip", delimiter="")
        first_char = pinyin_char[0]
        path = os.path.join(basepath, first_char)
        if not os.path.exists(path):
            os.mkdir(path)

        singers = song_name.split("-")[0]
        first_singer = singers.split("、")[0]
        first_singer = first_singer.replace(" +", "_")
        path = os.path.join(path, first_singer)
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        content = response.content

        # find the right filepath
        extension = "mp3"
        pos = url.rfind(".")
        if pos >= 0:
            extension = url[url.rfind(".")+1:]

        filepath = _get_file_path(song_name)
        filename = "%s.%s" % (song_name, extension)
        filepath = os.path.join(filepath, filename)
        with open(filepath, "wb") as f:
            f.write(content)

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        traceback.print_exc()
        session.markRequestFails()
        return False

def crawl_kugou_job():
    def create_job_worker(item_queue:Queue):
        for job in KugouJob.objects(category="cz_song", finished=False).order_by("+category").limit(500):
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
            succ = crawl_keyword(url, keyword)
        elif category == "bz_songlist":
            succ = crawl_songlist(url)
        elif category == "cz_song":
            param = item["param"]
            succ = crawl_song(url, param[0], param[1], thread_id)
        # elif category == "ah_downloadsong":
        #     song_name = item["param"][0]
            # succ = download_song(url, song_name)

        if succ:
            finishJob(job)
            FileLogger.warning(f"[{thread_id}] success on {url} of {category}")
        else:
            failJob(job)
            FileLogger.error(f"[{thread_id}] fail on {url} of {category}")
        time.sleep(0)
        return succ

    worker = MultiThreadQueueWorker(threadNum=1, minQueueSize=400, crawlFunc=crawl_worker, createJobFunc=create_job_worker)
    worker.start()

def _get_mid():
    js_code = """
function Guid() {
    function e() {
        return (65536 * (1 + Math.random()) | 0).toString(16).substring(1)
    }
    return e() + e() + "-" + e() + "-" + e() + "-" + e() + "-" + e() + e() + e()
}
    """
    ctx = execjs.compile(js_code)
    guid = ctx.call("Guid")
    return guid

# 爬取酷狗的歌曲信息和mp3, 遇到了验证码的困难，改为爬取kuwo的歌曲
if __name__ == "__main__":
    connect(host="192.168.0.116", port=27017, db="kugou", alias="kugou", username="canoxu", password="4401821211", authentication_source='admin')

    # startProxy(mode=ProxyMode.NO_PROXY)
    crawl_kugou_job()

    # download_song("https://webfs.hw.kugou.com/202310241620/3ac9ef45e72247a1ab2f1e1b6752855c/v2/ff0a168777ea56e0737c025774c025b1/G195/M03/1B/09/Y4cBAF5zR7KATFN3ABctjo-3aW0724.mp3", "周杰伦、Lara梁心颐 - 珊瑚海")import os

    # fs = open("/home/cano/list.txt", "r")
    # fs = open("D:/list.txt", "r")
    # count= 0
    #
    # url = fs.readline()
    # url = url.strip()
    # while len(url) > 0:
    #     songlist = KugouSongList.objects(url=url).first()
    #     if songlist is not None:
    #         songs = songlist.songs
    #         for song in songs:
    #             songurl = song["url"]
    #             name = song["title"]
    #             left = songurl.rfind("/")
    #             right = songurl.rfind(".html")
    #             identify = songurl[left + 1:right] if left >= 0 and right >= 0 else None
    #             if identify is not None:
    #                 _create_or_addto_song(identify, name, songurl, songlist)
    #
    #     url = fs.readline()
    #     url = url.strip()
    #
    #     count += 1
    #     if count % 100 == 0:
    #         print(count)
    # _create_or_addto_song(identify, name, songurl, songlist)

    # obj = _crawl_song_url("https://www.kugou.com/mixsong/j2i4n34.html", "j2i4n34")
    # print(obj)