import re
import time
import urllib.request
from queue import Queue

import progressbar
from mongoengine import connect, Document, EmbeddedDocument, StringField, IntField, FloatField, ListField, DateTimeField, BooleanField, EmbeddedDocumentField

from librovox.text_retrieval import get_text_data
from utils.Job import createJob, finishJob, failJob
from utils.httpProxy import ProxyMode, startProxy, getHTMLSession
from utils.logger import FileLogger
from utils.multiThreadQueue import MultiThreadQueueWorker

class RequestPBar():
    def __init__(self):
        self.pbar = None

    def __call__(self, block_num, block_size, total_size):
        if not self.pbar:
            self.pbar = progressbar.ProgressBar(maxval=total_size)
            self.pbar.start()

        downloaded = block_num * block_size
        if downloaded < total_size:
            self.pbar.update(downloaded)
        else:
            self.pbar.finish()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6",
    "Referer": "https://librivox.org/search"
}
COOKIES = {
    "PHPSESSID": "t0kfbnj367395ao3385tlujglvba6lit"
}

class LibrivoxJob(Document):
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
        "db_alias": "librivox"
    }

class LibrivoxBook(Document):
    authors = ListField()
    copyright_year = StringField()
    description = StringField()
    bookid = StringField()
    language = StringField()
    num_sections = StringField()
    title = StringField()
    totaltime = StringField()
    totaltimesecs = IntField()
    url_librivox = StringField()
    url_other = StringField()
    url_project = StringField()
    url_rss = StringField()
    url_text_source = StringField()
    url_zip_file = StringField()
    meta = {
        "strict": True,
        "collection": "book",
        "db_alias": "librivox"
    }

class LibrivoxBookChapter(EmbeddedDocument):
    chapterid = StringField()
    url = StringField()
    name = StringField()
    reader_name = StringField()
    reader_id = StringField()
    reader_url = StringField()
    time = StringField()

class LibrivoxBookChapters(Document):
    bookid = StringField()
    chapters = ListField(EmbeddedDocumentField(LibrivoxBookChapter))
    meta = {
        "strict": True,
        "collection": "chapters",
        "db_alias": "librivox"
    }

def crawlBookList(url, offset):
    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        json_data = response.json()
        books = json_data.get("books", [])
        for book in books:
            libribook = LibrivoxBook.objects(bookid=book["id"]).first()
            if libribook is None:
                libribook = LibrivoxBook(bookid=book["id"])

            libribook.authors = book["authors"]
            libribook.copyright_year = book["copyright_year"]
            libribook.description = book["description"]
            libribook.language = book["language"]
            libribook.num_sections = book["num_sections"]
            libribook.title = book["title"]
            libribook.totaltime = book["totaltime"]
            libribook.totaltimesecs = book["totaltimesecs"]
            libribook.url_librivox = book["url_librivox"]
            libribook.url_other = book["url_other"]
            libribook.url_project = book["url_project"]
            libribook.url_rss = book["url_rss"]
            libribook.url_text_source = book["url_text_source"]
            libribook.url_zip_file = book["url_zip_file"]
            libribook.save()

            createJob(LibrivoxJob, "bookinfo", libribook.url_librivox)
            createJob(LibrivoxJob, "booktext", libribook.url_text_source)

        for i in range(1, 10):
            next = offset + i * 100
            url = f'https://librivox.org/api/feed/audiobooks/?offset={next}&format=json&limit=100'
            createJob(LibrivoxJob, "crawlbook", url, param=[next])

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

def crawlBookInfo(url, bookid):
    session = getHTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return False

        trs = response.html.find(".chapter-download tr")
        if trs is None: return False

        libchapters = LibrivoxBookChapters.objects(bookid=bookid).first()
        if libchapters is None:
            libchapters = LibrivoxBookChapters(bookid=bookid)

        chapter_list = []
        for tr in trs:
            tds = tr.find("td")
            if len(tds) != 4: continue
            chapter = LibrivoxBookChapter()
            chapter.chapterid = tds[0].text.split(" ")[-1]
            a = tds[1].find("a", first=True)
            if a:
                chapter.url = a.attrs["href"]
                chapter.name = a.text
            a = tds[2].find("a", first=True)
            if a:
                chapter.reader_url = a.attrs["href"]
                chapter.reader_name = a.text
                chapter.reader_id = a.attrs["href"].split("/")[-1]
            chapter.time = tds[3].text

            chapter_list.append(chapter)
        libchapters.chapters = chapter_list
        libchapters.save()

        session.markRequestSuccess()
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        session.markRequestFails()
        return False

def crawlBookText(url, bookid):
    global Global_Save_Path
    if Global_Save_Path is None or len(Global_Save_Path) == 0:
        print("should set the Global_Save_Path!")
        return False

    if not url.startswith("http"):
        url = "http://" + url
    try:
        full_text = get_text_data(url)
        path = f"{Global_Save_Path}texts/{bookid}.txt"
        with open(path, "w") as fp:
            fp.write(full_text)
        return True

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {url} !")
        return True  # special! still return true even throw errors because some text_urls are unkonwn!

def get_chapters_urls(chapters):
    if len(chapters) == 0:
        return []
    elif len(chapters) == 1:
        return chapters
    else:
        totalmins = 0
        selected = []
        for chapter in chapters[1:]:  # skip the first chapter to make sure the voice is better
            selected.append(chapter)
            mins = int(chapter.time.split(":")[1])
            totalmins += mins
            if totalmins >= 10: break
        return selected

proxy = 'http://127.0.0.1:10809'  # 请替换为实际代理地址和端口
proxy_handler = urllib.request.ProxyHandler({'http': proxy, 'https': proxy})
opener = urllib.request.build_opener(proxy_handler)
def crawlDownloadBook(bookid):
    global Global_Save_Path, opener
    bookchapters = LibrivoxBookChapters.objects(bookid=bookid).first()
    if bookchapters is None: return False

    try:
        chapters = bookchapters.chapters
        chapters = get_chapters_urls(chapters)

        for chapter in chapters:
            url = chapter.url
            out_path = f"{Global_Save_Path}downloads/{chapter.reader_id}_{bookid}_{chapter.chapterid}.mp3"
            print(f"downloading {url}")
            # urllib.request.urlretrieve(url, out_path, RequestPBar())

            # 使用自定义的OpenerDirector对象下载文件
            response = opener.open(url)
            content = response.read()
            # 将文件保存到本地
            with open(out_path, 'wb') as f:
                f.write(content)

        return True
    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling {bookid} !")
        return False

def crawlLibrivox():
    def createJobWorker(itemQueue:Queue):
        for job in LibrivoxJob.objects(finished=False, category="downloadbook").order_by("+tryDate").limit(300):
            url = job.name
            category = job.category
            params = job.param
            itemQueue.put({
                "job":job,
                "url":url,
                "category":category,
                "params": params
            })

    def crawlWorker(threadId:int, item):
        job = item["job"]
        url = item["url"]
        category = item["category"]

        succ = False
        if category == "crawlbook":
            offset = item["params"][0]
            succ = crawlBookList(url, offset)
        elif category == "bookinfo":
            bookid = item["params"][0]
            succ = crawlBookInfo(url, bookid)
        elif category == "booktext":
            bookid = item["params"][0]
            succ = crawlBookText(url, bookid)
        elif category == "downloadbook":
            bookid = url
            succ = crawlDownloadBook(bookid)

        if succ:
            finishJob(job)
            FileLogger.warning(f"[{threadId}] success on {url} of {category}")
        else:
            failJob(job)
            FileLogger.error(f"[{threadId}] fail on {url} of {category}")
        time.sleep(1)
        return succ

    worker = MultiThreadQueueWorker(threadNum=10, minQueueSize=100, crawlFunc=crawlWorker, createJobFunc=createJobWorker)
    worker.start()

Global_Save_Path = "D:/dataset/librivox/"
if __name__ == "__main__":
    connect(host="192.168.0.101", port=27017, db="librivox", alias="librivox", username="canoxu", password="4401821211", authentication_source='admin')
    # connect(db="douban", alias="douban", username="canoxu", password="4401821211", authentication_source='admin')

    # Global_Save_Path = "/home/cano/dataset/librivox/"
    Global_Save_Path = "D:/dataset/librivox/"
    a = "abc"
    crawlLibrivox()

    # bookids = LibrivoxBook.objects().distinct("bookid")
    # for id in bookids:
    #     print(id)
    #     param = [id]
    #     createJob(LibrivoxJob, "downloadbook", id)