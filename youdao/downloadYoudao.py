# 从有道网页中爬取中英翻译的结构数据
import json
import time
from enum import Enum
import re

import execjs
import pyppeteer
from utils.logger import FileLogger
from mongoengine import Document, StringField, EnumField, BooleanField, IntField, DynamicDocument, DictField, ListField, connect
from requests_html import HTMLSession, HTML

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Host': 'www.youdao.com',
    'Referer': 'https://www.youdao.com/',
    'sec-ch-ua': 'Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': 'Windows',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
}
COOKIES = {
    'OUTFOX_SEARCH_USER_ID_NCOO': '847821535.1620756',
    'OUTFOX_SEARCH_USER_ID': '677558371@10.110.96.154',
    '___rl__test__cookies': '1661827924686',
}

class JobType(Enum):
    GET_TRANSLATE = "get_translate"
    GET_SUGGESTION = "get_suggestion"

class Job(Document):
    word = StringField(required=True)
    type = EnumField(JobType)
    priority = IntField(required=True)
    finished = BooleanField(required=True, default=False)
    retrys = IntField(required=True, default=0)
    meta = {
        "strict": True,
        'collection': 'job',
        'db_alias': 'youdao',
    }

class YoudaoTranslation(DynamicDocument):
    word = StringField()
    url = StringField()
    content = ListField()
    meta = {
        "strict": False,
        'collection': 'translation',
        'db_alias': 'youdao',
    }

#创建更多爬取job
def createCrawlJob(word:str, type:JobType):
    word = word.strip()
    word = re.sub(r"\s+", " ", word)
    job = Job.objects(word=word.lower(), type=type).first()
    if job is None:
        priority = 20 if type == JobType.GET_TRANSLATE else 10
        job = Job(word=word, type=type, priority=priority)
        job.save()

# 拆分词组保证每个单词都有爬取
def createSplitWordJob(word:str):
    word = word.strip().lower()
    word = re.sub(r"\s+", " ", word)
    wordParts = word.split(" ")
    if len(wordParts) > 1:
        for part in wordParts:
            if not part.isalpha(): continue
            createCrawlJob(part, JobType.GET_TRANSLATE)

DOC = "<a href='www.baidu.com' />"
SCRIPT = "() => { %s; return cano_youdao_value;}"
pyppeteer.DEBUG = True  # print suppressed errors as error log

session = HTMLSession()
def crawlWord(word:str):
    translation = YoudaoTranslation.objects(word=word).first()
    if translation is not None: return True

    renderSession = HTMLSession()
    scriptHtml = HTML(session=renderSession, html=DOC)

    succ = False
    try:
        wordStr = word.replace(" ", "%20")
        link = "https://www.youdao.com/result?word=%s&lang=en" % wordStr
        response = session.get(link, headers=HEADERS, cookies=COOKIES)
        if response.content:
            scripts = response.html.xpath("//body/script")
            scriptText = None
            for s in scripts:
                if s.text and s.text.find("window.__NUXT__") >= 0:
                    scriptText = s.text.replace("window.__NUXT__", "cano_youdao_value")
            if scriptText is not None:
                scriptText = SCRIPT % scriptText

                val = scriptHtml.render(script=scriptText, reload=True)
                datas = val.get("data", None)

                if datas is not None:
                    translation = YoudaoTranslation(word=word, url=link)
                    translation.content = datas
                    translation.save()
                    FileLogger.info(f"get translation of '{word}'")
                    succ = True

                    if len(datas) > 0 and datas[0].get("wordData", None) is not None:
                        wordData = datas[0].get("wordData")
                        # 同根词
                        relWords = wordData.get("rel_word", {}).get("rels", [])
                        for relWord in relWords:
                            rel = relWord.get("rel", {})
                            words = rel.get("words", [])
                            for wordObj in words:
                                w = wordObj.get("word", None)
                                if w is not None:
                                    createCrawlJob(w, JobType.GET_TRANSLATE)
                        # if len(relWords) > 0: FileLogger.info(f"create {len(relWords)} group 同根词")

                        # 同近义词
                        synos = wordData.get("syno", {}).get("synos", [])
                        for syno in synos:
                            words = syno.get("ws", [])
                            for w in words:
                                createCrawlJob(w, JobType.GET_TRANSLATE)
                        # if len(synos) > 0: FileLogger.info(f"create {len(synos)} group 同近义词")

                        #词典短语
                        phrs = wordData.get("phrs", {}).get("phrs", [])
                        for phr in phrs:
                            w = phr.get("headword", None)
                            if w is not None:
                                createCrawlJob(w, JobType.GET_TRANSLATE)
                        # if len(phrs) > 0: FileLogger.info(f"create {len(phrs)} 词典短语")

                        # 例句
                        sentences = wordData.get("blng_sents_part", {}).get("sentence-pair", [])
                        for sentence in sentences:
                            sent = sentence["sentence"]
                            createSplitWordJob(sent)

                        # wiki
                        wikiSummarys = wordData.get("wikipedia_digest", {}).get("summarys", [])
                        for summary in wikiSummarys:
                            sent = summary["summary"]
                            createSplitWordJob(sent)

                    # # 创建爬取suggestion
                    # createCrawlJob(word, JobType.GET_SUGGESTION)

                    # 拆分词组保证每个单词都有爬取
                    createSplitWordJob(word)

    except Exception as ex:
        succ = False
        FileLogger.error(ex)
        FileLogger.error(f"error on getting word {word}")
        time.sleep(1)

    renderSession.close()
    return succ

def suggestWord(word:str):
    succ = False
    try:
        wordStr = word.replace(" ", "%20")
        link = "https://dict.youdao.com/suggest?num=30&ver=3.0&doctype=json&cache=false&le=en&q=%s" % wordStr
        response = session.get(link, headers=HEADERS, cookies=COOKIES)
        if response.content:
            jsonContent = json.loads(response.content)
            entities = jsonContent["data"]["entries"] if jsonContent["data"] and jsonContent["data"]["entries"] else []
            for entity in entities:
                newWord = entity["entry"]
                # 创建爬取 word job
                createCrawlJob(newWord, JobType.GET_TRANSLATE)

    except Exception as ex:
        succ = False
        FileLogger.error(ex)
        FileLogger.error(f"error on getting suggestion based on {word}")
        time.sleep(1)
    return succ

def crawlTranslation():
    while True:
        jobs = Job.objects(finished=False).order_by("-priority").limit(100)
        if len(jobs) == 0: break
        FileLogger.info(f"get another {len(jobs)} jobs and craw.")

        for job in jobs:
            word = job.word
            succ = True
            if job.type == JobType.GET_TRANSLATE:
                succ = crawlWord(word)
            elif job.type == JobType.GET_SUGGESTION:
                succ = suggestWord(word)
            if succ or job.retrys >= 5:
                job.finished = True
                job.save()
            else:
                job.finished = False
                job.retrys += 1
                job.save()

            time.sleep(0.5)

if __name__ == "__main__":
    connect(db="youdao", alias="youdao", username="canoxu", password="4401821211", authentication_source='admin')
    crawlTranslation()

    # # 125000
    # i = 0
    # for k in range(0, 20):
    #     for tran in YoudaoTranslation.objects().skip(i).limit(10000):
    #         if len(tran.content) == 0: continue
    #         wordData = tran.content[0]["wordData"]
    #         sentences = wordData.get("wikipedia_digest", {}).get("summarys", [])
    #         for sentence in sentences:
    #             sent = sentence["summary"]
    #             createSplitWordJob(sent)
    #
    #         i += 1
    #         if i % 100 == 0: print(i)