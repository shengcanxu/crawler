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

from kuwo.kuwoCrawler import KuwoSong, BASIC_HEADERS, BASIC_COOKIES, KuwoJob
from utils.Job import createJob, finishJob, failJob
from utils.httpProxy import getHTMLSession, startProxy, ProxyMode
from utils.logger import FileLogger
from utils.multiThreadQueue import MultiThreadQueueWorker
from kuwo.js_call import js_context, run_inline_javascript

DOWNLOAD_BASE_PATH = "/data/dataset/songfiles/"

# 下载mp3， 只能下载免费的
def download_song(url:str, song_id:str, artist:str, song_name:str):
    if DOWNLOAD_BASE_PATH is None:
        FileLogger.error("please set the DOWNLOAD_BASE_PATH !")
        return False
    song = KuwoSong.objects(identify=song_id).first()
    if song is None: return True

    session = HTMLSession()  # direct download, don't use any proxy
    try:
        response = session.get(url, headers=BASIC_HEADERS, cookies=BASIC_COOKIES)
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

def download_worker():
    while True:
        time.sleep(5)
        for job in KuwoJob.objects(category="ez_download", finished=False).limit(50):
            url = job.name
            category = job.category
            param = job.param
            identify = param[0]
            artist = param[1]
            song_name = param[2]

            succ = download_song(url, identify, artist, song_name)
            if succ:
                finishJob(job)
                FileLogger.warning(f"success on {url} of {category}")
            else:
                failJob(job)
                FileLogger.error(f"fail on {url} of {category}")
            time.sleep(0)

# 下载酷我的歌曲mp3

if __name__ == "__main__":
    # connect(db="kuwo", alias="kuwo", username="canoxu", password="4401821211", authentication_source='admin')
    connect(host="192.168.0.101", port=27017, db="kuwo", alias="kuwo", username="canoxu", password="4401821211", authentication_source='admin')

    # DOWNLOAD_BASE_PATH = "D:/test/"
    DOWNLOAD_BASE_PATH = "/data/dataset/songfiles/"

    download_worker()
