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
from requests_html import HTMLSession

from utils.Job import createJob, finishJob, failJob
from utils.httpProxy import getHTMLSession, startProxy, ProxyMode
from utils.logger import FileLogger
from utils.multiThreadQueue import MultiThreadQueueWorker
from kuwo.js_call import js_context, run_inline_javascript

BASEURL = "https://hub.vroid.com"
HEADERS = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "sec-ch-ua": "\"Chromium\";v=\"92\", \" Not A;Brand\";v=\"99\", \"Google Chrome\";v=\"92\"",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        ### Future Versions Might Need This Header To Be Updated To Match Updates To Webiste/
        "X-Api-Version": "11"
}
COOKIES = {
    "_vroid_session":"6a19880bc31be900f6bfacb983463db7"
}

class VroidModel(Document):
    modelid = StringField(required=True)
    characterid = StringField(required=True)
    list_url = StringField(required=True)
    data = DictField(required=False)
    downloaded = BooleanField(default=False)
    filename = StringField(required=False)
    filesize = IntField(required=False)
    download_path = StringField(required=False)
    meta = {
        "strict": True,
        "collection": "models",
        "db_alias": "vroid"
    }


def crawl_vroid_model_list(url: str) -> str | None:
    session = HTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES)
        if response is None: return None

        jsonobj = json.loads(response.text)
        data = jsonobj.get("data", [])
        FileLogger.info(f"get {len(data)} models.")

        for model in data:
            model_id = model.get("id", None)
            character_id = model.get("character", {}).get("id", None)
            if model_id is not None and character_id is not None:
                model_obj = VroidModel.objects(modelid=model_id).first()
                if model_obj is None:
                    model_obj = VroidModel(modelid=model_id)

                model_obj.characterid = character_id
                model_obj.list_url = url
                model_obj.data = model
                model_obj.save()

        next_url = jsonobj.get("_links", {}).get("next", {}).get("href", None)
        if next_url is not None:
            next_url = BASEURL + next_url
        return next_url

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling: {url} !")

        return None


if __name__ == "__main__":
    connect(host="localhost", port=27017, db="vroid", alias="vroid", username="canoxu", password="4401821211", authentication_source='admin')

    # start_url = 'https://hub.vroid.com/api/character_models'
    start_url = 'https://hub.vroid.com/api/character_models?max_id=4341104017321329091'
    next_url = crawl_vroid_model_list(start_url)
    while next_url is not None:
        FileLogger.info(f"next url: {next_url}")
        next_url = crawl_vroid_model_list(next_url)

