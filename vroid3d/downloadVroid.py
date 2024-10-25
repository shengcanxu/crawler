import os
from typing import Tuple
from queue import Queue
from mongoengine import connect
from requests_html import HTMLSession
from tqdm import tqdm
from utils.logger import FileLogger
from utils.multiThreadQueue import MultiThreadQueueWorker
from vroid3d.vroidCrawler import VroidModel

model_href_format = "/en/characters/{}/models/{}"
model_preview_format = "/api/character_models/{}/optimized_preview"
model_download_format = "/api/character_models/{}/versions/{}/download"

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


download_path = "/home/cano/dataset/vroid/"
def download_vrm_file(modelid:str, characterid: str) -> Tuple[bool, str, int]:
    url = f"https://hub.vroid.com/api/character_models/{modelid}/optimized_preview"
    session = HTMLSession()
    try:
        response = session.get(url, headers=HEADERS, cookies=COOKIES, stream=True)
        if response is None: return False, "", 0

        real_url = response.url
        filename = real_url.split("/")[-1]
        folder = filename.split(".")[0][-2:]
        filepath = os.path.join(download_path, folder)
        if not os.path.exists(filepath):
            os.mkdir(filepath)
        filepath = os.path.join(filepath, filename)

        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        with open(filepath, 'wb') as file:
            for data in response.iter_content(block_size):
                file.write(data)

        return True, filepath, total_size

    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"error on crawling: {url} !")
        return False, "", 0

def download_vrm_files(thread_num:int=1):
    def create_job_worker(item_queue: Queue):
        models = VroidModel.objects(downloaded=False, data__heart_count__gt=30).limit(500)
        for model in models:
            modelid = model.modelid
            characterid = model.characterid
            item_queue.put({
                "model": model,
                "modelid": modelid,
                "characterid": characterid
            })

    def crawl_worker(thread_id: int, item: object):
        model = item["model"]
        modelid = model["modelid"]
        characterid = model["characterid"]

        FileLogger.info(f"[{thread_id}]start downloading {modelid}")
        succ, filepath, total_size = download_vrm_file(modelid, characterid)
        if succ:
            model.downloaded = True
            model.download_path = filepath
            model.filename = filepath.split("/")[-1]
            model.filesize = total_size

            model.save()
            FileLogger.info(f"[{thread_id}]downloaded {modelid}")
            return True
        else:
            FileLogger.error(f"[{thread_id}]failed to download {modelid}")
            return False

    worker = MultiThreadQueueWorker(threadNum=thread_num, minQueueSize=400, crawlFunc=crawl_worker, createJobFunc=create_job_worker)
    worker.start()

if __name__ == "__main__":
    connect(host="localhost", port=27017, db="vroid", alias="vroid", username="canoxu", password="4401821211", authentication_source='admin')

    download_vrm_files(thread_num=10)