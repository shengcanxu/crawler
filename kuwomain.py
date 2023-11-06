from mongoengine import connect
from kuwo.kuwoCrawler import crawl_kuwo_job
from utils.httpProxy import startProxy, ProxyMode

if __name__ == "__main__":
    connect(host="192.168.0.116", port=27017, db="kuwo", alias="kuwo", username="canoxu", password="4401821211", authentication_source='admin')

    DOWNLOAD_BASE_PATH = "D:/songfiles/"
    # DOWNLOAD_BASE_PATH = "/home/cano/songfiles/"

    startProxy(mode=ProxyMode.PROXY_POOL)
    crawl_kuwo_job(thread_num=20)