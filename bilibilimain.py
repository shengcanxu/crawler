from mongoengine import connect
from bilibili.bilibiliCrawler import crawl_bilibili_job


Global_Save_Path = ""
if __name__ == "__main__":
    connect(host="192.168.0.101", port=27017, db="bilibili", alias="bilibili", username="canoxu", password="4401821211", authentication_source='admin')

    crawl_bilibili_job(thread_num=1)