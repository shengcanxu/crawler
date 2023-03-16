from mongoengine import connect, Document, StringField, IntField, ListField
from douban.crawlDoubanBook import crawlDoubanBook


if __name__ == "__main__":
    connect(db="douban", alias="douban", username="canoxu", password="4401821211", authentication_source='admin')

    crawlDoubanBook()
