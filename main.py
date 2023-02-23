from mongoengine import connect
from xiaoshuo.crawlXiaoShuo import refreshXiaoshuo

if __name__ == "__main__":
    connect(db="xiaoshuo", alias="xiaoshuo", username="canoxu", password="4401821211", authentication_source='admin')
    refreshXiaoshuo()