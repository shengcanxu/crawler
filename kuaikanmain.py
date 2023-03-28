from mongoengine import connect
from kuaikan.commic import refreshCrawl

if __name__ == "__main__":
    connect(db="kuaikan", alias="kuaikan", username="canoxu", password="4401821211", authentication_source='admin')
    connect(db="stock", alias="stock", username="canoxu", password="4401821211", authentication_source='admin')

    refreshCrawl()
