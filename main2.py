from mongoengine import connect
from xueqiu.getzhuanlan import refreshZhuanlan

if __name__ == "__main__":
    connect(db="xueqiu", alias="xueqiu", username="canoxu", password="4401821211", authentication_source='admin')
    connect(db="stock", alias="stock", username="canoxu", password="4401821211", authentication_source='admin')

    #  重复爬取只需要将所有的jobType = EXPAND_STOCK 和 jobType = GET_LIST 改成finished=False
    refreshZhuanlan()