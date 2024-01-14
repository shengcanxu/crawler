from mongoengine import connect

from librovox.crawlLibrivox import crawlLibrivox, LibrivoxBook, LibrivoxJob
from utils.Job import createJob

Global_Save_Path = ""
if __name__ == "__main__":
    connect(host="192.168.0.101", port=27017, db="librivox", alias="librivox", username="canoxu", password="4401821211", authentication_source='admin')

    # Global_Save_Path = "/home/cano/dataset/librivox/"
    Global_Save_Path = "D:/dataset/librivox/"
    crawlLibrivox()

    # bookids = LibrivoxBook.objects().distinct("bookid")
    # for id in bookids:
    #     print(id)
    #     param = [id]
    #     libribook = LibrivoxBook.objects(bookid=id).first()
    #     createJob(LibrivoxJob, "bookinfo", libribook.url_librivox, param=param)
    #     createJob(LibrivoxJob, "booktext", libribook.url_text_source, param=param)