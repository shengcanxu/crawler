import hashlib
import io
import os
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED

BASE_PATH = "D:/NewsBackup"

class PageBackup:
    def __init__(self, dateObj:datetime):
        datePath = str(dateObj.year) + "/" + str(dateObj.month) + "/"
        folderPath = os.path.abspath(os.path.join(BASE_PATH, datePath))
        if not os.path.exists(folderPath):
            os.makedirs(folderPath)
        self.zip = ZipFile(folderPath + "/" + str(dateObj.day) + ".zip", mode="a", compression=ZIP_DEFLATED, compresslevel=5)

    def writeHtml(self, pageName:str, content:str):
        if not self._existsName(pageName):
            self.zip.writestr(pageName, data=content)

    def writeImage(self, imageUrl:str, content:bytes):
        hashObj = hashlib.md5(imageUrl.encode())
        imageName = hashObj.hexdigest() + ".jpg"
        if not self._existsName(imageName):
            self.zip.writestr(imageName, data=content)

    def _existsName(self, name):
        namelist = self.zip.namelist()
        return True if name in namelist else False
