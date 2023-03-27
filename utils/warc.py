import io

import warcio
from warcio import WARCWriter, ArchiveIterator, StatusAndHeaders
import os
from urllib.parse import urlparse

# save and read WARC file, only save html and images in the html
class WarcFile:
    def __init__(self, htmlUrl:str):
        assert htmlUrl is not None and len(htmlUrl) > 0
        self.htmlUrl = htmlUrl
        self.filePath = self._mapUrlToFilepath(htmlUrl, "D:/WARC")
        folderPath = os.path.dirname(self.filePath)
        if not os.path.exists(folderPath):
            os.makedirs(folderPath)
        self._writer = None
        self._reader = None

    def _mapUrlToFilepath(self, url:str, baseFilepath:str):
        parsedUrl = urlparse(url)
        path = parsedUrl.path + ".warc.gz"
        filePath = os.path.abspath(os.path.join(baseFilepath, path.lstrip('/')))
        return filePath

    @property
    def writer(self):
        if self._writer is None:
            fh = open(self.filePath, "wb")
            self._writer = WARCWriter(fh, gzip=True)
        return self._writer

    @property
    def reader(self):
        if self._reader is None:
            fh = open(self.filePath, "rb")
            self._reader = ArchiveIterator(fh)
        return self._reader

    def writeHtml(self, response):
        httpHeaders = StatusAndHeaders('200 OK', response.headers.items(), protocol='HTTP/1.0')
        record = self.writer.create_warc_record(
            uri=response.url,
            record_type="response",
            warc_content_type="text/html",
            http_headers=httpHeaders,
            payload=io.BytesIO(response.content)
        )
        self.writer.write_record(record)

    def writeImage(self, response):
        imageBytes = io.BytesIO(response.content)
        httpHeaders = StatusAndHeaders('200 OK', response.headers.items(), protocol='HTTP/1.0')
        record = self.writer.create_warc_record(
            uri=response.url,
            record_type="resource",
            warc_content_type="image/jpeg",
            http_headers=httpHeaders,
            payload= imageBytes
        )
        self.writer.write_record(record)

    def names(self):
        resources = []
        for record in self.reader:
            if record.rec_type == "response":
                resources.append("html from %s" % record.rec_headers.get_header("WARC-Target-URI"))
            elif record.rec_type == "resource":
                resources.append("image from %s" % record.rec_headers.get_header("WARC-Target-URI"))
        return resources
