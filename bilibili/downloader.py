import copy
from utils.logger import FileLogger
from urllib import parse, request
import os

BASIC_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',  # noqa
    'Accept-Charset': 'UTF-8,*;q=0.5',
    'Accept-Encoding': 'gzip,deflate,sdch',
    'Accept-Language': 'en-US,en;q=0.8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.74 Safari/537.36 Edg/79.0.309.43',  # noqa
    'Referer':'https://www.bilibili.com/'
}
BASIC_COOKIES = {
}

opener = request.build_opener()
def download_media(url:str, file:str, headers=BASIC_HEADERS, cookies=BASIC_COOKIES, check=True):
    if os.path.exists(file): return True

    global opener
    req = request.Request(url, headers=headers)
    response = opener.open(req, None, timeout=15)

    chunk_size = 1024
    total_size = float("inf")
    if response.status == 200:
        total_size = int(response.getheader('content-length'))
        with open(file,'wb') as f:
            while True:
                data = response.read(chunk_size)
                if data:
                    f.write(data)
                else:
                    break
    else:
        FileLogger.error(f'Downloaded error: {url}')
        return False
    
    if check:
        if os.path.getsize(file) != total_size:
            error_size = os.path.getsize(file)
            os.remove(file)
            FileLogger.error(f"file size error, got {error_size} but expect {total_size}")
            return False
    return True


def download_media_continue(url:str, file:str, headers=BASIC_HEADERS, cookies=BASIC_COOKIES, check=True):
    if os.path.exists(file): return True

    tmpfile = file+'.download'
    #检查上次下载遗留文件
    if os.path.exists(tmpfile):
        size = os.path.getsize(tmpfile)
    else:
        size = 0
    #拷贝请求头
    headers = copy.deepcopy(headers)
    #预请求
    #核对文件信息
    with opener.open(request.Request(url,headers=headers),timeout=15) as pre_response:
        total_size = int(pre_response.getheader('content-length'))
        if pre_response.getheader('accept-ranges') == 'bytes' and size <= total_size and size > 0:
            #满足这些条件时才会断点续传, 否则直接覆盖download文件
            #条件: 支持range操作, 本地文件大小小于服务端文件大小
            #生成range头
            headers['Range'] = 'bytes={}-{}'.format(size,total_size)
            write_mode = 'ab+'
            done_size = size
        else:
            done_size = size = 0
            write_mode = 'wb+'
    pre_response.close()

    chunk_size = 1*1024
    if size < total_size:
        try:
            with opener.open(request.Request(url,headers=headers),timeout=15) as fp_web: #网络文件
                FileLogger.info(f'Fetching data from {url}, start_byte={size}, Code {fp_web.getcode()}')
                with open(tmpfile,write_mode) as fp_local: #本地文件
                    while True:
                        data = fp_web.read(chunk_size)
                        if not data:
                            break
                        fp_local.write(data)
                        done_size += len(data)
        except Exception as e:
            return False

    if check and os.path.getsize(tmpfile) != total_size:
        error_size = os.path.getsize(tmpfile)
        os.remove(tmpfile)
        FileLogger.error(f"file size error, got {error_size} but expect {total_size}")
        return False
    
    os.rename(tmpfile, file)
    return True
