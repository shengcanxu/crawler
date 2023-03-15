import datetime
import time
import random
from threading import Thread
from requests_html import HTMLSession
from enum import Enum
from utils.logger import FileLogger

class ProxyMode(Enum):
    NO_PROXY = 1
    SINGLE_PROXY = 2
    PROXY_POOL = 3

HTTP_PROXY_MAP = {}
HTTP_PROXY_MODE = ProxyMode.NO_PROXY

def fetchProxy():
    url = "https://api.xiaoxiangdaili.com/ip/get?appKey=953181634197606400&appSecret=ZWFW5ieW&cnt=&wt=text"
    session = HTMLSession()
    response = session.get(url)
    if response.status_code == 200:
        return response.text
    else:
        return None


def startFetchProxy():
    while True:
        if len(HTTP_PROXY_MAP) >= 100:
            time.sleep(1)
            continue

        proxy = fetchProxy()
        if proxy is not None:
            HTTP_PROXY_MAP[proxy] = {
                "timestamp": int(time.time()),
                "fails": 0
            }
            FileLogger.info(f"get a new proxy: {proxy}, there are {len(HTTP_PROXY_MAP)} proxies.")
            time.sleep(12)

# 设置代理。 如果有代理会启动额外线程从网上的线程获取接口获得并维护线程
def startProxy(mode:ProxyMode):
    global HTTP_PROXY_MODE
    HTTP_PROXY_MODE = mode

    if mode == ProxyMode.SINGLE_PROXY or mode == ProxyMode.PROXY_POOL:
        thread = Thread(target=startFetchProxy)
        thread.start()

class HTMLSessionWrapper():
    def __init__(self, session:HTMLSession, proxy:str):
        self.session = session
        self.proxy = proxy

    def markRequestFails(self):
        if self.proxy is None: return
        HTTP_PROXY_MAP[self.proxy]["fails"] += 1
        if HTTP_PROXY_MAP[self.proxy]["fails"] >= 5:
            del HTTP_PROXY_MAP[self.proxy]

    def get(self, url, headers=None, cookies=None):
        response = self.session.get(url, headers=headers, cookies=cookies)
        if response.status_code != 200:
            self.markRequestFails()
        return response

    def post(self, url, headers=None, cookies=None):
        response = self.session.post(url, headers=headers, cookies=cookies)
        if response.status_code != 200:
            self.markRequestFails()
        return response

def getProxyString():
    global HTTP_PROXY_MAP, HTTP_PROXY_MODE
    while True:
        if HTTP_PROXY_MODE == ProxyMode.SINGLE_PROXY:
            proxy, _ = HTTP_PROXY_MAP.popitem()
        else:
            proxies = list(HTTP_PROXY_MAP)
            index = random.randrange(0, len(proxies))
            proxy = proxies[index]

        meta = HTTP_PROXY_MAP[proxy]
        if meta["fails"] >= 5:
            del HTTP_PROXY_MAP[proxy]
        elif time.time() - meta["timestamp"] >= 120: # 大于两分钟IP已经过期
            del HTTP_PROXY_MAP[proxy]
        else:
            return proxy

# 用这个来替代原来的HTMLSession。 这个funciton可以实现无代理，单个代理或者代理池的方法
def getHTMLSession():
    global HTTP_PROXY_MODE
    if HTTP_PROXY_MODE == ProxyMode.SINGLE_PROXY or HTTP_PROXY_MODE == ProxyMode.PROXY_POOL:
        proxy = getProxyString()
        session = HTMLSession(browser_args=["--proxy-server=%s" % proxy])
        return HTMLSessionWrapper(session, proxy)
    else:
        return HTMLSessionWrapper(HTMLSession(), None)
