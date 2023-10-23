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
PROXY_RETRYS = 10
PROXY_LAST_SECONDS = 120

# 使用小象代理来作为代理池
def fetchProxy():
    url = "https://api.xiaoxiangdaili.com/ip/get?appKey=1033807947605889024&appSecret=WXcSbuGp&cnt=&wt=text"
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
            time.sleep(11)

# 设置代理。 如果有代理会启动额外线程从网上的线程获取接口获得并维护线程
def startProxy(mode:ProxyMode):
    global HTTP_PROXY_MODE
    HTTP_PROXY_MODE = mode

    if mode == ProxyMode.SINGLE_PROXY or mode == ProxyMode.PROXY_POOL:
        thread = Thread(target=startFetchProxy)
        thread.start()

class HTMLSessionWrapper():
    def __init__(self,  proxy:str):
        self.session = HTMLSession()
        self.proxy = proxy

    def markRequestFails(self):
        if self.proxy is None or self.proxy not in HTTP_PROXY_MAP: return
        HTTP_PROXY_MAP[self.proxy]["fails"] += 1
        if HTTP_PROXY_MAP[self.proxy]["fails"] >= PROXY_RETRYS:
            del HTTP_PROXY_MAP[self.proxy]

    def markRequestSuccess(self):
        if self.proxy is None or self.proxy not in HTTP_PROXY_MAP: return
        HTTP_PROXY_MAP[self.proxy]["fails"] = max(0, HTTP_PROXY_MAP[self.proxy]["fails"] - 1)

    def get(self, url, headers=None, cookies=None):
        if self.proxy:
            proxies = {"http": "http://"+self.proxy, "https": "http://"+self.proxy}
            self.session.proxies = proxies
        response = self.session.get(url, headers=headers, cookies=cookies, timeout=10)
        if response.status_code != 200:
            self.markRequestFails()
            return None
        return response

    def post(self, url, headers=None, cookies=None):
        if self.proxy:
            proxies = {"http": "http://" + self.proxy, "https": "http://" + self.proxy}
            self.session.proxies = proxies
        response = self.session.post(url, headers=headers, cookies=cookies, timeout=10)
        if response.status_code != 200:
            self.markRequestFails()
            return None
        return response

def getProxyString():
    global HTTP_PROXY_MAP, HTTP_PROXY_MODE
    while True:
        if len(HTTP_PROXY_MAP) == 0:
            time.sleep(1)
            continue

        proxies = list(HTTP_PROXY_MAP)
        if HTTP_PROXY_MODE == ProxyMode.SINGLE_PROXY:
            proxy, _ = proxies[-1]
        else:
            proxy = random.choice(proxies)

        meta = HTTP_PROXY_MAP[proxy]
        if meta["fails"] >= PROXY_RETRYS:
            del HTTP_PROXY_MAP[proxy]
        elif time.time() - meta["timestamp"] >= PROXY_LAST_SECONDS: # 大于两分钟IP已经过期
            del HTTP_PROXY_MAP[proxy]
        else:
            return proxy

# 用这个来替代原来的HTMLSession。 这个funciton可以实现无代理，单个代理或者代理池的方法
def getHTMLSession():
    global HTTP_PROXY_MODE
    if HTTP_PROXY_MODE == ProxyMode.SINGLE_PROXY or HTTP_PROXY_MODE == ProxyMode.PROXY_POOL:
        proxy = getProxyString()
        return HTMLSessionWrapper(proxy)
    else:
        return HTMLSessionWrapper(None)
