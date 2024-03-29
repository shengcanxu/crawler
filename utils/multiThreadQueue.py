# 用于多线程执行爬虫程序
# python多线程教程：https://www.cnblogs.com/yuanwt93/p/15886333.html
import traceback
from threading import Thread, Lock, Event
import time
from queue import Queue

class MultiThreadQueueWorker:
    def __init__(self, threadNum = 1, minQueueSize = 500, crawlFunc = None, createJobFunc = None):
        '''
        :param threadNum: 线程个数
        :param minQueueSize: 队列少于多少个item就调用createJobFunc来填充
        :param crawlFunc: 用于实际爬取的function, return crawl status
        :param createJobFunc: 用于往Queue中加入item的function. 返回False or None就意味着没有可以爬取的item了
        '''
        self.itemQueue = Queue()
        self.threadList = []
        self.threadNum = threadNum
        self.minQueueSize = minQueueSize
        self.crawlFunc = crawlFunc
        self.createJobFunc = createJobFunc

    def startAllThreads(self):
        threadList = []
        for i in range(self.threadNum):
            thread = Thread(target=self.worker, args=[i])
            self.threadList.append(thread)
            thread.start()
            threadList.append(thread)
            time.sleep(1)

    def start(self):
        # 使用一个单独的thread来逐步启动所有线程
        startThread = Thread(target=self.startAllThreads)
        startThread.start()

        if self.crawlFunc is None or self.createJobFunc is None:
            print("please provide the crawlFunc and createJobFunc")
            return

        while True:
            if self.itemQueue.qsize() >= self.minQueueSize:
                time.sleep(1)
                continue

            preLen = self.itemQueue.qsize()
            self.createJobFunc(self.itemQueue)
            addedItemLen = self.itemQueue.qsize() - preLen
            print(f"add {addedItemLen} items")
            if addedItemLen <= 0:
                break

    def worker(self, thread_id:int):
        errorCount = 0
        sleepSeconds = 0
        while True:
            try:
                item = self.itemQueue.get()
                if item is None:
                    time.sleep(1)
                    sleepSeconds += 1
                    if sleepSeconds >= 60:
                        break
                    else:
                        print(f"thread {thread_id} sleeps {sleepSeconds} seconds")
                        continue
                sleepSeconds = 0

                succ = self.crawlFunc(thread_id, item)
                if succ is False:
                    errorCount += 1
                    if errorCount >= 5:
                        time.sleep(10)
                        errorCount = 0
                    else:
                        time.sleep(1)

            except Exception as ex:
                print(ex)
                print(f"<multiThreadQueue.py>error on thread {thread_id}")
                errorCount += 1
                if errorCount >= 5:
                    time.sleep(10)
                    errorCount = 0
                else:
                    time.sleep(1)

def tryworker():
    def createJobWorker(itemList:list):
        print("main thread create job")
        itemList.append("abc")

    def crawlWorker(threadId:int, item):
        print(f"working on {threadId} with {item}")

    worker = MultiThreadQueueWorker(threadNum=2, minQueueSize=10, crawlFunc=crawlWorker, createJobFunc=createJobWorker)
    worker.start()

if __name__ == "__main__":
    tryworker()