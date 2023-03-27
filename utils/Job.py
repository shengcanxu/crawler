from mongoengine import Document, StringField, BooleanField, DateTimeField, IntField, ListField
import datetime

# 用于保存统计的全局变量
JOB_CREATED = 0
JOB_CREATED_LAST = 0
JOB_FINISHED = 0
JOB_FINISHED_LAST = 0
JOB_LAST_TIME = datetime.datetime.now()

# 输出新创建和完成的job数量
def printJobStatistic():
    global JOB_CREATED, JOB_FINISHED, JOB_LAST_TIME, JOB_CREATED_LAST, JOB_FINISHED_LAST
    current = datetime.datetime.now()
    if (current - JOB_LAST_TIME).seconds > 60:
        print("-------job statistic-----------")
        print(f"{JOB_CREATED-JOB_CREATED_LAST} jobs are created, {JOB_FINISHED-JOB_FINISHED_LAST} jobs are finished")
        print(f"total {JOB_CREATED} jobs are created, total {JOB_FINISHED} jobs are finished")
        print("-------------------------------")
        JOB_LAST_TIME = current
        JOB_CREATED_LAST = JOB_CREATED
        JOB_FINISHED_LAST = JOB_FINISHED

def createJob(JobType, category: str, name: str, param: list = None):
    global JOB_CREATED
    printJobStatistic()

    job = JobType.objects(category=category, name=name).first()
    if job is not None:
        if job.tryDate is not None:
            job.tryDate = None
            job.save()
        return job
    else:
        job = JobType(category=category, name=name, param=param)
        job.createDate = datetime.datetime.now()
        job.save()
        JOB_CREATED += 1
        return job

def createOrUpdateJob(JobType, category: str, name: str, param: list = None):
    global JOB_CREATED
    printJobStatistic()

    job = JobType.objects(category=category, name=name).first()
    if job is None:
        job = JobType(category=category, name=name, param=param)
        job.createDate = datetime.datetime.now()
        job.save()
        JOB_CREATED += 1
    else:
        job.category = category
        job.name = name
        job.param = param
        job.finished = False
        job.try_date = None
        job.save()
        JOB_CREATED += 1
    return job

def finishJob(job):
    global JOB_FINISHED
    printJobStatistic()

    if job:
        job.finished = True
        job.save()
        JOB_FINISHED += 1
        return job
    else:
        print("job is None")
        return None

def failJob(job):
    if job:
        job.tryDate = datetime.datetime.now()
        job.save()
        return job
    else:
        print("job is None")
        return None


def deleteJob(JobType, category: str, name: str):
    if name == "all":
        JobType.objects(category=category).delete()
    else:
        JobType.objects(category=category, name=name).delete()

# refresh job识别的是 lastUpdateDate(最后更新的时间)和updateDateSpan(刷新请求需要间隔的天数，越不频繁的任务需要间隔越长)
def createRefreshJob(JobType, category:str, name:str, param:list = None):
    global JOB_CREATED
    printJobStatistic()

    job = JobType.objects(category=category, name=name).first()
    if job is not None:
        lastUpdateDate = job.lastUpdateDate
        daySpan = job.daySpan
        if daySpan is not None and (datetime.datetime.now() - lastUpdateDate).days < daySpan:
            return job # 没有到更新的时候，什么都不改
        else: #更新
            job.lastUpdateDate = datetime.datetime.now()
            job.daySpan = 1
            job.finished = False
            job.save()
            return job
    else:
        job = JobType(category = category, name=name, param=param)
        job.createDate = datetime.datetime.now()
        job.finished = False
        job.lastUpdateDate = datetime.datetime.now()
        job.daySpan = 1
        job.save()
        JOB_CREATED += 1
        return job

def updateRefreshJobDaySpan(job, daySpan=7):
    if job.lastUpdateDate is None or job.daySpan is None:
        return False
    else:
        job.daySpan = daySpan
        return True
