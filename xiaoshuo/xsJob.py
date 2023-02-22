from mongoengine import Document, StringField, BooleanField, DateTimeField, IntField, ListField
import datetime

# 用于保存统计的全局变量
JOB_CREATED = 0
JOB_CREATED_LAST = 0
JOB_FINISHED = 0
JOB_FINISHED_LAST = 0
JOB_LAST_TIME = datetime.datetime.now()

class XSJob(Document):
    category = StringField(required=True)  # job分型
    name = StringField(required=True)  # job名称，一般是标识出不同的job，起job_id作用
    finished = BooleanField(requests=True, default=False)  # 是否已经完成
    createDate = DateTimeField(required=True)  # 创建时间
    tryDate = DateTimeField(required=False)  # 尝试运行时间
    param = ListField(require=False)  # 参数
    lastUpdateDate = DateTimeField(required=False)  # 最后一次更新时间，主要用于需要周期更新的任务
    daySpan = IntField(required=False)  # 每次更新的间隔，主要用于需要周期更新的任务
    meta = {
        "strict": True,
        "collection": "job",
        "db_alias": "xiaoshuo"
    }

# 输出新创建和完成的job数量
def printXSJobStatistic():
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

def createXSJob(category: str, name: str, param: list = None):
    global JOB_CREATED
    printXSJobStatistic()

    job = XSJob.objects(category=category, name=name).first()
    if job is not None:
        if job.tryDate is not None:
            job.tryDate = None
            job.save()
        return job
    else:
        job = XSJob(category=category, name=name, param=param)
        job.createDate = datetime.datetime.now()
        job.save()
        JOB_CREATED += 1
        return job

def createOrUpdateXSJob(category: str, name: str, param: list = None):
    global JOB_CREATED
    printXSJobStatistic()

    job = XSJob.objects(category=category, name=name).first()
    if job is None:
        job = XSJob(category=category, name=name, param=param)
        job.create_date = datetime.datetime.now()
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

def finishXSJob(job: XSJob):
    global JOB_FINISHED
    printXSJobStatistic()

    if job:
        job.finished = True
        job.save()
        JOB_FINISHED += 1
        return job
    else:
        print("job is None")
        return None

def failXSJob(job: XSJob):
    if job:
        job.tryDate = datetime.datetime.now()
        job.save()
        return job
    else:
        print("job is None")
        return None


def deleteXSJob(category: str, name: str):
    if name == "all":
        XSJob.objects(category=category).delete()
    else:
        XSJob.objects(category=category, name=name).delete()


