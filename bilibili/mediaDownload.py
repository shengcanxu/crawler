
import argparse
import os
import platform
from queue import Queue
import json
import time
from urllib import parse, request
from bilibili.biliapis import wbi
from bilibili.bilibiliCrawler import BilibiliJob, BilibiliUser
from bilibili.downloader import download_media, download_media_continue
from utils.Job import failJob, finishJob
from utils.httpProxy import getHTMLSession

from utils.logger import FileLogger
from utils.multiThreadQueue import MultiThreadQueueWorker
from mongoengine import connect, Document, StringField, DictField, BooleanField, DateTimeField, IntField, ListField, LongField


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
HEADERS = [BASIC_HEADERS]
COOKIES = [BASIC_COOKIES]

def get_video_stream_urls(cid,avid=None,bvid=None,dolby_vision=False,hdr=False,_4k=False,_8k=False) -> dict:
    '''get the urls of a video by cid and bvid/avid'''
    # 根据参数生成 fnval 与 fourk 的值
    fnval = 16
    fourk = 0
    if hdr:
        fnval = fnval|64
    if _4k:
        fourk = 1
        fnval = fnval|128
    #if dolby_audio:
    #    fnval = fnval|256
    if dolby_vision:
        fnval = fnval|512
    if _8k:
        fnval = fnval|1024
    
    params = {
        'cid': cid,
        'fnval': fnval,
        'fourk': fourk
    }
    api_wbi = 'https://api.bilibili.com/x/player/wbi/playurl'
    api_legacy = 'https://api.bilibili.com/pgc/player/web/v2/playurl'
    api_legacy_backup = 'https://api.bilibili.com/pgc/player/web/playurl'
    api_legacy_backup_2 = 'https://api.bilibili.com/x/player/playurl'
    if avid != None:
        params['avid'] = avid
    elif bvid != None:
        params['bvid'] = bvid
    else:
        raise AssertionError('avid and bvid, choose one plz.')
    
    succ_flag = 0
    # 尝试请求 wbi 接口
    response = getHTMLSession().get(
        api_wbi + '?' + parse.urlencode(wbi.sign(params=params)),
        headers=HEADERS[0],
        cookies=COOKIES[0]
    )
    data = json.loads(response.text)
    if data['code'] == 0:
        succ_flag = 1
        data = data['data']
    else:
        FileLogger.error('playurl-getting method wbi failure: '+data['message'])

    # 尝试请求原来的接口1
    if not succ_flag:
        response = getHTMLSession().get(
            api_legacy + '?' + parse.urlencode(params),
            headers=HEADERS[0],
            cookies=COOKIES[0]
        )
        data = json.loads(response.text)
        if data['code'] == 0:
            succ_flag = 1
            data = data['data']
        else:
            FileLogger.error('playurl-getting method legacy A failure: '+data['message'])

    # 尝试请求原来的接口2
    if not succ_flag:
        response = getHTMLSession().get(
            api_legacy_backup+'?'+parse.urlencode(params),
            headers=HEADERS[0],
            cookies=COOKIES[0]
        )
        data = json.loads(response.text)
        if data['code'] == 0:
            succ_flag = 1
            data = data['result']
        else:
            FileLogger.error('playurl-getting method legacy B failure: '+data['message'])

    # 尝试请求原来的接口3
    if not succ_flag:
        response = getHTMLSession().get(
            api_legacy_backup_2+'?'+parse.urlencode(params),
            headers=HEADERS[0],
            cookies=COOKIES[0]
        )
        data = json.loads(response.text)
        if data['code'] == 0:
            succ_flag = 1
            data = data['result']
        else:
            FileLogger.error('playurl-getting method legacy C failure: '+data['message'])

    assert succ_flag, 'Stream request failure' 

    # if 'dash' in data:
    #     return _video_stream_dash_handler(data)
    # elif 'durl' in data:
    #     return _video_stream_mp4_handler(data)
    # else:
    #     raise AssertionError('Unknown stream format.')
    
    return _video_stream_dash_handler(data)

def _video_stream_dash_handler(data: dict) -> dict:
    assert 'dash' in data,'''DASH data not found,
This is usually caused by accessing vip media without vip account logged in.
(But sometimes happens without reason)'''

    audio = []
    if data['dash'].get('audio'):
        for au in data['dash']['audio']:
            audio.append({
                'quality':au['id'],#对照表 .bilicodes.stream_dash_audio_quality
                'url':au['baseUrl'],
                'url_backup':au['backupUrl'],
                'codec':au['codecs'],
                })
    if 'flac' in data['dash']:
        flac = data['dash']['flac']
        if flac:
            if flac['audio']:
                audio.append({
                    'quality':flac['audio']['id'],
                    'url':flac['audio']['base_url'],
                    'url_backup':flac['audio']['backup_url'], #list
                    'codec':flac['audio']['codecs']
                    })
                
    video = []
    for vi in data['dash']['video']:
        video.append({
            'quality':vi['id'],#对照表 .bilicodes.stream_dash_video_quality
            'url':vi['base_url'],
            'url_backup':vi['backup_url'],
            'codec':vi['codecs'],
            'width':vi['width'],
            'height':vi['height'],
            'frame_rate':vi['frameRate'],#帧率
            })
        
    stream = {
        'method':'dash',
        'audio':audio,
        'video':video,
        'length':data['timelength']/1000 #sec
        }
    return stream

def crawl_download_media(thread_id:int, userid:str, bvid:str, folder:str, audio_only=True):
    user = BilibiliUser.objects(userid=userid).first()
    if user is None: return False

    target_video = None
    videolist = user.videolist
    for video in videolist:
        if video["bv_id"] == bvid:
            target_video = video
            break
    if target_video is None: return False
    cid = target_video["pages"][0]["id"]

    folder = os.path.join(folder, userid)
    if not os.path.exists(folder):
        os.mkdir(folder)

    try:
        stream_data = get_video_stream_urls(cid, bvid=bvid, hdr=True, _4k=True, dolby_vision=True, _8k=True)
        astreams = stream_data["audio"] 
        aqs = []
        for astream in astreams:
            aqs.append(astream['quality'])
        audiostream = astreams[aqs.index(max(aqs))]
        urls = [audiostream['url']] + audiostream['url_backup']
        
        succ = False
        for url in urls: 
            try:
                filename = os.path.join(folder, f"{userid}_{bvid}_a.m4s")
                # succ = download_media(url, filename)
                succ = download_media_continue(url, filename)
            except:
                continue
            if succ: break

        if succ and not audio_only:   # download video
            vstreams = stream_data["video"]
            vqs = []
            for vstream in vstreams:
                vqs.append(vstream['quality'])
            videostream = vstreams[vqs.index(max(vqs))]
            urls = [videostream['url']] + videostream['url_backup']
            
            succ = False
            for url in urls: 
                try:
                    filename = os.path.join(folder, f"{userid}_{bvid}_v.m4s")
                    # succ = download_media(url, filename)
                    succ = download_media_continue(url, filename)
                except:
                    continue
                if succ: break

        return succ
    
    except Exception as ex:
        FileLogger.error(ex)
        FileLogger.error(f"[{thread_id}]error on userid: {userid} bvid: {bvid} !")
        if os.path.exists(filename):
            os.remove(filename)
        return False

def crawl_bilibili_job(thread_num:int=1, save_folder="./"):
    def create_job_worker(item_queue:Queue):
        for job in BilibiliJob.objects(finished=False).order_by("-category").limit(500): 
        # for job in BilibiliJob.objects(category="cz_videolist", finished=False).limit(500):
            url = job.name
            category = job.category
            param = job.param
            item_queue.put({
                "job": job,
                "url": url,
                "category": category,
                "param": param
            })

    def crawl_worker(thread_id:int, item:object):
        job = item["job"]
        url = item["url"]
        category = item["category"]
        FileLogger.info(f"[{thread_id}] working on {url} of {category}")
        succ = False
        if category == "dz_downaudio":  # 下载音频
            userid = str(item["param"][0])
            bvid = str(item["param"][1])
            succ = crawl_download_media(thread_id, userid, bvid, save_folder, audio_only=False)
        elif category == "dz_downvideo": # 下载视频
            userid = str(item["param"][0])
            bvid = str(item["param"][1])
            succ = crawl_download_media(thread_id, userid, bvid, save_folder, audio_only=False)

        if succ:
            finishJob(job)
            FileLogger.warning(f"[{thread_id}] success on {url} of {category}")
        else:
            failJob(job)
            FileLogger.error(f"[{thread_id}] fail on {url} of {category}")
        time.sleep(1)
        return succ

    # copy headers and cookies to make sure each thread has its own header and cookies
    for i in range(thread_num):
        HEADERS.append(BASIC_HEADERS.copy())
        COOKIES.append(BASIC_COOKIES.copy())

    worker = MultiThreadQueueWorker(threadNum=thread_num, minQueueSize=400, crawlFunc=crawl_worker, createJobFunc=create_job_worker)
    worker.start()

# 爬取bilibili的用户信息
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    if platform.system() == "Windows":
        parser.add_argument("--save_folder", type=str, default="D:/dataset/bilibili/downloads")
    else:
        parser.add_argument("--save_folder", type=str, default="/home/cano/dataset/bilibili/downloads/")
    args = parser.parse_args()

    connect(host="192.168.0.101", port=27017, db="bilibili", alias="bilibili", username="canoxu", password="4401821211", authentication_source='admin')

    # startProxy(mode=ProxyMode.PROXY_POOL)
    crawl_bilibili_job(thread_num=1, save_folder=args.save_folder)

    # cid = "1283343200"
    # bvid = "BV1Pw411e7Zo"
    # astream = get_video_stream_urls(cid,bvid=bvid,hdr=True,_4k=True,dolby_vision=True,_8k=True)
    # print(astream)

    # url = "https://xy112x86x163x28xy.mcdn.bilivideo.cn:8082/v1/resource/1404148128-1-30280.m4s?agrr=1&build=0&buvid=&bvc=vod&bw=21220&deadline=1705315459&e=ig8euxZM2rNcNbdlhoNvNC8BqJIzNbfqXBvEqxTEto8BTrNvN0GvT90W5JZMkX_YN0MvXg8gNEV4NC8xNEV4N03eN0B5tZlqNxTEto8BTrNvNeZVuJ10Kj_g2UB02J0mN0B5tZlqNCNEto8BTrNvNC7MTX502C8f2jmMQJ6mqF2fka1mqx6gqj0eN0B599M%3D&f=u_0_0&gen=playurlv2&logo=A0000001&mcdnid=1003420&mid=0&nbs=1&nettype=0&oi=2028725789&orderid=0%2C3&os=mcdn&platform=pc&sign=6058a4&traceid=trirMvsGQOStNx_0_e_N&uipk=5&uparams=e%2Cuipk%2Cnbs%2Cdeadline%2Cgen%2Cos%2Coi%2Ctrid%2Cmid%2Cplatform&upsig=d740d118f3d749e21927fd0ee54aac2a"
    # succ = download_media(url, "test.m4a", "D:/download")
    # print(succ)