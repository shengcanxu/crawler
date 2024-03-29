import re

def bvid_to_avid_offline(bvid):
    _table = 'fZodR9XQDSUm21yCkr6zBqiveYah8bt4xsWpHnJE7jL5VG3guMTKNPAwcF'
    _s = [11,10,3,8,4,6]
    _tr = {}
    _xor = 177451812
    _add = 8728348608
    for _ in range(58):
        _tr[_table[_]] = _
        
    r = 0
    for i in range(6):
        r += _tr[bvid[_s[i]]]*58**i
    return (r-_add)^_xor

def avid_to_bvid_offline(avid):
    _table = 'fZodR9XQDSUm21yCkr6zBqiveYah8bt4xsWpHnJE7jL5VG3guMTKNPAwcF'
    _xor = 177451812
    _add = 8728348608
    _s = [11,10,3,8,4,6]
    
    avid = (avid^_xor)+_add
    r = list('BV1  4 1 7  ')
    for i in range(6):
        r[_s[i]] = _table[avid//58**i%58]
    return ''.join(r)

def convert_number(a):
    try:
        a = int(a)
    except:
        return '-'
    else:        
        y = a/(10000**2)
        if y >= 1:
            return f'{round(y,2)}亿'
        w = a/10000
        if w >= 1:
            return f'{round(w,2)}万'
        return str(a)

def second_to_time(sec):
    h = sec // 3600
    sec = sec % 3600
    m = sec // 60
    s = sec % 60
    return '%d:%02d:%02d'%(h,m,s)

def format_img(url,w=None,h=None,f='jpg'):
    '''For *.hdslb.com/bfs* only.'''
    if ('.hdslb.com/bfs' not in url) and ('archive.biliimg.com/bfs' not in url):
        raise RuntimeError('Not-supported URL Type:%s'%url)
    tmp = []
    if w:
        tmp += [str(w)+'w']
    if h:
        tmp += [str(h)+'h']
    tmp = '_'.join(tmp)
    if f and f in ['png','jpg','webp']:
        tmp += '.'+f
    if tmp:
        return url+'@'+tmp
    else:
        return url

def parse_url(url):
    # if 'b23.tv/' in url:#短链接重定向
    #     try:
    #         url = requester.get_redirect_url('https://b23.tv/'+re.findall(r'b23\.tv/([a-zA-Z0-9]+)',url,re.I)[0])
    #     except:
    #         pass
    #音频id
    res = re.findall(r'au([0-9]+)',url,re.I)
    if res:
        return int(res[0]),'auid'
    #bv号
    res = re.findall(r'BV[a-zA-Z0-9]{10}',url,re.I)
    if res:
        return res[0],'bvid'
    #av号
    res = re.findall(r'av([0-9]+)',url,re.I)
    if res:
        return int(res[0]),'avid'
    #专栏号
    res = re.findall(r'cv([0-9]+)',url,re.I)
    if res:
        return int(res[0]),'cvid'
    #整个剧集的id
    res = re.findall(r'md([0-9]+)',url,re.I)
    if res:
        return int(res[0]),'mdid'
    #整个季度的id
    res = re.findall(r'ss([0-9]+)',url,re.I)
    if res:
        return int(res[0]),'ssid'
    #单集的id
    res = re.findall(r'ep([0-9]+)',url,re.I)
    if res:
        return int(res[0]),'epid'
    #手动输入的uid
    res = re.findall(r'uid([0-9]+)',url,re.I)
    if res:
        return int(res[0]),'uid'
    #漫画id
    res = re.findall(r'mc([0-9]+)',url,re.I)
    if res:
        return int(res[0]),'mcid'
    #歌单
    res = re.findall(r'am([0-9]+)',url,re.I)
    if res:
        return int(res[0]),'amid'
    #用户空间相关
    uid = re.findall(r'space\.bilibili\.com\/([0-9]+)',url,re.I)
    if uid:
        uid = int(uid[0])
        #合集
        if 'collectiondetail' in url.lower():
            sid = re.findall(r'sid\=([0-9]+)',url,re.I)
            if sid:
                return (uid,int(sid[0])),'collection'
        #收藏夹
        elif 'favlist' in url.lower() and 'ftype=create' in url.lower():
            mlid = re.findall(r'fid\=([0-9]+)',url,re.I)
            if mlid:
                return (uid,int(mlid[0])),'favlist'
            
        #系列
        elif 'seriesdetail' in url.lower():
            sid = re.findall(r'sid\=([0-9]+)',url,re.I)
            if sid:
                return (uid,int(sid[0])),'series'
        return uid,'uid'
    return None,'unknown'