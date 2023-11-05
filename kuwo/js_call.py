# 解决execjs执行报： ‘gbk‘ codec can‘t decode byte 0xac  https://blog.csdn.net/qq_19309473/article/details/124152048
import subprocess
from functools import partial
subprocess.Popen = partial(subprocess.Popen, encoding="utf-8")
import execjs


js_code = """
function encode_secret(t, e) {
    if (null == e || e.length <= 0)
        return null;
    for (var n = "", i = 0; i < e.length; i++)
        n += e.charCodeAt(i).toString();
    var r = Math.floor(n.length / 5)
      , o = parseInt(n.charAt(r) + n.charAt(2 * r) + n.charAt(3 * r) + n.charAt(4 * r) + n.charAt(5 * r))
      , l = Math.ceil(e.length / 2)
      , c = Math.pow(2, 31) - 1;
    if (o < 2)
        return null;
    var d = Math.round(1e9 * Math.random()) % 1e8;
    for (n += d; n.length > 10; )
        n = (parseInt(n.substring(0, 10)) + parseInt(n.substring(10, n.length))).toString();
    n = (o * n + l) % c;
    var h = ""
      , f = "";
    for (i = 0; i < t.length; i++)
        f += (h = parseInt(t.charCodeAt(i) ^ Math.floor(n / c * 255))) < 16 ? "0" + h.toString(16) : h.toString(16),
        n = (o * n + l) % c;
    for (d = d.toString(16); d.length < 8; )
        d = "0" + d;
    return f += d
}

function encode_reqid(t, e, n) {  //just like uuid function. created uuid based on the current timestamp
    var r, o, d = 0, h = 0;   // this is changed to do initialization
    function l(){   // it's a random function
        arr = [];
        for(var i=0; i<16; i++){
            arr[i] = parseInt(Math.random() * 256)
        }
        return arr;
    }
    function c(t, e) {   //this is a function to change number to hexadecimal string (e.g. 17 -> "11")
        for (var n = [], i = 0; i < 256; ++i)
            n[i] = (i + 256).toString(16).substr(1);
        var i = e || 0
            , r = n;
        return [r[t[i++]], r[t[i++]], r[t[i++]], r[t[i++]], "-", r[t[i++]], r[t[i++]], "-", r[t[i++]], r[t[i++]], "-", r[t[i++]], r[t[i++]], "-", r[t[i++]], r[t[i++]], r[t[i++]], r[t[i++]], r[t[i++]], r[t[i++]]].join("")
    }
    
    var i = e && n || 0
      , b = e || []
      , f = (t = t || {}).node || r
      , v = void 0 !== t.clockseq ? t.clockseq : o;
    if (null == f || null == v) {
        var m = l();
        null == f && (f = r = [1 | m[0], m[1], m[2], m[3], m[4], m[5]]),
        null == v && (v = o = 16383 & (m[6] << 8 | m[7]))
    }
    var y = void 0 !== t.msecs ? t.msecs : (new Date).getTime()
      , w = void 0 !== t.nsecs ? t.nsecs : h + 1
      , dt = y - d + (w - h) / 1e4;
    if (dt < 0 && void 0 === t.clockseq && (v = v + 1 & 16383),
    (dt < 0 || y > d) && void 0 === t.nsecs && (w = 0),
    w >= 1e4)
        throw new Error("uuid.v1(): Can't create more than 10M uuids/sec");
    d = y,
    h = w,
    o = v;
    var x = (1e4 * (268435455 & (y += 122192928e5)) + w) % 4294967296;
    b[i++] = x >>> 24 & 255,
    b[i++] = x >>> 16 & 255,
    b[i++] = x >>> 8 & 255,
    b[i++] = 255 & x;
    var _ = y / 4294967296 * 1e4 & 268435455;
    b[i++] = _ >>> 8 & 255,
    b[i++] = 255 & _,
    b[i++] = _ >>> 24 & 15 | 16,
    b[i++] = _ >>> 16 & 255,
    b[i++] = v >>> 8 | 128,
    b[i++] = 255 & v;
    for (var A = 0; A < 6; ++A)
        b[i + A] = f[A];
    return e || c(b)
}
"""

js_context = execjs.compile(js_code)


def run_inline_javascript(script_text:str):
    js_code = "function runinline(){%s; return dataobj;}" % script_text
    js_inline_ctx = execjs.compile(js_code)
    dataobj = js_inline_ctx.call("runinline")
    return dataobj