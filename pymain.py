# from warcio.warcwriter import WARCWriter
# from warcio.statusandheaders import StatusAndHeaders
#
# import requests
#
# with open('d:/example.warc', 'wb') as output:
#     writer = WARCWriter(output, gzip=False)
#
#     resp = requests.get('https://stock.stockstar.com/RB2023032200018418.shtml',
#                         # headers={'Accept-Encoding': 'identity'},
#                         stream=True)
#
#     # get raw headers from urllib3
#     headers_list = resp.raw.headers.items()
#
#     http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')
#
#     record = writer.create_warc_record('https://xueqiu.com/1778682397/244680715', 'response',
#                                         payload=resp.raw,
#                                         http_headers=http_headers)
#
#     writer.write_record(record)
#
#     resp = requests.get('https://static.stockstar.com/cmsrobo/zjlx_top50_5_1679469067.jpg', # headers={'Accept-Encoding': 'identity'},
#         stream=True)
#
#     # get raw headers from urllib3
#     headers_list = resp.raw.headers.items()
#
#     http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')
#
#     record2 = writer.create_warc_record('zjlx_top50_5_1679469067.jpg', 'response', payload=resp.raw, http_headers=http_headers)
#
#     writer.write_record(record2)


import requests
from warcio.archiveiterator import ArchiveIterator

def print_records(path):
    with open(path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                print(record.raw_stream.read())

            elif record.rec_type == 'response':
                if record.http_headers.get_header('Content-Type') == 'text/html':
                    print(record.http_headers.get_header('WARC-Target-URI'))
                    print(record.content_stream().read())
                    print('')

# WARC
print_records('d:/example.warc')
