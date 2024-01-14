import requests
import os
from internetarchive import get_item, download

def download_text_data(textID, outDir):

    item = get_item(textID)
    namesFile = []
    for data in item.files:
        name = data['name']
        if os.path.splitext(name)[1] == ".txt":
            namesFile.append(name)

    if len(namesFile) == 0:
        return False, []

    return download(textID, files=namesFile, destdir=outDir), namesFile


def get_archive_id(textURL):

    indexStart = textURL.find("archive.org/details/") \
        + len("archive.org/details/")
    if indexStart < 0:
        return False

    indexEnd = textURL[indexStart:].find("/")
    if indexEnd < 0:
        return textURL[indexStart:]
    return textURL[indexStart:(indexStart + indexEnd)]


def get_archive_org_text_data(url):

    ID = get_archive_id(url)
    tmpDir = "tmp"
    status, fileNames = download_text_data(ID, tmpDir)

    if len(fileNames) == 0:
        raise RuntimeError("Invalid URL")

    fullText = ""
    for fileName in fileNames:
        fullPath = os.path.join(tmpDir, os.path.join(ID, fileName))
        with open(fullPath, 'r', encoding="ISO-8859-1") as file:
            data = file.read()

        os.remove(fullPath)
        fullText += data.replace('\\n', '\n') + '\n'

    return fullText


def is_archive_org_url(url):
    if url.find("https://archive.org/stream/") == 0 \
            or url.find("http://archive.org/stream/") == 0:
        url = url.replace("archive.org/stream/", "archive.org/details/")
    return url.find("archive.org/details/") >= 0

def is_guttenberg_url(url):
    return url.find('http://www.gutenberg.org') == 0 or \
        url.find('https://www.gutenberg.org') == 0 or \
        url.find("http://gutenberg.org") == 0


def get_guttenberg_data(url):
    txtID = url.split('/')[-1]
    targetURL = f'http://www.gutenberg.org/cache/epub/{txtID}/pg{txtID}.txt'
    return requests.get(targetURL)._content.decode("utf-8")


def get_text_data(url):
    if is_guttenberg_url(url):
        return get_guttenberg_data(url)
    elif is_archive_org_url(url):
        return get_archive_org_text_data(url)
    elif is_bartheleby_url(url):
        return get_bartheleby_data(url)
    elif is_main_lesson_url(url):
        return get_all_text_from_main_lesson(url)
    elif is_hathitrust_url(url):
        return load_hathitrust_book(url)
    else:
        raise RuntimeError(f'Unknown web API {url}')