import glob
import json
import os

import librosa
from mongoengine import connect

from kuwoCrawler import KuwoSong, KuwoSongList

def exctract_info(base_path:str):
    """从mongoDB里将歌曲相关的内容输出到json文件中"""
    songlist_map = {}
    # get the songlist map for mapping songlist
    map_path = base_path + "songlistmap.txt"
    if os.path.exists(map_path):
        with open(map_path, "r", encoding="utf-8") as f:
            songlist_map = json.load(f)
    else:
        songlists = KuwoSongList.objects({}).exclude("songs", "crawledpages")
        for songlist in songlists:
            exinfo = songlist.exinfo
            songlist_map[songlist.identify] = {
                "identify": songlist.identify,
                "uname": exinfo["uname"] if "uname" in exinfo else "",
                "userName": exinfo["userName"] if "userName" in exinfo else "",
                "songcount": exinfo["total"] if "total" in exinfo else 0,
                "name": exinfo["name"] if "name" in exinfo else "",
                "tag": exinfo["tag"].split(",") if "tag" in exinfo else [],
                "info": exinfo["info"] if "info" in exinfo else "",
                "listencnt": exinfo["listencnt"] if "listencnt" in exinfo else 0,
                "avglisten": round(exinfo["listencnt"] / exinfo["total"]) if "total" in exinfo and exinfo["total"] > 0 else 0,
            }
        with open(map_path, "w", encoding="utf-8") as f:
            json.dump(songlist_map, f, ensure_ascii=False, indent=4)
    print("songlist count:", len(songlist_map))

    songs_ids = KuwoSong.objects(filepath__exists=True).only("identify")
    for song_id in songs_ids:
        try:
            song = KuwoSong.objects(identify=song_id.identify).first()
            filepath = song.filepath.replace(".mp3", ".json")
            if os.path.exists(filepath):
                print(f"{filepath} exists, skip!")
                continue

            if song is not None:
                songlists = [songlist_map[id] for id in song.songlists if id in songlist_map]
                songlists.sort(key=lambda x: x["avglisten"], reverse=True)
                if len(songlists) > 50:
                    songlists = songlists[:50]

                info = song.info
                json_obj = {
                    "identify": song.identify,
                    "name": info["name"] if "name" in info else "",
                    "artist": info["artist"] if "artist" in info else "",
                    "artistid": info["artistid"] if "artistid" in info else "",
                    "isvip": song.isvip,
                    "duration": info["duration"] if "duration" in info else 0,
                    "album": info["album"] if "album" in info else "",
                    "url": song.url,
                    "lyric": song.lyric,
                    "score100": info["score100"] if "score100" in info else 0,
                    "songlistcount": song.songlistcount,
                    "avg_play_count": song.avg_play_count,
                    "max_play_count": song.max_play_count,
                    "top_songlists": songlists
                }
                json_str = json.dumps(json_obj, indent=4, ensure_ascii=False)

                with open(filepath, "w", encoding="utf-8") as fp:
                    fp.write(json_str)
                    print(f"write {filepath}")

        except Exception as ex:
            with open(base_path + "error_file_infos.txt", "w") as errorfp:
                errorfp.write(filepath + "\n")

def check_songfiles(base_path:str):
    """检查已经下载的mp3文件，如果文件大小小于10s，则删除"""
    files = glob.glob(base_path + "**/*.mp3", recursive=True)
    with open(base_path+"error_files.txt", "w") as fp:
        for filename in files:
            try:
                duration = librosa.get_duration(filename=filename)
                if duration <= 10:
                    print(filename)
                    fp.write(filename + "\n")
            except Exception as ex:
                print(filename)
                fp.write(filename + "\n")

def remove_songs_info(base_path:str):
    """删除发生错误的歌曲"""
    with open(base_path + "error_files.txt", "r") as fp:
        files = fp.readlines()
    for filepath in files:
        filepath = filepath.strip()
        if os.path.exists(filepath):
            os.remove(filepath)

        song = KuwoSong.objects(filepath=filepath).first()
        if song is not None:
            del song.filepath
            song.save()
            print(f"delete {filepath}")
        else:
            print(f" ---- {filepath} not found ----- ")


if __name__ == "__main__":
    connect(host="192.168.0.101", port=27017, db="kuwo", alias="kuwo", username="canoxu", password="4401821211", authentication_source='admin')

    path = "/data/dataset/songfiles/"

    # 检查所有的歌曲文件，将出错的和小于10S的歌曲都删除
    # check_songfiles(base_path = path)
    # remove_songs_info(base_path= path)

    # 从mongoDB里将歌曲相关的内容输出到json文件中
    # exctract_info(base_path=path)

    # songs_ids = KuwoSong.objects(filepath__exists=True).only("identify")
    # for song_id in songs_ids:
    #     try:
    #         song = KuwoSong.objects(identify=song_id.identify).first()
    #         if song is not None:
    #             print(song_id.identify)
    #             filepath = song.filepath.replace("/home/cano/songfiles/", "/data/dataset/songfiles/")
    #             song.filepath = filepath
    #             song.save()
    #     except Exception as ex:
    #         pass
