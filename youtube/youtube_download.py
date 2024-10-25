import sys
sys.path.append("D:/projectfork/youtube-dl")
import youtube_dl

### project from https://github.com/ytdl-org/youtube-dl ###

if __name__ == '__main__':
    # argv = ["--proxy", "https://127.0.0.1:10809", "--verbose", "-o", "D:/download", "https://www.youtube.com/watch?v=XOn0UVp-LgE"]
    argv = ["--proxy", "https://127.0.0.1:10809", "--verbose", "-o", "D:/download", "https://www.youtube.com/watch?v=xYK3U0HA36E&list=PLopTMJMIFlGGA272dbP8Xg3Tr0HSVYcFU"]

    youtube_dl.main(argv)
