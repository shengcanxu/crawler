import glob
import librosa


def check_songfiles(base_path:str):
    files = glob.glob(base_path + "**/*.mp3", recursive=True)
    for filename in files:
        duration = librosa.get_duration(filename=filename)
        if duration <= 10:
            print(filename)

if __name__ == "__main__":
    check_songfiles(base_path = "/data/dataset/songfiles/")