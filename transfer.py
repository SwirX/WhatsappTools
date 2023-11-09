from os import system, listdir
from os.path import isfile, abspath, join
from sys import argv
import time
import datetime

try:
    starttimestamp = time.time()
    _type = argv[1]
    if _type == "music":
        musicDir = r"C:\Users\Bouya\Music"
        files = listdir(musicDir)
        for file in files:
            if isfile(join(musicDir, file)):
                cmd = f"adb push \"{join(musicDir, file)}\" sdcard/Music"
                print(f"pushing {join(musicDir, file)} to sdcard/Music")
                system(cmd)
    endtimestamp = time.time()
    timespent = endtimestamp - starttimestamp
    print(f"transfer ended in {timespent}s")
    print("installing a musicplayer")
    system(r'adb install "C:\Users\Bouya\Documents\me\Phone\Apks\RetroMusicPlayer.apk"')

except Exception:
    musicDir = r"C:\Users\Bouya\Music"
    files = listdir(musicDir)
    for file in files:
        if isfile(file):
            cmd = f"adb push {file} sdcard/Music"
            resp = system(cmd)
            print(resp)