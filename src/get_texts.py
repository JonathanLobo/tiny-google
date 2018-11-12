import requests
import os

image_url = "https://www.gutenberg.org/files/?/?-0.txt"

if not os.path.exists("../texts"):
    os.makedirs("../texts")

for i in range(1, 1001):
    r = requests.get(image_url.replace("?", str(i)))
    file = "./texts/text" + str(i) + ".txt"

    with open(file,'wb') as f:
        f.write(r.content)

    if os.path.getsize(file) < 2 * 1024:
      os.remove(file)
