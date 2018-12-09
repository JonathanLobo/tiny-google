from collections import defaultdict
import nltk
import os

nltk.download('punkt')
dir = "../inputs/"
docs = []

print(dir)

for doc in os.listdir(dir):
    file_content = open(dir + doc).read()
    tokens = nltk.word_tokenize(file_content)
    docs.append(tokens)

inv_index = defaultdict(lambda: defaultdict(int))
for idx, text in enumerate(docs):
    for word in text:
        inv_index[word][idx] = inv_index[word][idx] + 1
