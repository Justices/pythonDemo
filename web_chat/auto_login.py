#encoding: utf-8
import itchat
import re
import jieba
import matplotlib.pyplot as plt
from wordcloud import WordCloud, ImageColorGenerator
from PIL import Image
import os
import numpy as np


itchat.auto_login(hotReload=True)
friends_list = itchat.get_friends()[0:]
tlist = []

for item in friends_list:
    signature = item['Signature'].strip().replace('span', '').replace('class', '').replace('emoji', "")
    rep = re.compile("lf\d.+")
    signature = rep.sub("", signature)
    tlist.append(signature)

text = "".join(tlist)
word_list = jieba.cut(text, cut_all=True)
wl_space_split = " ".join(word_list)

d = os.path.dirname(__file__)
alice_coloring = np.array(Image.open(os.path.join(d, "wechat.jpg")))
my_wordcloud = WordCloud(background_color="white", max_words=2000, mask=alice_coloring,
                         max_font_size=40, random_state=42,
                         font_path='/Users/sebastian/Library/Fonts/Arial Unicode.ttf')\
    .generate(wl_space_split)

## 普通的生成图片
#my_wordcloud = WordCloud(background_color="white", max_words=2000,
                         # max_font_size=40, random_state=42,
                         # font_path='/Users/sebastian/Library/Fonts/Arial Unicode.ttf').generate(wl_space_split)

image_colors = ImageColorGenerator(alice_coloring)
plt.imshow(my_wordcloud.recolor(color_func=image_colors))
plt.imshow(my_wordcloud)
plt.axis("off")
plt.show()

# 保存图片 并发送到手机
my_wordcloud.to_file(os.path.join(d, "wechat_cloud.png"))
itchat.send_image("wechat_cloud.png", 'filehelper')