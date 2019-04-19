from urllib2 import HTTPCookieProcessor,build_opener
from cookielib import CookieJar,MozillaCookieJar

from redis_test import Redis


# 1. build a cookie with file name
# 2. create a cookie handler
# 3. build a opener
fileName = 'cookie.txt'
cookie = MozillaCookieJar(fileName)
handler = HTTPCookieProcessor(cookie)
opener = build_opener(handler)


response = opener.open("http://www.baidu.com")
for item in cookie:
    print 'Name = ' + item.name
    print 'Value = ' + item.value

cookie.save(ignore_discard=True,ignore_expires=True)