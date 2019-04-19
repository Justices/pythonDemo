# encoding:utf-8
import urllib, urllib2, sys
import ssl

host = 'https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id=YD5HeqdQ0Y2yQy3XPu3mvRVA&client_secret=9f9BhOr8QLURa6LDHvcjQhYnjb0A6y4q '
request = urllib2.Request(host)
request.add_header('Content-Type', 'application/json; charset=UTF-8')
response = urllib2.urlopen(request)
content = response.read()
if (content):
    print(content)