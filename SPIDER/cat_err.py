
import requests
from lxml import etree
from xml.etree.ElementTree import Element


url = 'http://192.168.1.180:8080/cat/r/m/XXXX-XXXX-ac141a7c-423452-76846?domain=XXXX-XXXX'

rep = requests.get(url)
html_parser = etree.HTML(rep.content)

err_list = html_parser.xpath('//td[@class="error"]/..')

for element in err_list:
    for child in element.getchildren():
        print etree.tostring(child)

print err_list