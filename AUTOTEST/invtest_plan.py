#!/usr/bin/python
#encoding=utf-8
from concurrent.futures import ThreadPoolExecutor
from urllib2 import Request, urlopen
from datetime import datetime
from functools import partial
import json
from Queue import Queue
from decimal import Decimal

ex = ThreadPoolExecutor(max_workers=2)

request_urls = {"create_order": 'http://trd.stb.com/trd-XXXX/invest/purchase/createTransOrder',
                'pay_order': 'http://trd.stb.com/trd-XXXX/invest/purchase/payment'}

order_queue = Queue()

consume_queue = Queue()


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return "%.2f" % obj
        return json.JSONEncoder.default(self, obj)


def create_trans_order(fp_id, trans_amount, product_type):
    parameter_dict = dict()
    parameter_dict['userId'] = '12017878'
    parameter_dict['transAmount'] = trans_amount
    parameter_dict['transShare'] = trans_amount / 100
    parameter_dict['productId'] = str(fp_id)
    parameter_dict['productType'] = product_type
    parameter_dict["productName"] = "定活产品"
    parameter_dict['productYield'] = "10"
    parameter_dict['otherYield'] = 0
    parameter_dict['systemId'] = 'NONOBANK'
    parameter_dict['channelId'] = 'Mobile'
    parameter_dict['version'] = 'iso-9.3.4'
    return json.dumps(parameter_dict, cls=DecimalEncoder)


def create_request_template(request_type, parameter, is_producer=True):
    url = request_urls[request_type]
    req = Request(url=url)
    req.add_header('Content-type', 'application/json')
    rep = urlopen(req, parameter)
    if is_producer:
        order_queue.put(rep)
    else:
        consume_queue.put(rep)


def payment_order(trans_id):
    payment_dict = dict()
    payment_dict['transId'] = trans_id
    payment_dict['userId'] = '12017878'
    payment_dict['forgetPwdUrl'] = ''
    payment_dict['sucUrl'] = ''
    payment_dict['returnUrl'] = ''
    payment_dict['requestTimestamp'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H-%M-%S')
    return json.dumps(payment_dict)


if __name__ == '__main__':
    parameter_list = [create_trans_order(8770, Decimal(x *6 * 1000.00).quantize(Decimal('0.00')), "26") for x in
                      xrange(1, 6)]
    trans_partial = partial(create_request_template, 'create_order')
    ex.map(trans_partial, parameter_list)
    while order_queue.not_empty:
        rep = order_queue.get().read()
        param_dict = json.loads(rep)
        trans_id = param_dict['data']['transId']
        create_request_template(request_type='pay_order', parameter=payment_order(trans_id), is_producer=False)
        print consume_queue.get().read()

