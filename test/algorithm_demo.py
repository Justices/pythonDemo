from math import sqrt,pow, floor, ceil


def fetch_max_int(input_list, output_list):
    value = max(input_list)
    output_list.append(value)
    input_list.remove(value)
    if input_list is None or  len(input_list) == 0 :
        print "".join(output_list)
    else:
        fetch_max_int(input_list, output_list)


def get_liv_fang(num):
    sqrt_value = sqrt(num)
    fourth_value = sqrt(sqrt_value)
    for x in xrange(int(floor(fourth_value)), int(ceil(sqrt_value))):
        if pow(x, 3) == num:
            print "the num %d is li fangshu"%num
            return
    print "the num %d is not li fangshu" % num


if __name__ == '__main__':
    input_list = ['21', '3','9', '32']
    output_list = []
    fetch_max_int(input_list, output_list)
    get_liv_fang(19683)