import sys
import operator


class Solution(object):
    data = list()
    def climbStairs(self, n):
        data = list(n)
        """
        :type n: int
        :rtype: int
        """
        if n == 1 or n == 2:
            return n
        result = self.climbStairs(n-1) + self.climbStairs(n-2)
        self.data.append(result)
        return self.data[n-1]


def func(amount, ping, gai, total):
    total += amount
    ping_extra = ping + amount
    gai_extra = gai + amount
    over_ping = ping_extra%5
    over_gai = gai_extra%3
    extra_amount = ping_extra/5 + gai_extra/3
    over_ping += extra_amount
    over_gai += extra_amount
    if over_ping < 5 and over_gai < 3:
        print total + extra_amount
    else:
        func(extra_amount, (over_ping - extra_amount), (over_gai-extra_amount), total)


def get_len_str(input_str):
    size = len(input_str)
    if size%2 == 1:
        print "the only value is one "
    left_half_str = input_str[0: size/2]
    right_half_str = input_str[size/2:size]
    temp = right_half_str[::-1]
    if left_half_str == temp:
        get_len_str(left_half_str)
    else:
        print len(input_str)

de


if __name__ == '__main__':
    # solution = Solution()
    # print solution.climbStairs(5)
    get_len_str('AAAAAAA')