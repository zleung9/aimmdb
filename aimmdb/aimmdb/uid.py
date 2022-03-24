import math
import sys
import os

# adapted from shortuuid
# see https://github.com/skorokithakis/shortuuid

# the collision probability for n entries using base b keys of length m can be approximated by n^2/(2 * b^m)
# for 8 byte keys and 10^6 entries (n = 10^6, b = 256, m = 8) this implies a collision probability of ~ 2.6 * 10^-8 which is acceptably low
# we encode as base57 using an alphabet designed for maximum legibility

# base 57
_alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_alphabet_len = len(_alphabet)
_uid_bytes = 8
_uid_length = int(math.ceil(math.log(256**_uid_bytes, _alphabet_len)))


def int_to_string(number, padding=_uid_length):
    output = ""
    while number:
        number, digit = divmod(number, _alphabet_len)
        output += _alphabet[digit]

    if padding:
        remainder = max(padding - len(output), 0)
        output = output + _alphabet[0] * remainder
    return output[::-1]


def string_to_int(string):
    number = 0
    for char in string:
        number = number * _alphabet_len + _alphabet.index(char)
    return number


def uid():
    x = int.from_bytes(os.urandom(_uid_bytes), sys.byteorder)
    return int_to_string(x)
