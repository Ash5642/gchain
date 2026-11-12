from pybloom_live import BloomFilter
from functools import reduce
import secrets, string, os, random
# Initialize with an expected maximum capacity and an acceptable error rate
# error_rate = 0.001 translates to a 0.1% chance of a false positive

# Adding elements
raw_arr = []

for pos in range(0, 10000):
    raw_arr.append(random.randbytes(32))

min = 1
max = 0
bf = BloomFilter(capacity=2000, error_rate=0.001)
base_size = len(bf.bitarray.to01())
occ = 1
#printbase_size)
for pos in range(0, 100):
    filter_size=random.randint(base_size, base_size*2)
    bf = BloomFilter(capacity=filter_size, error_rate=0.001)
    mask = random.randbytes(32)
    for value in raw_arr:
        bf.add(reduce(lambda x, y: bytes(b1 ^ b2 for b1, b2 in zip(x, y)), [value, mask]))

    filter = bf.bitarray.to01()
    zeroes = 0
    for char in filter:
        if char == '0':
            zeroes = zeroes+1
    occ = float(zeroes)/float(len(filter))
    if max < occ:
        max = occ
    if min > occ:
        min = occ


#printf"Max: {max}")
#printf"Min: {min}")