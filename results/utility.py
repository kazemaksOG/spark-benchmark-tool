from globals import *

import numpy as np
import math
from copy import copy



class Bin:
    def __init__(self, start, end, max=1, id=0, e=0.005):
        self.e = e
        self.start = start 
        self.end = end 
        self.max = max
        self.id = id
        self.pos = -1
        self.subbins = []

    def add(self, bin_elem):
        self.subbins.append(bin_elem)


    def pack_subbins(self):
        self.subbins = [bin for bin in self.subbins if bin.end - bin.start > self.e]
        ends = copy(self.subbins)
        n = len(self.subbins)
        self.subbins.sort(key=lambda x: x.start)
        ends.sort(key=lambda x: x.end)

        i = 0
        j = 0

        taken_pos = set()
        while i < n :

            if self.subbins[i].start >= ends[j].end and ends[j].pos in taken_pos:
                taken_pos.remove(ends[j].pos)
                j += 1
            else:

                pos = find_closest_to_0(taken_pos)
                self.subbins[i].pos = pos
                taken_pos.add(pos)
                self.max = self.max if self.max > len(taken_pos) else len(taken_pos)
                i += 1
    def overlap(self, start, end, on_id=False):

        # print(f"############ TIME {start}")
        id_set = set()
        count = 0
        for bin in self.subbins:

            if (
                (bin.start < start and bin.end > end) 
                or (bin.start > start and bin.end < end)
                or (bin.start > start and bin.start < end)
                or (bin.end > start and bin.end < end)):

                if on_id :
                    if bin.id not in id_set:
                        id_set.add(bin.id)
                        count += 1
                else:
                    count += 1
        return count







# names usually are bench_NAME_CONFIG_EXTRA
def get_human_name(filename):
    name = ""
    config = ""

    index = len("bench_")

    for sch in SCHEDULERS:
        if filename[index:].startswith(sch):
            name = sch 
            break

    # move index forawrd
    index += len(name) + len("_")

    for conf in CONFIGS:
        if filename[index:].startswith(conf):
            config = conf
            break

    return name, config

def get_job_type(name):
    for job_type in JOB_TYPES:
        if job_type in name:
            return job_type
    return "unclassified job"

def get_average(arr):
    return np.mean(arr)


def get_worst_10_percent(arr):
    arr = np.array(arr)
    n = max(1, int(len(arr) * 0.1))
    top_10_percent = np.partition(arr, -n)[-n:]
    return np.mean(top_10_percent)

def get_worst_1_percent(arr):
    arr = np.array(arr)
    n = max(1, int(len(arr) * 0.01))
    top_1_percent = np.partition(arr, -n)[-n:]
    return np.mean(top_1_percent)

def find_closest_to_0(nums):
    candidate = 0 
    while candidate in nums:
        candidate+=1 
    return candidate 


def round_sig(x, sig=4):
    if x == 0:
        return 0  # Avoid log(0) error
    return round(x, sig - int(math.floor(math.log10(abs(x)))) - 1)




