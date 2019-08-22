#!/usr/bin/env python
# -*- coding: utf-8 -*-
##############################
# @Date    : 2019-08-19
# @File    : snowflake.py
# @Purpose : xxxxx
# @Args    : None
# @Author  : yunlong.gao@accenture.com
##############################

import time

class InputError(Exception):
    pass
class InvalidSystemClock(Exception):
    pass

class IdWorker(object):
    """docstring for IdWorker"""
    def __init__(self, worker_id=0, data_center_id=0):
        super(IdWorker, self).__init__()
        self._worker_id = worker_id
        self._data_center_id = data_center_id

        # stats
        self.ids_generated = 0

        #Wed Mar 21 00:00:00 2018
        self._twepoch = 1521561600000

        self._sequence = 0
        self._worker_id_bits = 5
        self._data_center_id_bits = 5
        self._sequence_bits = 12

        self.max_worker_id = -1 ^ (-1 << self._worker_id_bits)
        self.max_data_center_id = -1 ^ (-1 << self._data_center_id_bits)

        self._worker_id_shift = self._sequence_bits
        self._data_center_id_shift = self._sequence_bits + self._worker_id_bits
        self._timestamp_left_shift = self._sequence_bits + self._worker_id_bits + self._data_center_id_bits
        self._sequence_mask = -1 ^ (-1 << self._sequence_bits)

        self._last_timestamp = -1

        # Sanity check for worker_id
        if self._worker_id > self.max_worker_id or self._worker_id < 0:
            raise InputError("worker_id", "worker id can't be greater than %i or less than 0" % self.max_worker_id)
        if self._data_center_id > self.max_data_center_id or self._data_center_id < 0:
            raise InputError("data_center_id", "data center id can't be greater than %i or less than 0" % self.max_data_center_id)

    def _time_gen(self):
        return int(time.time() * 1000)

    def _till_next_millis(self, last_timestamp):
        timestamp = self._time_gen()
        while last_timestamp <= timestamp:
            timestamp = self._time_gen()

        return timestamp

    def _next_id(self):
        timestamp = self._time_gen()

        if self._last_timestamp > timestamp:
            raise InvalidSystemClock("Clock moved backwards. Refusing to generate id for %i milliseocnds" % self._last_timestamp)

        if self._last_timestamp == timestamp:
            self._sequence = (self._sequence + 1) & self._sequence_mask
            if self._sequence == 0:
                timestamp = self._till_next_millis(self._last_timestamp)
        else:
            self._sequence = 0

        self._last_timestamp = timestamp

        new_id = ((timestamp - self._twepoch) << self._timestamp_left_shift) | (self._data_center_id << self._data_center_id_shift) | (self._worker_id << self._worker_id_shift) | self._sequence
        self.ids_generated += 1
        return new_id

    def get_worker_id(self):
        return self._worker_id

    def get_timestamp(self):
        return self._time_gen()

    def get_id(self):
        new_id = self._next_id()
        return new_id

    def get_datacenter_id(self):
        return self._data_center_id

if __name__ == '__main__':
    my_id = IdWorker()
    for i in range(10):
        print("{} id: {}".format(i, my_id.get_id()))
    print("id count: {}".format(my_id.ids_generated))