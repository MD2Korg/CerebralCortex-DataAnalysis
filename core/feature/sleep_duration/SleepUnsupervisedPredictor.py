# Copyright (c) 2018, MD2K Center of Excellence
#   Code collected from Dr. Akane Sano (https://bitbucket.org/akanes/pac-code.git)
#   Algorithm: http://ieeexplore.ieee.org/document/6563918/
#   Imported by Md Shiplu Hawlader <shiplu.cse.du@gmail.com; mhwlader@memphis.edu>
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import argparse
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint

import numpy
from datetime import datetime, timedelta, timezone, tzinfo


class SleepUnsupervisedPredictor():
    """
    Unsupervised sleep duration predictor from http://ieeexplore.ieee.org/document/6563918/
    """

    def find_longest_subvector(self, vector: object, val: object, thresh: object = 8, gap: object = 4) -> object:
        """

        :rtype: object
        :param vector:
        :param val:
        :param thresh:
        :param gap:
        :return:
        """
        cur_start_idx = -1

        candidates = []

        for i in range(vector.shape[0]):
            if vector[i] == val:
                if cur_start_idx == -1:
                    cur_start_idx = i

            else:
                if cur_start_idx == -1:
                    continue

                if i - cur_start_idx + 1 > thresh:
                    candidates.append([cur_start_idx, i])

                cur_start_idx = -1

        if cur_start_idx != -1:
            candidates.append([cur_start_idx, i - 1])

        max_item = [0, 0]
        current_item = None
        for item in candidates:
            if current_item is None:
                current_item = item
            elif item[0] - current_item[1] + 1 <= gap:
                current_item[1] = item[1]
            else:
                if current_item[1] - current_item[0] > max_item[1] - max_item[0]:
                    max_item = current_item

                current_item = item
        if current_item is not None and current_item[1] - current_item[0] > max_item[1] - max_item[0]:
            max_item = current_item

        return max_item[0], max_item[1]

    def rolling_window(self, a: object, size: object) -> object:
        """

        :rtype: object
        :param a:
        :param size:
        :return:
        """
        shape = a.shape[:-1] + (a.shape[-1] - size + 1, size)
        strides = a.strides + (a.strides[-1],)

        return numpy.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)

    def pyramid(self, input_vector: object, scale: object, threshold: object = 0.05) -> object:
        """

        :rtype: object
        :param input_vector:
        :param scale:
        :param threshold:
        :return:
        """
        input_matrix = numpy.reshape(input_vector, (-1, scale))

        input_mean = numpy.nanmean(input_matrix, axis=1)

        output = numpy.zeros(input_mean.shape)
        output[1:] = numpy.abs(numpy.diff(input_mean))

        output[numpy.isnan(output)] = 1000

        if (output.max() - output.min()) == 0:
            output = numpy.zeros(output.shape)
        else:
            output = (output - output.min()) / (output.max() - output.min())

        positive = output <= threshold
        output[positive] = 1
        output[~positive] = 0

        bool_indices = numpy.all(self.rolling_window(output, 2) == [0, 1], axis=1)
        bool_indices = numpy.concatenate((bool_indices, [False]))
        output[bool_indices] = 1

        return output

    def expand(self, input_vector: object, scale: object) -> object:
        """

        :rtype: object
        :param input_vector:
        :param scale:
        :return:
        """
        input_matrix = numpy.reshape(input_vector, (-1, 1))
        expand_matrix = numpy.tile(input_matrix, (1, int(scale)));

        return expand_matrix.flatten()

    def vote(self, vector: object, threshold: object = 0.05) -> object:
        """

        :rtype: object
        :param vector:
        :param threshold:
        :return:
        """
        vector_60m = self.pyramid(vector, 3600, threshold)
        vector_30m = self.pyramid(vector, 1800, threshold)
        vector_15m = self.pyramid(vector, 900, threshold)
        vector_7_5m = self.pyramid(vector, 450, threshold)

        vector_vote = self.expand(vector_60m, 60 / 7.5) * 0.3 + self.expand(vector_30m, 30 / 7.5) * 0.3 + self.expand(
            vector_15m, 15 / 7.5) * 0.2 + vector_7_5m * 0.1

        return vector_vote

    def predict(self, audio_vector: object, ligth_vector: object, act_vector: object, lock_vector: object) -> object:
        """

        :rtype: object
        :param audio_vector:
        :param ligth_vector:
        :param act_vector:
        :param lock_vector:
        :return:
        """
        act_vote = self.pyramid(act_vector, 900, threshold=0.03);
        lock_vote = self.pyramid(lock_vector, 900, threshold=0.1);

        audio_vote = self.vote(audio_vector, 0.05)
        light_vote = self.vote(ligth_vector, 0.008)

        sleep_vote = (light_vote + .5 * audio_vote)
        sleep_vote *= self.expand(act_vote, 15 / 7.5) * self.expand(lock_vote, 15 / 7.5)
        sleep_vote[sleep_vote < sleep_vote.max() * 0.6] = 0

        sleep_vote[sleep_vote > 0] = 1

        longest_start_idx, longest_end_idx = self.find_longest_subvector(sleep_vote, 1, 8, 1)

        return longest_start_idx, longest_end_idx, sleep_vote.shape
