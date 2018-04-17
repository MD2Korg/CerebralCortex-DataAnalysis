# Copyright (c) 2018, MD2K Center of Excellence
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

"""
Documentation of GPS features.

:Features:
1. Cumulative Staying Time at a given place [1].
2. Transition Number [1].
3. The total distance covered [2]
4. The maximum distance between two location [2]
5. The standard deviation of the displacement [2]
6. The maximum distance from home [2]
7. The number of different places visited [2]
8. The radius of gyration [2]
9. The routine Index [2]
10. Available data in time

:References:
1. Yu Huang , Haoyi Xiong , Kevin Leach , Yuyan Zhang , Philip Chow , Karl Fua , Bethany A. Teachman, Laura E. Barnes,
Assessing social anxiety using gps trajectories and point-of-interest data, Proceedings of the 2016 ACM International
Joint Conference on Pervasive and Ubiquitous Computing, September 12-16, 2016, Heidelberg, Germany
2. Canzian L, Musolesi M (2015) Trajectories of depression: unobtrusive monitoring of depressive states by means of
smartphone mobility traces analysis. In: Proceedings of the 2015 ACM international joint conference on pervasive and
ubiquitous computing (UbiComp’15). ACM, New York, pp 1293-1304

"""
