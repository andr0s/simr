#
# License:
#   This Source Code Form is subject to the terms of the Mozilla Public
#   License, v. 2.0. If a copy of the MPL was not distributed with this file,
#   You can obtain one at http://mozilla.org/MPL/2.0/.

# Authors:
#   Copyright (c) 2014, Andrey Nozhkin <no.andrey [AT] gmail [D0T] com>

# Original repository:
#   https://github.com/andr0s/simr
#


import os
import sys
import unittest

CWD = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, os.path.join(CWD, '..'))

from SiMR import *


def _mapper1(self, stream):
    for line in stream:
        if 'banana' in line['words']:
            yield 'banana', line['numbers']


class TestSequenceFunctions(unittest.TestCase):
    def test_1(self):
        SiMR.mapper = _mapper1
        MR = SiMR(
            input_file=os.path.join(CWD, 'source.txt'),
            output_file='/tmp/t1.txt'
        )
        MR.run()
        c1 = open(os.path.join(CWD, 'test1.txt'), 'r').read()
        c2 = open('/tmp/t1.txt', 'r').read()
        self.assertEqual(c1, c2)


if __name__ == '__main__':
    unittest.main()