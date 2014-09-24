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