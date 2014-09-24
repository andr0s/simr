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


import sys
import itertools
import os
import tempfile
import operator
import gzip
import datetime


try:
    import ujson as json
    print 'using ujson'
except ImportError:
    try:
        import simplejson as json
        print 'using simplejson'
    except ImportError:
        import json
        print 'using json'


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
class SiMR(object):

    #--------------------------------------------------------------------------
    def __init__(self,
                 input_file, output_file,
                 tmp_dir='/tmp', cleanup=True,
                 separator='|_|-^|[*]|^-|_|',  # feel free to change it to tabs
            ):
        self.input_file = input_file
        self.output_file = output_file
        self.cleanup = cleanup
        self.separator = separator
        self.tmp_dir = tmp_dir
        self._started_utc = datetime.datetime.utcnow()
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        # check if we can use parallel version of sort
        self._can_use_parallel_sort()

    #--------------------------------------------------------------------------
    def _can_use_parallel_sort(self):
        _tmp_sort_file = os.path.join(self.tmp_dir, 'tmp_sort_test.txt')
        with open(_tmp_sort_file, 'w') as fh:
            fh.write('1\n2\n3')
        sort_result = os.system(
            'sort --parallel=4 %s > /dev/null 2>&1' % _tmp_sort_file)
        if sort_result == 0:
            self.use_parallel_sort = True
        else:
            self.use_parallel_sort = False

    #--------------------------------------------------------------------------
    def loader(self, handle_or_path):
        """ Turns the given file handle or path into iterator """
        if isinstance(handle_or_path, basestring):
            if handle_or_path.lower().endswith('.gz'):
                handle_or_path = gzip.GzipFile(handle_or_path, mode='r')
            else:
                handle_or_path = open(handle_or_path, 'r')
        for line in handle_or_path:
            if not line.strip():  # empty line
                continue
            if line.startswith('#'):  # comment
                continue
            if self.separator in line and line.strip()[0] != '{':  # 'mapped' file
                try:
                    key, value = line.split(self.separator)
                except Exception, e:
                    print str(e)
                    print line
                yield key, json.loads(value.strip())
            else:
                yield json.loads(line)

    #--------------------------------------------------------------------------
    def mapper(self, stream):
        """ Accepts iterator ('stream' in our terms). """
        assert False, 'you should override this method'

    #--------------------------------------------------------------------------
    def _sort(self, stream):
        """ Sorts the given key-value stream """
        tmp_file = tempfile.NamedTemporaryFile(mode='w', dir=self.tmp_dir,
                                               delete=False)
        for key, value in stream:
            if not isinstance(value, basestring):
                # forgot to convert to json?
                value = json.dumps(value)
            tmp_file.write('%s%s%s\n' % (key, self.separator, value))
        tmp_file.close()
        tmp_file2 = tempfile.NamedTemporaryFile(mode='w', dir=self.tmp_dir,
                                                delete=False)
        tmp_file2.close()
        self.tmp_file_sort = tmp_file2.name
        _extra_parallel_arg = '' if not self.use_parallel_sort else '--parallel=4'
        os.system(
            'sort  %s  --buffer-size=20%% --temporary-directory="%s" "%s" > "%s"' % (
                _extra_parallel_arg,
                self.tmp_dir, tmp_file.name, tmp_file2.name
            )
        )
        if self.cleanup:
            os.unlink(tmp_file.name)
        return self.loader(tmp_file2.name)

    #--------------------------------------------------------------------------
    def filter(self, stream):
        """ Implement your own iterator filter here, if needed """
        return stream

    #--------------------------------------------------------------------------
    def combiner(self, stream):
        for group in itertools.groupby(stream, operator.itemgetter(0)):
            key, value = group
            yield key, value

    #--------------------------------------------------------------------------
    def reducer(self, stream):
        if self.output_file.lower().endswith('.gz'):
            fh = gzip.GzipFile(self.output_file, 'w')
        else:
            fh = open(self.output_file, 'w')
        for i, s in enumerate(stream):
            if i % 10000 == 0:
                print '  reducer:', i
                sys.stdout.flush()
            key, value = s
            fh.write('\n')
            fh.write('#' * 79 + '\n')
            fh.write(key + '\n')
            for v in value:
                if len(v) == 2:
                    if v[0] == key:
                        v = v[1]
                        fh.write(' ' * 4 + json.dumps(v) + '\n')

    #--------------------------------------------------------------------------
    def run(self):
        # get file iterator ('stream')
        stream = self.loader(self.input_file)
        # execute 'map'
        stream = self.mapper(stream)
        # sort results
        stream = self._sort(stream)
        # filter
        stream = self.filter(stream)
        # combine
        stream = self.combiner(stream)
        # reduce result
        stream = self.reducer(stream)
        # cleanup
        if self.cleanup:
            trash1 = getattr(self, 'tmp_file_sort', None)
            if trash1 and isinstance(trash1, basestring):
                os.unlink(trash1)
        print 'completed in %s seconds' % (datetime.datetime.utcnow()
                                           - self._started_utc).total_seconds()