SiMR
====

A Simple Map-Reduce framework written in Python.

Why? Because there are no simple MR frameworks that allow processing real-world Big Data. All the existing MR frameworks are either:

1) just only Hadoop streaming wrappers,

2) education-purposes simple frameworks not capable for processing terabytes of data.

What's wrong with Hadoop? Almost nothing - except that fact that you have to setup your Hadoop cluster and debug its very unclear logs. Also, on some relatively small sets of data, Hadoop is not really fast - remember, it must distribute the source files across the cluster and execute its hungry Java code.

What's wrong with small education MR frameworks? They can olny process small sets of data. They often don't use streaming data formats and require all the input\output to be kept in RAM; some of them even try to sort the intermediate results in the memory!

SiMR is an attempt to build a bridge between the space ship (Hadoop) and the toy paper planes (other simple MR frameworks).

It's used in production and appears to be quite fast and stable. However, it does not support more than one machine so you won't be able to build a cluster. For such purposes you better choose Hadoop, Disco or any other decent mature project.

Another advantage is that you can write your MR jobs using a streaming pattern. It's really cool; it combines all the power of the traditional MR algorithm with the speed and convenience of streaming frameworks such as Cascading.

Just an example:

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
    
'stream' is just a Python iterator! You can combine it, filter it, dump\load to\from file and do just whatever you want!

License? MPL 2.0.

Future plans? Lots of them! Feel free to fork the code and send push requests back =)