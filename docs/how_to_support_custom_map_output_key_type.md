Currently, native task framework supports IntWritable, LongWritable, Text, BytesWritable, FloatWritable, BooleanWritable, DoubleWritable, HiveKey. 
We also need to support other customized key types. This tutorial will walk you through the steps to support a new key type.

Here is the steps: 
1. Implement Java serializer, and implement interface INativeComparable.
2. Implement native comparator in native space.
3. Register the java serializer into the system.
4. Register the native comparator in the system.
5. Done

Implement Java serializer
=============
``TODO``

Implement Native Comparator
=============
``TODO``

Register the java serializer
=============
``TODO``

Register the native comparator
=============
``TODO``
