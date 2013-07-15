#**nativetask 测试报告**#

----------
## Abstract ##
This paper will introduce the compability, performance of nativetask using the test result.

## 1. Compatibility Test ##
In this model.we tested functions as bellow:
	
	1. How many map out key types are supported.
	2. Whether support any types of values types.
	3. How many compression algorithms are supported.
	4. How to support combiner.
	5. How many combination of java + native are supported. (for example, native mapper + java collector, native mapper + native collector)

**1.1 TestCase**
	
*1.1.1 KV Test*

	

	for function 1, we select key types :
		IntWritable,FloatWritable,LongWritable,DoubleWritable,VIntWritable,VLongWritable,Text,BooleanWritable.
	
	for function 2 , we select value types:	
		IntWritable,FloatWritable,LongWritable,DoubleWritable,VIntWritable,VLongWritable,Text,BooleanWritable.
	
	we configure job for every key-value choice, so we have almost 64 jobs.

*1.1.2 Compression Test*

	for function 3, we configure job for every compress algorithm.
	Now, we only tested snappy compression , so we have one job, one .testcase.


**1.2 Test Result**

*1.2.1 KV Test Result*

As shown in the following table, the first column stand for key types, and the first row stand for value types."***√***" means key-value choice is tested successfully."exception" means key-value choice is refused.

<table class="table table-bordered table-striped table-condensed">
	<tr>
		<td></td>
		<td>IntWritable</td>
		<td>FloatWritable</td>
		<td>LongWritable</td>
		<td>DoubleWritable</td>
		<td>VIntWritable</td>
		<td>VLongWritable</td>
		<td>Text</td>
		<td>BooleanWritable</td>
	</tr>
	<tr>
		<td>IntWritable</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
	</tr>
	<tr>
		<td>FloatWritable</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
	</tr>
	<tr>
		<td>LongWritable</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
	</tr>
	<tr>
		<td>DoubleWritable</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
	</tr>
	<tr>
		<td>VIntWritable</td>
		<td>exception</td><td>exception</td>
		<td>exception</td><td>exception</td>
		<td>exception</td><td>exception</td>
		<td>exception</td><td>exception</td>
	</tr>
	<tr>
		<td>VLongWritable</td>
		<td>exception</td><td>exception</td>
		<td>exception</td><td>exception</td>
		<td>exception</td><td>exception</td>
		<td>exception</td><td>exception</td>
	</tr>
	<tr>
		<td>Text</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
	</tr>
	<tr>
		<td>BooleanWritable</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
		<td>√</td><td>√</td>
	</tr>
</table>

*1.2.2 Compression Test Result*




##2. Conclusion ##
we have tested nativetask's compability. This paper shows nativetask have supported most key-value, but some are currently not supported(i.e VIntWritable key and VLongWritable key).

Next, we will test more about nativetask, as stress,combiner,performance and so on.