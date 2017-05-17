//driver class


import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class driver64 {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("mapred.textoutputformat.separator",",");
		Job job = new Job(conf, "DemoTask1");
		job.setJarByClass(driver64.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(map64.class);
		job.setReducerClass(reduce64.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		
		
		job.waitForCompletion(true);
		
		
		
			}}



//mapper class

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
public class map64 extends Mapper<LongWritable, Text, Text, IntWritable> {
	IntWritable no = new IntWritable(1);
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\,+");
		if(lineArray.length>1)
		{
		
		int sur = Integer.parseInt(lineArray[1]);
		if(sur==0)
		{
		context.write(new Text("Number of people survived in Titanic"), no);
		}
	}
	}
}




//reducer class


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
public class reduce64 extends Reducer<Text,IntWritable,Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> value , Context con) throws IOException, InterruptedException
	{
		int sum =0;
		for(IntWritable v : value)
		{
			sum = sum + v.get();
		}
		con.write(key, new IntWritable(sum));
	}

}


//output


~/Desktop$ hadoop fs -cat /user/chins/output/64op3/part-r-00000
SEQorg.apache.hadoop.io.Text org.apache.hadoop.io.IntWritable���,���G g�#[_�%! Number of people survived in Titanic%:~/Desktop$ 
~/Desktop$ hadoop fs -text /user/chins/output/64op3/part-r-00000
Number of people survived in Titanic	549




Note as it is sequence file it is in unreadable format hence opened using the -text command.
