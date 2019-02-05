import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.stream.StreamSupport;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BigramCount {
	static final String DEL = " *-------";
	
	public static class TokenizerMapper extends Mapper<Object, Text, PairText, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static PairText mapcount = new PairText(DEL, "Mapper Call Count: ", 1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(mapcount, one);
			
			String last = null;
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens())	{
				String current = itr.nextToken().toLowerCase();
				if(last != null) {
					context.write(new PairText(current, last), one);
					context.write(new PairText(last, current), one);
				}
				last = current;
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<PairText, IntWritable, PairText, IntWritable> {
		
		public void reduce(PairText key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = StreamSupport.stream(values.spliterator(), false).mapToInt(x->x.get()).sum();
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = init(args);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	static Job init(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		FileUtils.deleteDirectory(new File(otherArgs[1]));
		Job job = Job.getInstance(conf, "bigram count");
		
		job.setJarByClass(BigramCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(PairText.class);
		job.setOutputValueClass(IntWritable.class);
		return job;
	}
	
	static class PairText implements WritableComparable<PairText> {  
		public Text a, b;
		public IntWritable p;
		
		public PairText(String a, String b, int pr) {
			this.a = new Text(a); 
			this.b = new Text(b); 
			this.p = new IntWritable(pr);
		}
		public PairText() { this("", "", 0); }
		public PairText(String a,String b) { this(a, b, 0); }
		
		public String a() { return a.toString(); }
		public String b() { return b.toString(); }
		
		public void set(Text a,Text b) { this.a = a; this.b = b; }
		public void set(String a, String b) { this.a = new Text(a); this.b = new Text(b); }
		
		public String toString() { return a + " " + b;}
		
		public void readFields(DataInput in) throws IOException { 
			a.readFields(in);
			b.readFields(in); 
			p.readFields(in);
		}
		public void write(DataOutput out) throws IOException { 
			a.write(out); 
			b.write(out);
			p.write(out);
		}
		
		public int compareTo(PairText o) {
			if(p.compareTo(o.p) != 0)
				return p.compareTo(o.p);
		    return a.compareTo(o.a) != 0 ? a.compareTo(o.a) : b.compareTo(o.b);
		}
		public boolean equals(PairText o) {
		    return a.equals(o.a) && b.equals(o.b) ? true : false;
		}
	}
}