import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairsOccurrence {

  public static class WordPair implements Writable,WritableComparable<WordPair> {
	private Text word;
	private Text neighbor;

	// Default Constructor 
	public WordPair() {
		this.word = new Text();
		this.neighbor = new Text();
	}
	
	// Parameterized Constructor
	public WordPair(Text wordOne, Text wordTwo) {
		this.word = wordOne;
		this.neighbor = wordTwo;
	}

	// Compare to
	public int compareTo(WordPair other) {
        int returnVal = this.word.compareTo(other.word);
        if(returnVal != 0){
            return returnVal;
        }
        if(this.neighbor.toString().equals("*")){
            return -1;
        }else if(other.neighbor.toString().equals("*")){
            return 1;
        }
        return this.neighbor.compareTo(other.neighbor);
        }

	// To String
	public String toString() {
		return "{word=["+word+"]"+" neighbor["+neighbor+"]}";
	}
	// ReadFields 
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		neighbor.readFields(in);
	}
	// Write 
	public void write(DataOutput out) throws IOException {
		word.write(out);
		neighbor.write(out);
	}

	// Read
	public static WordPair read(DataInput in) throws IOException {
		WordPair wordPair = new WordPair();
		wordPair.readFields(in);
		return wordPair;
	}
 
 public int hashCode() {
        int result = word != null ? word.hashCode() : 0;
        result = 163 * result + (neighbor != null ? neighbor.hashCode() : 0);
        return result;
    }

	// Equals 
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WordPair wordPair = (WordPair) o;

        if (neighbor != null ? !neighbor.equals(wordPair.neighbor) : wordPair.neighbor != null) return false;
        if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

        return true;
    }

  }

  public static class CoOccurrenceMapper
       extends Mapper<LongWritable, Text, WordPair, IntWritable>{
	

    private final static IntWritable one = new IntWritable(1);
    private WordPair wordPair = new WordPair();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        int neighbors = context.getConfiguration().getInt("neighbors", 2);
		String line = value.toString().replaceAll("[^a-z\\s]","");
	        String[] tokens = line.split("\\s+");

	        if (tokens.length > 1) {
	          for (int i = 0; i < tokens.length; i++) {
			if(!tokens[i].equals("")) {
	              wordPair.word.set(tokens[i]);

	             int start = (i - neighbors < 0) ? 0 : i - neighbors;
	             int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
	              for (int j = start; j <= end; j++) {
	                  if (j == i) continue;
	                   wordPair.neighbor.set(tokens[j]);
	                   context.write(wordPair, one);
	              }
		}
	          }
	      }
  }
}

  public static class reducer
       extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(WordPair key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key,result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(PairsOccurrence.class);
    job.setMapperClass(CoOccurrenceMapper.class);
    job.setCombinerClass(reducer.class);
    job.setReducerClass(reducer.class);
    job.setOutputKeyClass(WordPair.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
