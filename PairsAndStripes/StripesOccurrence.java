import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
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

public class StripesOccurrence {

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
	public static  WordPair read(DataInput in) throws IOException {
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
  
  public static class NewMapWritable extends MapWritable {
     public String toString() {
	String s = new String("{ ");
	Set<Writable> keys = this.keySet();
	for(Writable key : keys) {
	  IntWritable count = (IntWritable) this.get(key);
	  s =  s + key.toString() + "=" + count.toString() + ",";
	}
	s = s + " }";
	return s;
     }
  }

  public static class CoOccurrenceMapper
       extends Mapper<LongWritable, Text, Text, NewMapWritable>{
	

    private final static NewMapWritable mapToStore = new NewMapWritable();
    private Text word = new Text();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        int neighbors = context.getConfiguration().getInt("neighbors", 2);
		String line = value.toString().replaceAll("[^a-z\\s]","");
	        String[] tokens = line.split("\\s+");

	        if (tokens.length > 1) {
	          for (int i = 0; i < tokens.length; i++) {
			if(!tokens[i].equals("")) {
	              word.set(tokens[i]);
		      mapToStore.clear();

	             int start = (i - neighbors < 0) ? 0 : i - neighbors;
	             int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
	              for (int j = start; j <= end; j++) {
	                  if (j == i) continue;
			Text neighbor = new Text(tokens[j]);
			if(mapToStore.containsKey(neighbor)) {
			   IntWritable count = (IntWritable) mapToStore.get(neighbor);
	           	   count.set(count.get() + 1);
	                   
	              } else {
				mapToStore.put(neighbor, new IntWritable(1));
			}
		      }
		    context.write(word, mapToStore);
		}
	          }
	      }
  }
}

  public static class reducer
       extends Reducer<Text,MapWritable,Text,NewMapWritable> {
    private NewMapWritable incMap = new NewMapWritable();
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<NewMapWritable> values,
                       Context context

                       ) throws IOException, InterruptedException {
      incMap.clear();
      for (NewMapWritable val : values) {
	addAll(val);
      }

      context.write(key, incMap);
    }

   public void addAll(NewMapWritable mapWritable) {
	Set<Writable> keys = mapWritable.keySet();
	for(Writable key : keys) {
		IntWritable fromCount = (IntWritable) mapWritable.get(key);
		if( incMap.containsKey(key)) {
			IntWritable count = (IntWritable) incMap.get(key);
			count.set(count.get() + fromCount.get());
		} else {
			incMap.put(key, fromCount);
		}
	}
   }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(StripesOccurrence.class);
    job.setMapperClass(CoOccurrenceMapper.class);
    job.setCombinerClass(reducer.class);
    job.setReducerClass(reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NewMapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
