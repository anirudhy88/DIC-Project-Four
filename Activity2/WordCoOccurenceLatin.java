import java.io.*;
import java.util.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCoOccurenceLatin {
  static Map<String, ArrayList<String>> mapMain = new HashMap<String, ArrayList<String>>();

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

  public static class readCsv {
    public static void createMainMap(String fileName) throws IOException{
	BufferedReader br = new BufferedReader(new FileReader(fileName));	
	String line = null;
		
	while((line = br.readLine()) != null) {
		String[] array = line.split(",");
		for(int i = 1; i < array.length; i++) {
			if(!mapMain.containsKey(array[0])) {
			   ArrayList<String> temp = new ArrayList<String>();
			   temp.add(array[1]);
                           mapMain.put(array[0], temp);
			}
			else {
			   mapMain.get(array[0]).add(array[i]);
			}
		}		
	}
    }
  }

  public static class TokenizerMapper
       extends Mapper<Object, Text, WordPair, Text>{

   
    private Text word = new Text();
    private WordPair wordPair = new WordPair();
    private Text wordValueToEmit = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String actualStr = value.toString();
      
      // <docid, [chapter#, line#]>
      int index = actualStr.indexOf(">");
      if(index < 0) return;
      String metaData = actualStr.substring(actualStr.indexOf("<")+1, actualStr.indexOf(">"));
      System.out.println("metadata is : " + metaData);
      String[] metaDataArray = metaData.split("\\s+");
      //StringBuffer emittingString = new StringBuffer();
      String emittingString = "";
      String headerLessString = actualStr.replaceAll("<" + metaData + ">", "");
      //headerLessString = headerLessString.trim();
      headerLessString = headerLessString.trim().replaceAll("[^a-zA-Z ]+", "");
      //emittingString.append("<");
      emittingString = "<";
      if(metaDataArray.length == 2) {
         // <luc. 1.1>
	 emittingString += metaDataArray[0] + ", [" ;
         String[] chpLin1 = metaDataArray[1].split(".");
	 if(chpLin1.length == 1) {
	   emittingString +=  chpLin1[0] + ",";
	 } else if (chpLin1.length == 2){
 	   emittingString += chpLin1[0] + "," +  chpLin1[1] + ",";
	 } else {
	   emittingString +=  ","  + " ,";
         }
      } else if(metaDataArray.length == 3){
	 // <verg. aen. 1.1>
	 // <ambrose. ap_david_altera. 1>
	 // <boe. pat. Incipit> 
	 // <boe. pat. 10-4>
         emittingString += metaDataArray[0]  + metaDataArray[1] + ", [";
	 ArrayList<String> chpLin2 = new ArrayList<String>();
         if(metaDataArray[2].indexOf("-") >= 0) {
	  String[] chTempOne = metaDataArray[2].split("-");
	  for(String s : chTempOne)
          chpLin2.add(s); }
	 else if(metaDataArray[2].indexOf(".") >= 0) {
	   String[] chTempTwo = metaDataArray[2].split(".");
	  for(String s : chTempTwo)
          chpLin2.add(s); }
 	 if(chpLin2.size() == 1) {
	   emittingString +=  chpLin2.get(0) + ", null" + ",";
	 } else if(chpLin2.size() == 2){
	   //System.out.println("metaDataArray is :"meteDataArray[0] + "  " +metaDataArray[1]);
	   emittingString += chpLin2.get(0) + "," + chpLin2.get(1) + ",";
         } else {
	   emittingString +=  ","  + " ,";
         }
      } else if(metaDataArray.length == 4){
	emittingString += metaDataArray[0]  + metaDataArray[1] +  metaDataArray[2] + ", [";
	 ArrayList<String> chpLin2 = new ArrayList<String>();
         if(metaDataArray[3].indexOf("-") >= 0) {
	  String[] chTempOne = metaDataArray[3].split("-");
	  for(String s : chTempOne)
          chpLin2.add(s); }
	 else if(metaDataArray[3].indexOf(".") >= 0) {
	   String[] chTempTwo = metaDataArray[3].split(".");
	  for(String s : chTempTwo)
          chpLin2.add(s); }
 	 if(chpLin2.size() == 1) {
	   emittingString +=  chpLin2.get(0) + ", null" + ",";
	 } else if(chpLin2.size() == 2){
	   emittingString += chpLin2.get(0) + "," + chpLin2.get(1) + ",";
           } else {
	   emittingString +=  ","  + " ,";
         }
	} else {
	  return;
       }
      int cou = 0;
      StringTokenizer itr = new StringTokenizer(headerLessString);
      //int neighbors = context.getConfiguration().getInt("neighbors", 2);
      String[] tokens = headerLessString.split("\\s+");

      if (tokens.length > 1) {
        for (int i = 0; i < tokens.length-1; i++) {
          
	     
             cou++;
	String text = tokens[i];
	//System.out.println("Emitting string is :" + emittingString);
 	//System.out.println("indexof is : " +headerLessString.indexOf(text));
	String indexToAdd = "" + cou;
	emittingString += indexToAdd  + "]>";
	wordValueToEmit.set(emittingString.toString());
	text.replaceAll("j", "i");
	text.replaceAll("v", "u");
        //System.out.println("Emitting string is :" + emittingString);
        //int neighbors = context.getConfiguration().getInt("neighbors", 1);
        
       	  tokens[i+1].replaceAll("j", "i");
	  tokens[i+1].replaceAll("v", "u");
        if(mapMain.containsKey(text)) {
	  ArrayList<String> temp = mapMain.get(text);
	  for(String str : temp) {
             wordPair.word.set(str);
             wordPair.neighbor.set(tokens[i+1]);
	     //word.set(str);
             context.write(wordPair, wordValueToEmit);
	  }
	} else {
	    wordPair.word.set(text);
	    wordPair.neighbor.set(tokens[i+1]);
            context.write(wordPair, wordValueToEmit);
	}

	emittingString = emittingString.substring(0, emittingString.length()-2-indexToAdd.length());
	//System.out.println("Emitting string is :" + emittingString);
	//emittingString = emittingString.delete(emittingString.length()-3, emittingString.length()-1);
      }
     }
    }
  }
    public static class IntSumReducer
       extends Reducer<WordPair,Text,WordPair,Text> {
    private Text result = new Text();
    private int count = 0;
    public void reduce(WordPair key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      StringBuffer completeString = new StringBuffer();
      
      for (Text val : values) {
        completeString.append(val.toString());
	completeString.append(",");
	count++;
      }
      completeString.append("   count : ");
      completeString.append(String.valueOf(count)) ; 
      result.set(completeString.toString());
      context.write(key, result);
      count = 0;
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    readCsv.createMainMap(args[2]);
    Job job = Job.getInstance(conf, "word count latin");
    job.setJarByClass(WordCoOccurenceLatin.class);
    // job.setNumReduceTasks(5);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(WordPair.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
