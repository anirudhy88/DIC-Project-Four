import java.io.*;
import java.util.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountLatin {
  static Map<String, ArrayList<String>> mapMain = new HashMap<String, ArrayList<String>>();
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
       extends Mapper<Object, Text, Text, Text>{

   
    private Text word = new Text();
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
      //System.out.println("metaDataArray is : ");
      //for(String s : metaDataArray) {
	//System.out.print(s + " ");
      //}
      //System.out.println("headerLessString is :"+ headerLessString);
      int cou = 0;
      StringTokenizer itr = new StringTokenizer(headerLessString);
      while (itr.hasMoreTokens()) {
        cou++;
	String text = itr.nextToken();
        /*emittingString.append("headerLessString.indexOf(text)");
	emittingString.append("]>");*/
	//System.out.println("Emitting string is :" + emittingString);
 	//System.out.println("indexof is : " +headerLessString.indexOf(text));
	String indexToAdd = "" + cou;
	emittingString += indexToAdd  + "]>";
	wordValueToEmit.set(emittingString.toString());
	text.replaceAll("j", "i");
	text.replaceAll("v", "u");
        //System.out.println("Emitting string is :" + emittingString);

        if(mapMain.containsKey(text)) {
	  ArrayList<String> temp = mapMain.get(text);
	  for(String str : temp) {
	     word.set(str);
             context.write(word, wordValueToEmit);
	  }
	} else {
	    word.set(text);
            context.write(word, wordValueToEmit);
	}
	emittingString = emittingString.substring(0, emittingString.length()-2-indexToAdd.length());
	//System.out.println("Emitting string is :" + emittingString);
	//emittingString = emittingString.delete(emittingString.length()-3, emittingString.length()-1);
      }
    }
  }

 /* public static class IntSumCombiner
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String completeString = "";
      
      for (Text val : values) {
        completeString += val.toString() + ",";
      }
      result.set(completeString);
      context.write(key, result);
    }
  }*/
    public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    private int count = 0;
    public void reduce(Text key, Iterable<Text> values,
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
    job.setJarByClass(WordCountLatin.class);
    job.setNumReduceTasks(5);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
