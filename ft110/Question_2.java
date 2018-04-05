package prodectrecommendation;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;

public class ProductRecommendation extends Configured implements Tool {


  private static HashMap<String,Integer> map = new HashMap<String, Integer>();
  private static HashMap<String,Integer> pairmap = new HashMap<String, Integer>();
  private static HashMap<String,Integer> triplemap = new HashMap<String, Integer>();
  private final static IntWritable one = new IntWritable(1);


  public static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
      Text item = new Text();
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String[] products = value.toString().split("\\s+");
          for(String s : products){
                  item.set(new Text(s));
                  context.write(item, one);
          }

      }
  }

  public static class FirstReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

          int sum = 0;
          for (IntWritable val : values)
               sum = sum + val.get();

          context.write(key, new IntWritable(sum));

          if(sum >= 100)
              map.put(key.toString(),sum);
      }
  }

  public static class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

      Text pair = new Text();
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

          String[] products = value.toString().split(" ");
          for(int i = 0 ; i < products.length -1 ; i++){
              for(int j= i+1 ; j < products.length ; j++){
                  String p1 =products[i];
                  String p2 =products[j];
                  if(map.containsKey(p1) && map.containsKey(p2)){
                      if(p1.compareTo(p2) < 0 )
                          pair.set(new Text(p1+"," +p2));
                      else
                          pair.set(new Text(p2+"," +p1));
                      context.write(pair,one);

                  }
              }
          }
      }

  }
  public static class SecondReducer extends Reducer<Text, IntWritable, Text, Text> {


      HashMap<String, Double> confs = new HashMap<String, Double>();

      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

          int sum = 0;
          for (IntWritable val : values) {
              sum += val.get();
          }

          if(sum >= 100)
              pairmap.put(key.toString(),sum);


      }
      @Override
      public void cleanup (Context context) throws IOException, InterruptedException {
          System.out.println("here");
          for( String pair : pairmap.keySet()){
//                 System.out.println(pair);
                  String p1 = pair.split(",")[0];
                  String p2 = pair.split(",")[1];

                  double conf = pairmap.get(pair) / (double) map.get(p1);
                  confs.put(p1 + " -> " + p2,conf);

                  conf = pairmap.get(pair) / (double) map.get(p2);
                  confs.put(p2 + " -> " + p1,conf);

          }

          Map sortedMap = sortByValues(confs);

          int counter = 0;
          for (Object key: sortedMap.keySet()) {

              if (counter ++ == 20) {
                  break;
              }
              context.write(new Text(key.toString()), new Text(sortedMap.get(key).toString()));
          }


      }
      private static HashMap sortByValues(HashMap map) {
          List list = new LinkedList(map.entrySet());
          // Defined Custom Comparator here

          Collections.sort(list, new Comparator() {
              // descending order
              public int compare(Object o1, Object o2) {
                  return ((Comparable) ((Map.Entry) (o2)).getValue())
                          .compareTo(((Map.Entry) (o1)).getValue());
              }
          });

          // using LinkedHashMap to preserve the insertion order
          HashMap sortedHashMap = new LinkedHashMap();
          for (Iterator it = list.iterator(); it.hasNext();) {
              Map.Entry entry = (Map.Entry) it.next();
              sortedHashMap.put(entry.getKey(), entry.getValue());
          }
          return sortedHashMap;
      }

  }

  public static class ThirdMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

      Text triple = new Text();
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

          String[] products = value.toString().split(" ");
          for(int i = 0 ; i < products.length - 2 ; i++){
              for(int j = i+1 ; j < products.length - 1 ; j++){
                  for(int k = j+1 ; k < products.length ; k++){
                      List<String> p = new ArrayList<String>();
                      p.add(products[i]);
                      p.add(products[j]);
                      p.add(products[k]);
                      String pair1;
                      String pair2;
                      String pair3;

                      Collections.sort(p);
                      pair1 = p.get(0) +","+p.get(1);
                      pair2 = p.get(1) +","+p.get(2);
                      pair3 = p.get(0) +","+p.get(2);
                  if(pairmap.containsKey(pair1) && pairmap.containsKey(pair2) && pairmap.containsKey(pair3)) {
                      triple.set(p.get(0) +","+p.get(1)+","+p.get(2));
                      context.write(triple, one);
                    }

                  }
              }
          }
      }

  }
  public static class ThirdReducer extends Reducer<Text, IntWritable, Text, Text> {


      HashMap<String, Double> confs = new HashMap<String, Double>();

      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

          int sum = 0;
          for (IntWritable val : values) {
              sum += val.get();
          }

          if(sum >= 100)
              triplemap.put(key.toString(),sum);


      }
      @Override
      public void cleanup (Context context) throws IOException, InterruptedException {
          System.out.println("here");
          for( String triple : triplemap.keySet()){
//                 System.out.println(pair);
              String p1 = triple.split(",")[0];
              String p2 = triple.split(",")[1];
              String p3 = triple.split(",")[2];

              int dom = triplemap.get(triple);

              String rule ="("+p1+","+p2+")->"+p3;
              double conf = dom / (double) pairmap.get(p1+","+p2);
              confs.put(rule,conf);

              rule = "("+p1+","+p3+")->"+p2;
              conf = dom / (double) pairmap.get(p1+","+p3);
              confs.put(rule,conf);

              rule = "("+p2+","+p3+")->"+p1;
              conf = dom / (double) pairmap.get(p2+","+p3);
              confs.put(rule,conf);

          }

          Map sortedMap = sortByValues(confs);

          int counter = 0;
          for (Object key: sortedMap.keySet()) {

              if (counter ++ == 20) {
                  break;
              }
              context.write(new Text(key.toString()), new Text(sortedMap.get(key).toString()));
          }


      }
      private static HashMap sortByValues(HashMap map) {
          List list = new LinkedList(map.entrySet());
          // Defined Custom Comparator here

          Collections.sort(list, new Comparator() {
              // descending order
              public int compare(Object o1, Object o2) {
                  if(((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue()) ==0)
                      return ((Comparable) ((Map.Entry) (o1)).getKey()).compareTo(((Map.Entry) (o2)).getKey());
                  return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
              }
          });

          // using LinkedHashMap to preserve the insertion order
          HashMap sortedHashMap = new LinkedHashMap();
          for (Iterator it = list.iterator(); it.hasNext();) {
              Map.Entry entry = (Map.Entry) it.next();
              sortedHashMap.put(entry.getKey(), entry.getValue());
          }
          return sortedHashMap;
      }

  }

public int run(String[] args) throws Exception {
       
       return 0;
  }
  public static void main(String[] args) throws Exception {
      
	  Configuration conf1 = new Configuration();
      Job job1 = new Job(conf1, "pass one");
      job1.setJarByClass(ProductRecommendation.class);
      job1.setMapperClass(FirstMapper.class);
      job1.setCombinerClass(FirstReducer.class);
      job1.setReducerClass(FirstReducer.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1, new Path(args[1]));

      job1.waitForCompletion(true);

      Configuration conf2 = new Configuration();
      Job job2 = new Job(conf2, "pass two");
      job2.setJarByClass(ProductRecommendation.class);
      job2.setMapperClass(SecondMapper.class);
      job2.setReducerClass(SecondReducer.class);
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(IntWritable.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);
      FileInputFormat.setInputPaths(job2,args[0]);
      FileOutputFormat.setOutputPath(job2, new Path(args[2]));
      
      job2.waitForCompletion(true);

      
      Configuration conf3 = new Configuration();
      Job job3 = new Job(conf3, "pass two");
      job3.setJarByClass(ProductRecommendation.class);
      job3.setMapperClass(ThirdMapper.class);
      job3.setReducerClass(ThirdReducer.class);
      job3.setMapOutputKeyClass(Text.class);
      job3.setMapOutputValueClass(IntWritable.class);
      job3.setOutputKeyClass(Text.class);
      job3.setOutputValueClass(Text.class);
      FileInputFormat.setInputPaths(job3,args[0]);
      FileOutputFormat.setOutputPath(job3, new Path(args[3]));
      

      
      System.exit(job3.waitForCompletion(true) ? 0 : 1);


//      System.out.println(Arrays.toString(args));
//      int res = ToolRunner.run(new Configuration(), new ProductRecommendation(), args);
//      System.exit(res);
  }
}