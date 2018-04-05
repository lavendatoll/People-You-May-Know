package peopleyoumightknow;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.*;

//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;



public class PeopleYouMightKnow extends Configured implements Tool {

	

	    private static HashMap<String,Integer> recommend;
	    private static HashMap<String,HashMap<String,Integer>> recoList = new HashMap<String, HashMap<String, Integer>>();
	    private static HashMap<String,String> outputPair = new HashMap<String, String>();
	    public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {

	        String[] userRow, friendList;
	        Text keyUser = new Text();
	        Text suggest = new Text();
	        Text existed = new Text();

	        int i,j;

	        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	            userRow = value.toString().split("\\s");
	            if(userRow.length == 1){
	                userRow = null;
	                return;
	            }
	            friendList = userRow[1].split(",");
	            for(i = 0; i < friendList.length ;i ++ ){
	                keyUser.set(new Text(friendList[i]));
	                for(j = 0 ;j < friendList.length; j++ ){
	                    if( i != j) {
	                        suggest.set(friendList[j] + ",1");
	                        context.write(keyUser,suggest);
	                    }
	                }
	                existed.set(userRow[0]+",-1");
	                context.write(keyUser,existed);
	            }
	               //System.out.println(keyUser.toString());
	        }
	    }

	    public static class FriendReducer extends Reducer<Text, Text, Text, Text> {
	        int K = 10;

	        List<String> input;

//	        HashMap<Integer,Integer> recommend;
//	        HashMap<String,HashMap<Integer,Integer>> recoList = new HashMap<String, HashMap<Integer, Integer>>();
//	        @Override
	        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

	            recommend = new HashMap<String, Integer>();
//	            recommend = new HashMap<Integer, Integer>();
	            input = new ArrayList<String>();
	            //System.out.println("reduce" + key.toString());
	            for (Text val : values)
	                input.add(val.toString());

	            for(String val: input){
	//
//	                if(val.toString().contains(" ") && !val.toString().contains(",") )
//	                    continue;
	                String id = val.toString().split(",")[0];
//	                System.out.println(val.toString());
//	                int id = Integer.parseInt(val.toString().split(",")[0]);
	                if(val.contains("-1"))
	                    recommend.put(id,-1);

	                if(recommend.containsKey(id)) {
	                    if(recommend.get(id) != -1)
	                        recommend.put(id, recommend.get(id) + 1);
	                }
	                else
	                    recommend.put(id,1);

	            }
	            recoList.put(key.toString(),recommend);


	        }

	        @Override
	        public void cleanup(Context context) throws IOException, InterruptedException {
	            Text keyUser = new Text();
	            Text output = new Text();
	            for (Map.Entry<String, HashMap<String, Integer>> m : recoList.entrySet()) {
	                StringBuilder result = new StringBuilder();
	                String k = m.getKey();
	                HashMap<String, Integer> v = m.getValue();
	                HashMap<Integer, Integer> newmap = new HashMap<Integer, Integer>();
	                for (String s : v.keySet()) {
	                    s = s.split(" ")[0];
	                    if(v.get(s) != null && !s.equals(""))
	                        newmap.put(Integer.parseInt(s), v.get(s));
	                }

	                HashMap sortedmap = sortByValues(newmap);
	                int count = 0;
	                for (Object okey : sortedmap.keySet()) {
	                    if(!okey.toString().equals("")) {

	                        if (Integer.parseInt(sortedmap.get(okey).toString()) > 0) {
	                                result.append(okey.toString());
	                                result.append(" ");


	                        }
	                        if (count ++ == 9)
	                            break;
	                    }
	                }

//	                System.out.println(k);

	                outputPair.put(k,result.toString());

	            }

	            for(String k : outputPair.keySet()){
	                keyUser.set(k + "\t");
	                output.set(outputPair.get(k));

	                if(!output.toString().equals("")){
//	                    if (k.equals("924") || k.equals("8491") || k.equals("8492") || k.equals("9019") || k.equals("9020") ||
//	                            k.equals("9021") || k.equals("9022") || k.equals("9990") || k.equals("9992") || k.equals("9993"))
//	                    System.out.println("o:" + output+";");
	                    context.write(keyUser,output);

	                }

//	                    System.out.println(k.toString() + ": " +output);
	            }
	        }
	        private static HashMap sortByValues(HashMap map) {
	            List list = new LinkedList(map.entrySet());
	            // Defined Custom Comparator here

	            Collections.sort(list, new Comparator() {
	                // descending order
	                public int compare(Object o1, Object o2) {

	                    if (((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue()) == 0)
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
	        private static HashMap sortByValuesandKey(HashMap<String,Integer> map) {
	            HashMap<Integer,Integer> newmap = new HashMap<Integer, Integer>();

	            for(String key: map.keySet()){
	                newmap.put(Integer.parseInt(key),map.get(key));
	            }
	            List list = new LinkedList(newmap.entrySet());
	            // Defined Custom Comparator here

	            Collections.sort(list, new Comparator() {
	                // descending order
	                public int compare(Object o1, Object o2) {

	                    if (((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue()) == 0)
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
//	        System.out.println(Arrays.toString(args));
//	        Job job = new Job(getConf(), "peopleyoumayknow");
//	        job.setJarByClass(PeopleYouMightKnow.class);
//	        job.setOutputKeyClass(Text.class);
//	        job.setOutputValueClass(IntWritable.class);
//
//	        job.setMapperClass(FriendMapper.class);
//	        job.setCombinerClass(FriendReducer.class);
//
//	        job.setReducerClass(FriendReducer.class);
//	        job.setOutputKeyClass(Text.class);
//	        job.setOutputValueClass(Text.class);
//	        FileInputFormat.addInputPath(job, new Path(args[0]));
//	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	       // System.exit(job.waitForCompletion(true) ? 0 : 1);

	        return 0;
	    }
	    public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        Job job = new Job(conf, "people you might know");
	        job.setJarByClass(PeopleYouMightKnow.class);
	        job.setMapperClass(FriendMapper.class);
	        job.setCombinerClass(FriendReducer.class);
	        job.setReducerClass(FriendReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);

//	        System.out.println(Arrays.toString(args));
//	        int res = ToolRunner.run(new Configuration(), new PeopleYouMightKnow(), args);
//	
//	        System.exit(res);
	    }
	}

