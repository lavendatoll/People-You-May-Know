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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;

public class PeopleYouMightKnow extends Configured implements Tool {

    public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {

        String[] userRow, friendList;
        Text keyUser = new Text();
        Text suggest = new Text();
        Text existed = new Text();
//        private final static IntWritable one = new IntWritable(1);
        int i,j;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("in mapper, input "+ key + " " + value + ";");
            userRow = value.toString().split("\\s");
            if(userRow.length == 1){
                userRow = null;
                return;
            }
            friendList = userRow[1].split(",");
            for(i = 0; i < friendList.length ;i ++ ){
                keyUser.set(new Text(friendList[i]));
                for(j = 0 ;j < friendList.length; j++ ){
                    if( i != j){
                        suggest.set(friendList[j]+",1");
                        context.write(keyUser,suggest);
//                        System.out.print("suggest:");
//                        System.out.println(suggest.toString());
//                        System.out.println(keyUser + ",(" + suggest.toString() + ")");
                    }
                }
                existed.set(userRow[0]+",-1");
                context.write(keyUser,existed);
//                System.out.println("existed");
//                System.out.println(keyUser + ",(" + existed.toString() + ")");
            }

        }
    }

    public static class FriendReducer extends Reducer<Text, Text, Text, Text> {
//        private IntWritable result = new IntWritable();
        int K = 10;
        HashMap<String,Integer> recommend;
        StringBuffer suggest;
        StringBuffer tmp;
        StringBuilder result;
        List<String> input;
        String id;
        Text keyId;
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
//            System.out.println("reduce:value");
//            for(Text val: values)
//            System.out.println(val.toString());
            recommend = new HashMap<String, Integer>();
            suggest = new StringBuffer();
            tmp = new StringBuffer();
            result = new StringBuilder();
            //System.out.println("Kye in reducer " + key.toString());

            input = new ArrayList<String>();
            for (Text val : values) {
                input.add(val.toString());
                //System.out.println("getting vals here" + val);
            }

            for(String val: input){
                id = val.toString().split(",")[0];
                if(val.contains("-1"))
                    recommend.put(id,new Integer(-1));

                if(recommend.containsKey(id)) {
                    if(recommend.get(id) != -1)
                    recommend.put(id, recommend.get(id) + 1);
                }
                else
                    recommend.put(id,1);
            }
            Comparator<Map.Entry<String,Integer>> valueComparator = new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    int v1 = o1.getValue();
                    int v2 = o2.getValue();
                    if(v1== v2)
                        return o1.getKey().compareTo(o2.getKey());
                    return v1 - v2;
                }
            };
            PriorityQueue<Map.Entry<String,Integer>> queue = new PriorityQueue<Map.Entry<String, Integer>>(K,valueComparator);

            for(Map.Entry<String,Integer> en: recommend.entrySet() ){
                if(queue.size() < K){
                    queue.offer(en);
                }
                else{
                    Map.Entry<String,Integer> smallest = queue.poll();
                    if(smallest.getValue() < en.getValue())
                        queue.offer(en);
                    else
                        queue.offer(smallest);
                }
            }

                // turn queue's id into a string
                List<String> keys = new ArrayList<String>();

                while(!queue.isEmpty()){
                    keys.add(queue.poll().getKey());
                }
                Collections.reverse(keys);
                for(String s : keys){
                    result.append(s);
                    result.append(" ");
                }

//                result = result.reverse();
//                System.out.println(key.toString() + "\t" + result);
//                keyId = new IntWritable(Integer.parseInt(key.toString()));
                
            
            keyId = new Text(key.toString());
            context.write(keyId,new Text(result.toString()));
        }
    }
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        Job job = new Job(getConf(), "peopleyoumayknow");
        job.setJarByClass(PeopleYouMightKnow.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(FriendMapper.class);
        job.setCombinerClass(FriendReducer.class);

        job.setReducerClass(FriendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }
    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = new Job(conf, "people you might know");
//                job.setJarByClass(PeopleYouMightKnow.class);
//        job.setMapperClass(FriendMapper.class);
//        job.setCombinerClass(FriendReducer.class);
//        job.setReducerClass(FriendReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new PeopleYouMightKnow(), args);

        System.exit(res);
    }
}