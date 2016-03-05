import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

/**
 * Problem 2 for Machine Problem 1 from CS511 Sp16
 * Created by Wesley on 3/1/16.
 */
public class P2 {


    //Config to your own Directories
    private final static String HDFS_ADDRESS = "hdfs://127.0.0.1:9000/user/Wesley";
    private final static String DATA_INPUT = HDFS_ADDRESS + "/cs511/mp1/data/cs511_data";
    private final static String DATA_OUTPUT_P2_Tmp = HDFS_ADDRESS + "/cs511/mp1/data/out.p2.tmp";
    private final static String DATA_OUTPUT_P2 = HDFS_ADDRESS + "/cs511/mp1/data/out.p2";

    public static class P2Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable ikey, Text ivalue, Context context)
                throws IOException, InterruptedException {
            String[] s = ivalue.toString().split(",");
            String userId = s[1];
            int clickNum = Integer.valueOf(s[3]);
            String date = s[0];
            if (userId.length() > 0 && clickNum == 1)
                context.write(new Text(userId), new Text(date));
        }
    }

    public static class P2Reducer1 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text _key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            Iterator<Text> it = values.iterator();
            while (it.hasNext())
                context.write(_key, it.next());
        }
    }

    public static class UserIDDateWritable implements WritableComparable<UserIDDateWritable> {
        private String userID;
        private String time;
        private String date;

        public UserIDDateWritable() {
        }

        public UserIDDateWritable(String id, String time, String date) {
            this.userID = new String(id);
            this.time = new String(time);
            this.date = new String(date);
        }

        public void write(DataOutput out)
                throws IOException {
            out.writeUTF(userID);
            out.writeUTF(time);
            out.writeUTF(date);
        }

        public void readFields(DataInput in)
                throws IOException {
            userID = new String(in.readUTF());
            time = new String(in.readUTF());
            date = new String(in.readUTF());
        }

        public String getUserID() {
            return userID;
        }

        public String getTime() {
            return time;
        }

        public String getDate() {
            return date;
        }

        public boolean timeLessthan(UserIDDateWritable other, long mill) throws Exception {
            SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
            Date date1 = format.parse(time);
            Date date2 = format.parse(other.getTime());
            long difference = date2.getTime() - date1.getTime();
            return Math.abs(difference) < mill;
        }

        public int compareTo(UserIDDateWritable v) {
            return (userID+time).compareTo(v.getUserID()+v.getTime());
        }

        public int compareId(UserIDDateWritable v){
            return userID.compareTo(v.getUserID());
        }
    }

    public static class P2Mapper2 extends Mapper<LongWritable, Text, UserIDDateWritable, Text> {

        private static final Text emptyText = new Text("");

        @Override
        public void map(LongWritable ikey, Text ivalue, Context context)
                throws IOException, InterruptedException {
            String[] raw_data = ivalue.toString().split("\\s+");
            UserIDDateWritable key = new UserIDDateWritable(raw_data[0], raw_data[2], raw_data[1]);
            context.write(key, emptyText);
        }

    }

    public static class P2Reducer2 extends Reducer<UserIDDateWritable, Text, Text, Text> {
        private static ArrayList<UserIDDateWritable> ALLRECORD = new ArrayList<UserIDDateWritable>();
        private static final Text emptyText = new Text("");

        @Override
        protected void setup(Context context)
                throws IOException,
                InterruptedException {
            super.setup(context);
            if (ALLRECORD.size() > 0)
                return;
            Configuration conf = context.getConfiguration();
            Path tmpdata = new Path(DATA_OUTPUT_P2_Tmp + "/part-r-00000");
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(tmpdata)));
            String line;
            line = br.readLine();
            while (line != null) {
                String[] raw_data = line.toString().split("\\s+");
                UserIDDateWritable tmp = new UserIDDateWritable(raw_data[0], raw_data[2], raw_data[1]);
                ALLRECORD.add(tmp);
                line = br.readLine();
            }
        }

        @Override
        protected void reduce(UserIDDateWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (UserIDDateWritable v : ALLRECORD) {
                if (key.compareId(v) < 0) {
                    try {
                        if (key.timeLessthan(v, 2000)) {
                            String result = key.getDate() + " " + key.getTime() + "," + key.getUserID() + "," + v.getUserID();
                            context.write(new Text(result), emptyText);
                        }
                    } catch (Exception e) {

                    }
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("data.path", DATA_INPUT);


        Job job1 = Job.getInstance(conf);
        Job job2 = Job.getInstance(conf);

        job1.setJobName("MP1 P2 Phrase1");
        job2.setJobName("MP1 P2 Phrase2");

        Path tmpout = new Path(DATA_OUTPUT_P2_Tmp);
        Path out = new Path(DATA_OUTPUT_P2);
        FileSystem fs = out.getFileSystem(conf);

        if (fs.exists(tmpout))
            fs.delete(tmpout, true);
        if (fs.exists(out))
            fs.delete(out, true);

        //Job 1
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(P2Mapper1.class);
        job1.setReducerClass(P2Reducer1.class);
        job1.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job1.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(DATA_INPUT));
        FileOutputFormat.setOutputPath(job1, new Path(DATA_OUTPUT_P2_Tmp));
        job1.waitForCompletion(true);

        //Job 2
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(P2Mapper2.class);
        job2.setReducerClass(P2Reducer2.class);
        job2.setMapOutputKeyClass(UserIDDateWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job2.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(DATA_OUTPUT_P2_Tmp + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(DATA_OUTPUT_P2));
        job2.waitForCompletion(true);
    }
}
