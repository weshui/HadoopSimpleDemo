import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.Iterator;

/**
 * Problem 1 for Machine Problem 1 from CS511 Sp16
 * Created by Wesley on 3/1/16.
 */
public class P1 {
    //Config to your own Directories
    private final static String HDFS_ADDRESS = "hdfs://127.0.0.1:9000/user/Wesley";
    private final static String DATA_INPUT = HDFS_ADDRESS + "/cs511/mp1/data/cs511_data";
    private final static String DATA_OUTPUT_P1 = HDFS_ADDRESS + "/cs511/mp1/data/out.p1";
    private final static String DATA_OUTPUT_P2 = HDFS_ADDRESS + "/cs511/mp1/data/out.p2";


    public static class TripleWriteable implements Writable {

        private int[] data = new int[3];

        //Has to have default constructor
        public TripleWriteable() {
        }

        public TripleWriteable(int impressions, int click, int conversions) {
            data[0] = impressions;
            data[1] = click;
            data[2] = conversions;
        }

        public void write(DataOutput out)
                throws IOException {
            for (int num : data)
                out.writeInt(num);

        }

        public void readFields(DataInput in)
                throws IOException {
            for (int i = 0; i < 3; ++i)
                data[i] = in.readInt();
        }

        public int get(int i) {
            return data[i];
        }

        @Override
        public String toString() {
            return data[0] + "," + data[1] + "," + data[2];
        }

    }


    public static class P1Mapper extends Mapper<LongWritable, Text, Text, TripleWriteable> {

        @Override
        public void map(LongWritable ikey, Text ivalue, Context context)
                throws IOException, InterruptedException {
            String[] s = ivalue.toString().split(",");
            TripleWriteable v = new TripleWriteable(Integer.valueOf(s[2]),
                                                    Integer.valueOf(s[3]),
                                                    Integer.valueOf(s[4]));
            context.write(new Text(s[5]), v);
        }
    }

    public static class P1Reducer extends Reducer<Text, TripleWriteable, Text, Text> {

        private static final Text emptyText = new Text();

        @Override
        public void reduce(Text _key, Iterable<TripleWriteable> values,
                           Context context) throws IOException, InterruptedException {
            int impressionSum = 0;
            int clickSum = 0;
            int conversationSum = 0;
            Iterator<TripleWriteable> it = values.iterator();
            while (it.hasNext()) {
                TripleWriteable current = it.next();
                impressionSum += current.get(0);
                clickSum += current.get(1);
                conversationSum += current.get(2);
            }

            context.write(new Text(_key.toString() + "," +
                          new TripleWriteable(impressionSum, clickSum, conversationSum).toString()),
                          emptyText);

        }



    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("data.path", DATA_INPUT);


        Job job = Job.getInstance(conf);

        job.setJobName("MP1 P1");
        Path out = new Path(DATA_OUTPUT_P1);
        FileSystem fs = out.getFileSystem(conf);

        if (fs.exists(out))
            fs.delete(out, true);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TripleWriteable.class);

        job.setMapperClass(P1Mapper.class);
        job.setReducerClass(P1Reducer.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(DATA_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(DATA_OUTPUT_P1));

        job.waitForCompletion(true);
    }
}
