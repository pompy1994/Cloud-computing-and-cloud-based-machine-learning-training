import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.*;

public class Task2 {

    public static class CSVInputFormat extends TextInputFormat {
        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new CSVRecordReader();
        }

        @Override
        protected boolean isSplitable(org.apache.hadoop.mapreduce.JobContext context, org.apache.hadoop.fs.Path file) {
            return false;
        }
    }

    public static class CSVRecordReader extends RecordReader<LongWritable, Text> {
        private LongWritable key;
        private Text value = new Text();
        private long start = 0;
        private long end = 0;
        private long pos = 0;
        private FSDataInputStream filein;
        private StringBuffer sb;
        

        @Override
        public void close() throws IOException {
            if (filein != null) {
                filein.close();
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float) (end - start));
            }
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            FileSplit split = (FileSplit) genericSplit;
            final Path file = split.getPath();
            Configuration conf = context.getConfiguration();
            FileSystem fs = file.getFileSystem(conf);
            start = split.getStart();
            end = start + split.getLength();
            this.filein = fs.open(split.getPath());
            System.out.println("start " + start);
            System.out.println("end " + end);

            this.filein.seek(start);
            this.pos = 0;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new LongWritable();
            }
            key.set(pos);
            this.pos++;
            if (value == null) {
                value = new Text();
            }
            value.clear();
            if (sb == null) {
                sb = new StringBuffer();
            }
            sb.setLength(0);
            boolean hasdata = false;
            try {
                int i;
                boolean inside = false;
                while ((i = this.filein.read()) != -1) {
                    hasdata = true;
                    char c = (char) i;
                    if (c == '"') {
                        if (inside == false) {
                            sb.append("NULL");
                        }
                        inside = !inside;
                    } else if (!inside) {
                        if (c == '\n') {
                            value.set(sb.toString());
                            sb.setLength(0);
                            inside = false;
                            return true;
                        } else {
                            sb.append(c);
                        }
                    }
                }
            } catch (IOException e) {
                System.out.println(e);
            }
            if (!hasdata) {
                key = null;
                value = null;
                return false;
            } else {
                return true;
            }
        }
    }

    public static class Task2Mapper extends Mapper<LongWritable, Text, Text, Text>{

        private Text output_key = new Text();
        private Text output_value = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) {
                // drop first line
                return;
            }
            String[] items = value.toString().split(",");
            if (items.length < 18) {
                return;
            }
            String video_id = items[0];
            // if (video_id.equals("_I_D_8Z4sJE")) {
            //     System.out.println("[DEBUG]  " + items[1] + " " + items[8]);
            // }
            String trending_date = items[1];
            try {
                trending_date = new SimpleDateFormat("yy.MM.dd").format(new SimpleDateFormat("yy.dd.MM").parse(trending_date));
            } catch (ParseException e) {
                return;
            }

            String country = items[17];
            output_key.set(video_id);
            output_value.set(trending_date + "-" + items[8] + "-" + country);
            context.write(output_key, output_value);
        }
    }

    public static class DateComparator implements Comparator<String> {
        @Override
        public int compare(String lhs, String rhs) {
            return lhs.split("-")[0].compareToIgnoreCase(rhs.split("-")[0]);
        }
    }

    public static class CountryComparator implements Comparator<String> {
        @Override
        public int compare(String lhs, String rhs) {
            return lhs.split("-")[2].compareToIgnoreCase(rhs.split("-")[2]);
        }
    }

    public static class Task2Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> allInput = new ArrayList<>();
            for (Text val : values) {
                allInput.add(val.toString());
            }
            if (allInput.size() < 2) {
                return;
            }
            allInput.sort(new CountryComparator());

            // System.out.println(String.join(", ", allInput));

            ArrayList<ArrayList<String>> splitItems = new ArrayList<>();
            String country = allInput.get(0).split("-")[2];
            int i = 0, j;
            for (j = 0; j < allInput.size(); j++) {
                if (!allInput.get(j).split("-")[2].equals(country)) {
                    splitItems.add(new ArrayList<>(allInput.subList(i, j)));
                    country = allInput.get(j).split("-")[2];
                    i = j;
                }
            }
            if (i != j) {
                splitItems.add(new ArrayList<>(allInput.subList(i, j)));
            }

            for (ArrayList<String> items: splitItems) {
                if (items.size() >= 2) {
                    items.sort(new DateComparator());
                    String[] a = items.get(0).split("-");
                    String[] b = items.get(1).split("-");
                    if (a[0].equals(b[0])) {
                        // may have duplicate items
                        int k = 2;
                        while (a[0].equals(b[0]) && k < items.size()) {
                            b = items.get(k).split("-");
                            k++;
                        }
                        if (a[0].equals(b[0])) {
                            continue;
                        }
                    }
                    int first_count = Integer.parseInt(a[1]);
                    int second_count = Integer.parseInt(b[1]);
                    double increment = ((double)second_count / first_count * 100) - 100;
                    if (increment > 1000) {
                        String outputKey = a[2] + ";\t" + key.toString()+", ";
                        context.write(new Text(outputKey), new Text(String.format ("%.1f", increment)));
                    }
                }
            }
        }
    }

    public static class Pair implements WritableComparable<Pair> {
        public Text id;
        public Text country;
        public DoubleWritable increment;

        public Pair() {
            this.country = new Text();
            this.id = new Text();
            this.increment = new DoubleWritable();
        }

        public Pair(Text country, Text video_id, DoubleWritable increment) {
            this.country = country;
            this.id = video_id;
            this.increment = increment;
        }

        @Override
        public int compareTo(Pair rhs) {
            if (country.compareTo(rhs.country) == 0)
                return -(increment.compareTo(rhs.increment));
            else
                return (country.compareTo(rhs.country));
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.country.readFields(in);
            this.id.readFields(in);
            this.increment.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            this.country.write(out);
            this.id.write(out);
            this.increment.write(out);
        }

        @Override
        public int hashCode() {
            return (country.toString() + id.toString() + increment.toString()).hashCode();
        }
    }

    public static class SortMapper extends Mapper<Object, Text, Pair, DoubleWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split("\t");
            if (items.length != 3) {
                return;
            }
            DoubleWritable increment = new DoubleWritable(Double.valueOf(items[2]));
            context.write(new Pair(new Text(items[0]), new Text(items[1]), increment), increment);
        }
    }

    public static class SortReducer extends Reducer<Pair, DoubleWritable, Text, Text> {

        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable increment: values) {
                String outputKey = key.country.toString() + " " + key.id.toString();
                context.write(new Text(outputKey), new Text(String.valueOf(increment.get()) + "%"));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Task2");

        Path tempfile = new Path("tempfile-" + new Date().getTime());

        job1.setInputFormatClass(CSVInputFormat.class);
        job1.setJarByClass(Task2.class);
        job1.setMapperClass(Task2Mapper.class);
        job1.setReducerClass(Task2Reducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tempfile);

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Task2-sort");
        FileInputFormat.addInputPath(job2, tempfile);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.setJarByClass(Task2.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.waitForCompletion(true);

        System.exit(0);
    }
}