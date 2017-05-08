import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Math;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

public class BasicStat {

  public static class  MyWritable implements Writable{
                          float partialSum ;
                          float partialSquareSum;
                          float partialMin;
                          float partialMax;
                          float partialCount;
       public MyWritable(float partialSumreceive,float partialCountreceive,float partialMinreceive, float partialMaxreceive, float partialSquareSumreceive){
                       partialSum = partialSumreceive;
                       partialSquareSum = partialSquareSumreceive;
                       partialCount = partialCountreceive;
                       partialMin = partialMinreceive;
                       partialMax = partialMaxreceive;
                                                 }
       public MyWritable(){
                       partialSum = 0;
                       partialSquareSum = 0;
                       partialMin = 0;
                       partialMax = 0;
                       partialCount = 0;
                                                 }

                        public  float get_partialSum()
                                     {
                           return partialSum;

                                     }
                        public  float get_partialCount()
                                     {
                           return partialCount;

                                     }
                       public  float get_partialMin()
                                     {
                           return partialMin;

                                     }
                       public  float get_partialMax()
                                     {
                           return partialMax;

                                     }
                       public  float get_partialSquareSum()
                                     {
                           return partialSquareSum;

                                     }


                        public void readFields(DataInput in) throws IOException {
                        partialSum = in.readFloat();
                        partialCount = in.readFloat();
                        partialMin = in.readFloat();
                        partialMax = in.readFloat();
                        partialSquareSum = in.readFloat(); 
                                          }
                        public void write(DataOutput out) throws IOException {
                        out.writeFloat(partialSum);
                        out.writeFloat(partialCount);
                        out.writeFloat(partialMin);
                        out.writeFloat(partialMax);
                        out.writeFloat(partialSquareSum);
                                                  }




                            }


  public static class Map 
              extends Mapper<LongWritable, Text, Text, MyWritable>{
    Text commonkey = new Text("commonkey"); 
    private Text word = new Text();    
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()); 
    float sum_partial = 0;    
    float sum_square = 0;
    float counter = 0;
    float min = 10000.999f;
    float max = -10000.999f;
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());    
        sum_partial += Float.parseFloat(word.toString());
        sum_square += Float.parseFloat(word.toString()) * Float.parseFloat(word.toString());
        counter += 1.0;
        if(Float.parseFloat(word.toString()) <= min)
                {
           min = Float.parseFloat(word.toString());
                }
        if(Float.parseFloat(word.toString()) >= max)
                {
           max = Float.parseFloat(word.toString());
                }
      }
        MyWritable f = new MyWritable(sum_partial,counter,min,max,sum_square);
        context.write(commonkey, f);

    }

  }
  
  public static class Reduce
       extends Reducer<Text,MyWritable,Text,FloatWritable> {

    private FloatWritable result = new FloatWritable();
    private FloatWritable min = new FloatWritable();
    private FloatWritable max = new FloatWritable();
    private FloatWritable avg = new FloatWritable();
    private FloatWritable stddev = new FloatWritable();

    private Text min_key = new Text("Min:");
    private Text max_key = new Text("Max:");
    private Text avg_key = new Text("Avg:");
    private Text std_dev = new Text("StdDev:");
    public void reduce(Text key, Iterable<MyWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0; 
      float counter = 0;
      float square_sum = 0;
      float current_min = 10000.99f; 
      float current_max = -10000.99f; 
     for (MyWritable val : values) {
        if(val.get_partialMin() <= current_min){
                              current_min = val.get_partialMin();
                                    }
        if(val.get_partialMax() >= current_max){
                              current_max = val.get_partialMax();
                                    }
         sum += val.get_partialSum();
         square_sum += val.get_partialSquareSum(); 
        counter += val.get_partialCount();
      }
      result.set(sum);
      float average = sum/counter;
      min.set(current_min);
      max.set(current_max);
      avg.set(average);
   
      float std_semi = (square_sum/counter) - (average * average);
      double std_dub = Math.sqrt(std_semi);
      float std = (float)std_dub;
      stddev.set(std);
      context.write(min_key, min);
      context.write(max_key, max);
      context.write(avg_key, avg);
      context.write(std_dev, stddev); 
    }
  }

  

  // Driver program
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(); 
//    conf.setInt(NLineInputFormat.LINES_PER_MAP,100);
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
    if (otherArgs.length != 2) {
      System.err.println("Usage: BasicStat <in> <out>");
      System.exit(2);
    }
     conf.setInt(NLineInputFormat.LINES_PER_MAP,100);
    // create a job with name "basicstat"
    Job job = new Job(conf, "basicstat");
    job.setInputFormatClass(TextInputFormat.class);
    job.setJarByClass(BasicStat.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
   
    // set output key type   
    job.setOutputKeyClass(Text.class);
    // set output value type
    job.setOutputValueClass(FloatWritable.class);
    //set map output key class
    job.setMapOutputKeyClass(Text.class);
    //set map output value class
    job.setMapOutputValueClass(MyWritable.class);

    //set the HDFS path of the input data
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    // set the HDFS path for the output
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    //Wait till job completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
