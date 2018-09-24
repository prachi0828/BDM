import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SpatialJoin {
    static final int subtractWidth = 20;
    static final int subtractHeight = 5;


    private static boolean iswithin(int x, int y, int[] range) {
        if (x >= range[0] && x <= range[0] + range[2] && y >= range[1] && y <= range[1] + range[3]) {
            return true;
        } else {
            return false;
        }
    }

    public static class JoinComparator extends WritableComparator {

        public JoinComparator() {
            super(ComputeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ComputeKey first = (ComputeKey) a;
            ComputeKey second = (ComputeKey) b;
            return first.getComputeKey().compareTo(second.getComputeKey());
        }
    }
    
   

    private static List<int[]> RectGenerator(int startx, int starty, int height, int width) {
        List<int[]> result = new ArrayList<int[]>();
        //Checking the validity
        if (startx - width < 0) width = startx;
        if (starty - height < 0) height = starty;
        int leftx = startx - width;
        int downy = starty - height;
        int right_w = startx % subtractWidth;
        int up_h = starty % subtractHeight;
        if (width-right_w>0) {
            if (height-up_h>0){
            result.add(new int[]{leftx, downy, width-right_w, height-up_h});
            result.add(new int[]{leftx, starty-up_h, width-right_w, up_h});
            result.add(new int[]{startx-right_w, downy, right_w, height-up_h});
            result.add(new int[]{startx-right_w, starty-up_h, right_w, up_h});
            }else{
            result.add(new int[]{leftx, downy, width-right_w, height});
            result.add(new int[]{startx-right_w, downy, right_w, height});
            }
        }else{
            if (height-up_h>0){
               result.add(new int[]{leftx, downy, width, height-up_h});
               result.add(new int[]{leftx, starty-up_h, width, up_h}); 
            } else {
                result.add(new int[]{leftx, downy, width, height});
            }
        
        }
        return result;
    }
 
    public static class RectMapper extends Mapper<Object, Text, ComputeKey, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int[] UserRange = new int[4];
                                    UserRange[0] = conf.getInt("Userbottomx", -1);
                                    UserRange[1] = conf.getInt("Userbottomy", -1);
                                    UserRange[2] = conf.getInt("Usertopx", Integer.MAX_VALUE);
                                    UserRange[3] = conf.getInt("Usertopy", Integer.MAX_VALUE);

                                    String[] tokens = value.toString().split(",");
                                    String ID = tokens[0];
                                    int startx = Integer.parseInt(tokens[1]);
                                    int starty = Integer.parseInt(tokens[2]);
                                    int width = Integer.parseInt(tokens[3]);
                                    int height = Integer.parseInt(tokens[4]);
            if (startx <= UserRange[2] && startx - width >= UserRange[0] && starty <= UserRange[3] && starty - height >= UserRange[1]) {
                
                List<int[]> Rects = RectGenerator(startx, starty, width, height);
                for (int[] r : Rects) {
                    // tmp =  ID,x0,y0,w0,h0
                    // ID = r1 or rN
                    String tmp = ID;
                    assert r.length == 4;
                    for (int i = 0; i < r.length; i++) {
                        tmp = tmp + "," + r[i];
                    }
                    // GridID is the No of grids
                    String GridID = r[0] / subtractWidth + "," + r[1] / subtractHeight;
                    //return <GridID,Rect flag> as output Key
                    ComputeKey recordKey = new ComputeKey(GridID, ComputeKey.RECT_RECORD.get());
                    // output = <GridID,Rect_Record>, <ID,x0,y0,w0,h0>
                    context.write(recordKey, new Text(tmp)); 
                }
            }
        }
    }
    
    public static class PointMapper extends Mapper<Object, Text, ComputeKey, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int[] UserRange = new int[4];
            List<Integer> Cx = new ArrayList<Integer>();
            List<Integer> Cy = new ArrayList<Integer>();
            UserRange[0] = conf.getInt("Userbottomx", -1);
            UserRange[1] = conf.getInt("Userbottomy", -1);
            UserRange[2] = conf.getInt("Usertopx", Integer.MAX_VALUE);
            UserRange[3] = conf.getInt("Usertopy", Integer.MAX_VALUE);
            String[] tokens = value.toString().split(",");
            int x = Integer.parseInt(tokens[0]);
            int y = Integer.parseInt(tokens[1]);
            if (x >= UserRange[0] && x <= UserRange[2] && y >= UserRange[1] && x <= UserRange[3]) {
                if (x % subtractWidth == 0) Cx.add(x / subtractWidth - 1);
                Cx.add(x / subtractWidth);
                if (y % subtractHeight == 0) Cy.add(y / subtractHeight - 1);
                Cy.add(y / subtractHeight);
                // if point x,y is on the grid corner, it will have 2 GridID
                for (int i : Cx) {
                    for (int j : Cy) {
                        String GridID = i + "," + j;
                        ComputeKey recordKey = new ComputeKey(GridID, ComputeKey.POINT_RECORD.get());
                        // output = <GridID,point_Record>, <x,y>
                        context.write(recordKey, new Text(x + "," + y));
                    }
                }
            }
        }
    }
    
    
    public static class JoinRecuder extends Reducer<ComputeKey, Text, NullWritable, Text> {

        public void reduce(ComputeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, int[]> map = new HashMap<String, int[]>();
            for (Text t : values) {
                String[] vals = t.toString().split(",");
                //when a rect comes, put into Rect_hashmap ~ ID:[x0,y0,w,d]
                // Rect always comes in early that point when they have same grid
                if (key.getFlag().equals(ComputeKey.RECT_RECORD)) {
                    int[] tmp = new int[4];
                    tmp[0] = Integer.parseInt(vals[1]);
                    tmp[1] = Integer.parseInt(vals[2]);
                    tmp[2] = Integer.parseInt(vals[3]);
                    tmp[3] = Integer.parseInt(vals[4]);
                    map.put(vals[0], tmp);
                } else {
                     //when a point comes, check every entry [x0,y0,w,d] in hashmap
                     // whether point(x,y) is in any entry of the hashmap
                     // output the ID and the point
                    int x = Integer.parseInt(vals[0]);
                    int y = Integer.parseInt(vals[1]);
                    for (Map.Entry<String, int[]> e : map.entrySet()) {
                        if (iswithin(x, y, e.getValue())) {
                            context.write(NullWritable.get(), new Text(e.getKey() + ",(" + vals[0] + "," + vals[1] + ")"));
                        }
                    }
                }
            }
        }
    }
    
   public static void main(String[] args) throws Exception {
        // TODO code application logic here
      
       Scanner s = new Scanner(System.in);
       Configuration conf = new Configuration();
       System.out.printf("input Userbottomx :\n");
       conf.set("Userbottomx", s.nextInt() + "");
       System.out.printf("input Userbottomy :\n");
       conf.set("Userbottomy", s.nextInt() + "");
       System.out.printf("input Usertopx :\n");
       conf.set("Usertopx", s.nextInt() + "");
       System.out.printf("input Usertopy :\n");
       conf.set("Usertopy", s.nextInt() + "");
       long start = System.currentTimeMillis();
       Job job = new Job(conf, "query 1");
       job.setJarByClass(SpatialJoin.class);
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       job.setMapOutputKeyClass(ComputeKey.class);
       job.setMapOutputValueClass(Text.class);
       MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RectMapper.class);
       MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, PointMapper.class);
       job.setReducerClass(JoinRecuder.class);

       job.setGroupingComparatorClass(JoinComparator.class);
       job.setNumReduceTasks(100);
       job.setOutputKeyClass(NullWritable.class);
       job.setOutputValueClass(Text.class);
       FileOutputFormat.setOutputPath(job, new Path(args[2]));
       job.waitForCompletion(true);
       long end = System.currentTimeMillis();
       System.out.println((end - start) / 1000.0);
        
    }
}