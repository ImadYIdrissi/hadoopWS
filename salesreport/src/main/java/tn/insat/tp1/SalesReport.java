package tn.insat.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesReport {
// Ecrire un Job Map Reduce permettant, a partir du fichier purchases initial, de determiner le total des ventes par magasin.
// La structure du fichier purchases.txt est de la forme suivante : date temps magasin produit count paiement

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sales report");
        job.setJarByClass(SalesReport.class);
        job.setMapperClass(TokenizerMapper_Sales.class);
        job.setReducerClass(FloatSumReducer.class);
        job.setCombinerClass(FloatSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
