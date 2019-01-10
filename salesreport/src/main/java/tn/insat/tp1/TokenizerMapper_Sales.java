package tn.insat.tp1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper_Sales extends Mapper<Object, Text, Text, IntWritable> {
//    date temps magasin produit count paiement

    private Text date = new Text();
    private Text temps = new Text();
    private Text magasin = new Text();
    private Text produit = new Text();
    private FloatWritable count = new FloatWritable();
    private Text paiement = new Text();

    public void map(Object key, Text values, Mapper.Context context)
            throws IOException, InterruptedException, IllegalStateException{
        StringTokenizer itr = new StringTokenizer(values.toString(),"\t");
        while(itr.hasMoreTokens()){
            date.set(itr.nextToken());
            temps.set(itr.nextToken());
            magasin.set(itr.nextToken());
            produit.set(itr.nextToken());
            count.set(Float.valueOf(itr.nextToken()));
            paiement.set(itr.nextToken());

            context.write(magasin,count);
        }

    }
}
