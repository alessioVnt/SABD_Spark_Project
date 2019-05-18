package utilities;

import java.util.ArrayList;

public class MeasurementStatistics {

    //Method to compute query2 statistics
    public static ArrayList<Double> statisticsCompute(Iterable<Double> values){

        ArrayList<Double> results = new ArrayList<>();

        int occurrences = 0;
        Double sum = 0.0;

        Double max = 0.0;
        Double min = 0.0;

        //Compute Mean, Max and Min
        for (Double v : values) {
            if(occurrences == 0){
                max = v;
                min = v;
            }
            occurrences++;
            sum += v;
            if(v > max) max = v;
            if(v < min) min = v;
        }

        Double mean = sum/occurrences;

        //Compute Std dev
        sum = 0.0;
        for (Double v : values) {
            sum += Math.pow((v - mean), 2);
        }

        Double dev = Math.sqrt(sum/occurrences);

        //Add computed statistics in the results ArrayList
        results.add(mean);
        results.add(dev);
        results.add(max);
        results.add(min);

        return results;
    }
}
