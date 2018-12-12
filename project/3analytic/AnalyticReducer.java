import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalyticReducer
            extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        Double sumOfImm = 0.0;
        Double sumOfDeltaNumEstabs = 0.0;
        Double sumOfDeltaNumEmploys = 0.0;
        Double sumOfDeltaQ1Payroll = 0.0;
        Double sumOfDeltaAnnPayroll = 0.0;

        ArrayList<Text> cache = new ArrayList<Text>();

        for (Text v : values) {
            Text t = new Text();
            t.set(v);
            cache.add(t);

            String line = v.toString();
            String[] parts = line.split(",");
            count += 1;
            sumOfImm += Double.parseDouble(parts[12]);
            sumOfDeltaNumEstabs += (Double.parseDouble(parts[8]) - Double.parseDouble(parts[4]));
            sumOfDeltaNumEmploys += (Double.parseDouble(parts[9]) - Double.parseDouble(parts[5]));
            sumOfDeltaQ1Payroll += (Double.parseDouble(parts[10]) - Double.parseDouble(parts[6]));
            sumOfDeltaAnnPayroll += (Double.parseDouble(parts[11]) - Double.parseDouble(parts[7]));
        }

        int size = cache.size();

        Double meanImm = sumOfImm / count;
        Double meanEstab = sumOfDeltaNumEstabs / count;
        Double meanEmploys = sumOfDeltaNumEmploys / count;
        Double meanQ1Payroll = sumOfDeltaQ1Payroll / count;
        Double meanAnnPayroll = sumOfDeltaAnnPayroll / count;

        Double stdDevImm = 0.0;
        Double stdDevEstab = 0.0;
        Double stdDevEmp = 0.0;
        Double stdDevQ1 = 0.0;
        Double stdDevAnn = 0.0;

        Double pairSumEstab = 0.0;
        Double pairSumEmp = 0.0;
        Double pairSumQ1 = 0.0;
        Double pairSumAnn = 0.0;
        
    
        for (int i = 0; i < size; ++i) {
            String line = cache.get(i).toString();
            String[] parts = line.split(",");

            Double estabDiff = (Double.parseDouble(parts[8]) - Double.parseDouble(parts[4]));
            Double empDiff = (Double.parseDouble(parts[9]) - Double.parseDouble(parts[5]));
            Double q1Diff = (Double.parseDouble(parts[10]) - Double.parseDouble(parts[6]));
            Double annDiff = (Double.parseDouble(parts[11]) - Double.parseDouble(parts[7]));

            stdDevImm += Math.pow((Double.parseDouble(parts[12]) - meanImm), 2);
            stdDevEstab += Math.pow((estabDiff - meanEstab), 2);
            stdDevEmp += Math.pow((empDiff - meanEmploys), 2);
            stdDevQ1 += Math.pow((q1Diff - meanQ1Payroll), 2);
            stdDevAnn += Math.pow((annDiff - meanAnnPayroll), 2);

            pairSumEstab += (estabDiff - meanEstab) * (Double.parseDouble(parts[12]) - meanImm);
            pairSumEmp += (empDiff - meanEmploys) * (Double.parseDouble(parts[12]) - meanImm);
            pairSumQ1 += (q1Diff - meanQ1Payroll) * (Double.parseDouble(parts[12]) - meanImm);
            pairSumAnn += (annDiff - meanAnnPayroll) * (Double.parseDouble(parts[12]) - meanImm);
        }

        stdDevImm = Math.sqrt(stdDevImm / (count - 1));
        stdDevEstab = Math.sqrt(stdDevEstab / (count - 1));
        stdDevEmp = Math.sqrt(stdDevEmp / (count - 1));
        stdDevQ1 = Math.sqrt(stdDevQ1 / (count - 1));
        stdDevAnn = Math.sqrt(stdDevAnn / (count - 1));

        Double estabResult = (pairSumEstab / (stdDevImm * stdDevEstab)) / (count - 1);
        Double empResult = (pairSumEmp / (stdDevImm * stdDevEmp)) / (count - 1);
        Double q1Result = (pairSumQ1 / (stdDevImm * stdDevQ1)) / (count - 1);
        Double annResult = (pairSumAnn / (stdDevImm * stdDevAnn)) / (count - 1);

        String output = "Establishments=" + estabResult + 
            ", Employees=" + empResult +
            ", Q1 Payroll=" + q1Result +
            ", Annual Payroll=" + annResult;
        context.write(key, new Text(output));
    }
}
