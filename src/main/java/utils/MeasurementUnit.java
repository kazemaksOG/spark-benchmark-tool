package utils;

import java.sql.Timestamp;
import java.util.HashMap;

public class MeasurementUnit {
    private HashMap<String, Timestamp> measurements;
    private HashMap<String, Long> results;
    public MeasurementUnit() {
        measurements = new HashMap<>();
        results = new HashMap<>();
    }

    public void startMeasurement(String measurementName) {
        Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
        results.put("start_time" +"-"+ measurementName, timeStamp.getTime());
        measurements.put(measurementName, timeStamp);
    }

    public void endMeasurement(String measurementName) {
        Timestamp timeStamp = measurements.get(measurementName);

        if (timeStamp != null) {
            results.put(measurementName, System.currentTimeMillis() - timeStamp.getTime());
            results.put("end_time"+"-" + measurementName, System.currentTimeMillis());
        } else {
            System.err.println("Measurement " + measurementName + " not found");
        }
    }

    public HashMap<String, Long> getResults() {
        return results;
    }

}
