package org.opendc.oda.experimentrunner;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SchedulingAlgorithmComparatorExperimentRunner {
    public String runExperiment() throws ExecutionException, InterruptedException {
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//        Callable<String> callable = () -> {
            try{
                SchedulingAlgorithmComparatorExperiment exp = new SchedulingAlgorithmComparatorExperiment();
                exp.triggerExperiment();
                return "SUCCESS";
            }
            catch (Exception ex){
                ex.printStackTrace();
                return "FAILURE";
            }
//        };
//        Future<String> future = executorService.submit(callable);
//        return future.get();
    }
}
