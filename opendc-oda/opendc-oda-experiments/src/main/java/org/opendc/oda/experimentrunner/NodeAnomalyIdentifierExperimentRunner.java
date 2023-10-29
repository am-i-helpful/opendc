package org.opendc.oda.experimentrunner;

import java.util.concurrent.*;

public class NodeAnomalyIdentifierExperimentRunner {
    public String runExperiment() throws ExecutionException, InterruptedException {
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//        Callable<String> callable = () -> {
            try{
                NodeAnomalyIdentifierExperiment exp = new NodeAnomalyIdentifierExperiment();
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
