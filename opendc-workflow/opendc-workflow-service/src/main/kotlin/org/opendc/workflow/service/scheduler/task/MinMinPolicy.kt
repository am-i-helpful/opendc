package org.opendc.workflow.service.scheduler.task

import org.opendc.experiments.compute.topology.HostSpec
import org.opendc.workflow.service.internal.TaskState
import org.opendc.workflow.service.internal.WorkflowServiceImpl
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.workflow.api.WORKFLOW_TASK_CORES
import java.util.*

private data class ExecutionSpec(val task: TaskState, val host: HostSpec, val selectedCpus: List<ProcessingUnit>, val completionTime: Double)

public class MinMinPolicy(public val hosts : Set<HostSpec>) : TaskOrderPolicy {
    // A nested map of hosts, CPU cores, and the time a new task could start on this core.
    private val startTimes = hosts.associateBy(
        { host -> host },
        { host -> host.model.cpus.associateBy({ core -> core }, { 0.0 }).toMutableMap() })

    /**
     * A set of tasks is transformed into a queue by applying Min-Min.
     * @param tasks eligible tasks for scheduling
     */
    public fun orderTasks(tasks: Set<TaskState>): Queue<TaskState> {
        val unmappedTasks = tasks.toMutableSet()
        val orderedTasks = LinkedList<TaskState>()

        while (unmappedTasks.isNotEmpty()) {
            var exec: ExecutionSpec? = null
            var min = Double.MAX_VALUE

            for (task in unmappedTasks) {
                val result = getMinExecutionSpec(task, startTimes)
                if (result.completionTime < min) {
                    min = result.completionTime
                    exec = result
                }

                if (min == 0.0) {
                    break
                }
            }

            val (nextTask, selectedHost, selectedCores, completionTime) = exec!!

            for (core in selectedCores) {
                startTimes[selectedHost]!![core] = completionTime
            }

            nextTask.task.metadata["assigned-host"] = Pair(selectedHost.uid, selectedHost.name)
            unmappedTasks.remove(nextTask)

            orderedTasks.addLast(nextTask)
        }

        return orderedTasks
    }

    /**
     * For a given task the minimal execution time is found.
     * The host associated with this time is stored in the [startTimes] map.
     *
     * We assume that a machine only has the same kind of CPUs.
     * @param startTimes lists for each host the processing
     */
    private fun getMinExecutionSpec(taskState: TaskState, startTimes: Map<HostSpec, Map<ProcessingUnit, Double>>): ExecutionSpec {
        var min = Double.MAX_VALUE
        var selectedHost: HostSpec? = null
        var selectedCores: List<ProcessingUnit>? = null

        val requiredCores: Int = taskState.task.metadata[WORKFLOW_TASK_CORES] as? Int ?: 1
        // If we assume that a task can only run on the cores of exactly one CPU:
        // val potentialHosts = hosts.filter { it.model.cpus.groupBy { core -> core.node }.maxOf { entry -> entry.value.count() } >= requiredCores }
        val potentialHosts = hosts.filter { it.model.cpus.count() >= requiredCores }

        for (host in potentialHosts) {
            // Select the n cores that complete their last scheduled task first.
            // This only works because we assume all cores have the same clock rate.
            val minCoreCompletionTimes = startTimes[host]!!
                .toList()
                .sortedBy { it.second } // it.second = completion time of core
                .take(requiredCores)

            val startTime = minCoreCompletionTimes.maxOf { it.second }

            val execTime = calculateExecutionTime(taskState, requiredCores, host)
            val taskCompletionTime = startTime + execTime

            if (taskCompletionTime < min) {
                min = taskCompletionTime
                selectedHost = host
                selectedCores = minCoreCompletionTimes.map { it.first } // get CPU cores
            }
        }

        if (selectedHost == null || selectedCores == null) {
            throw Exception("No host or CPUs could be found")
        }

        return ExecutionSpec(taskState, selectedHost, selectedCores, min)
    }

    /**
     * Calculate the execution time for a task on a host.
     * The execution time is the time needed to execute the job on the machine
     * once it is running on it (if a task T waits until point in time A to run
     * on host H and then runs until time B, the execution time is B-A).
     */
    private fun calculateExecutionTime(task: TaskState, requiredCores: Int, host: HostSpec): Double {
        val cpuCycles = task.task.metadata["cpu-cycles"] as Long
        // Assumption: all cores of all CPUs of this host have same frequency.
        // Otherwise, the calculation would get way to complex. We would have to find a pair of
        // n cores. Let s be the time the last of these n cores becomes available (startTime).
        // Then the completion time is s + the maximum execution time of these cores. But the
        // new startTimes (availabilityTimes) depend on the actual clock rate of each core. Puh...
        val frequency = host.model.cpus.first().frequency
        return cpuCycles / (frequency * 0.8 * requiredCores)
    }

    override fun invoke(scheduler: WorkflowServiceImpl): Comparator<TaskState> {
        // TODO("This will never be implemented, because this is just a dirty hack to get our policy running without changing doSchedule too much ¯\\_(ツ)_/¯")
        // reference - https://stackoverflow.com/questions/55449443/using-comparator-in-kotlin
        return Comparator<TaskState> { a, b ->
            when {
                a == null && b == null -> return@Comparator 0
                a == null -> return@Comparator -1
                b == null -> return@Comparator 1

                else -> return@Comparator 0
            }
        }
    }

}
