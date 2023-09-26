package org.opendc.workflow.service.scheduler.task

import org.opendc.experiments.compute.topology.HostSpec
import org.opendc.simulator.compute.model.ProcessingUnit
import org.opendc.workflow.service.internal.TaskState
import java.util.*
import org.opendc.workflow.api.WORKFLOW_TASK_CORES
import org.opendc.workflow.service.internal.WorkflowServiceImpl

/**
 * One of the task scheduling policies in a DC environment.
 *
 * This class implements HEFT algorithm based ordering on the tasks read from a trace.
 * Extends TaskOrderPolicy class for allowing its execution.
 */

public class HEFTPolicy(private val hosts : Set<HostSpec>) : HolisticTaskOrderPolicy{ //HolisticTaskOrderPolicy
    /**
     * A set of tasks is transformed into a queue by applying HEFT algorithm.
     * @param tasks eligible tasks for scheduling
     */
    public override fun orderTasks(tasks: List<TaskState>) : Queue<TaskState>{
        val pendingTasks = tasks.toMutableSet()
        for (task in pendingTasks) {
            addUpwardRankToMetadata(task)
        }

        val sortedTasks = tasks.sortedByDescending { it.task.metadata["upward-rank"] as Double }

        // select the first task from sorted list
        // for-each processor(read 'host'), do:
        // compute the "Earliest Finish Time" (EFT) using the insertion-based scheduling policy
        // assign task to the host that minimises EFT of this selected task
        val tasksPerCore = hosts.flatMap { it.model.cpus }.map { it to mutableListOf<TaskState>() }.toMap()

        for (task in sortedTasks){
            var earliestFinishTime = Long.MAX_VALUE
            var scheduledStartTime = Long.MAX_VALUE
            var assignedHost: HostSpec? = null
            var assignedCore: ProcessingUnit? = null

            val requiredCores = task.task.metadata[WORKFLOW_TASK_CORES] as? Int ?: 1
            val potentialHosts = hosts.filter { it.model.cpus.count() >= requiredCores }

            for (host in potentialHosts) {
                // Execution time of task is same for each core of same host
                val executionTime = calculateExecutionTime(task, host)

                for (core in host.model.cpus) {
                    val scheduledTasks = tasksPerCore.getValue(core)
                    val (startTime, finishTime) = findPlaceInSchedule(executionTime, scheduledTasks)

                    if (finishTime < earliestFinishTime){
                        earliestFinishTime = finishTime
                        scheduledStartTime = startTime
                        assignedHost = host
                        assignedCore = core
                    }
                }
            }

            task.task.metadata["assigned-host"] = Pair(assignedHost!!.uid, assignedHost.name)
            task.task.metadata["finish-time"] = earliestFinishTime
            task.task.metadata["start-time"] = scheduledStartTime

            val scheduledTasks = tasksPerCore[assignedCore]!!
            insertTaskInSchedule(task, scheduledTasks)

            pendingTasks.remove(task)
        }

        return LinkedList(sortedTasks)
    }

    private fun calculateExecutionTime(task: TaskState, host: HostSpec): Long {
        val cpuCycles = task.task.metadata["cpu-cycles"] as Long
        val frequency = host.model.cpus.first().frequency
        return (cpuCycles / (frequency * 0.8)).toLong()
    }

    private fun findPlaceInSchedule(executionTime: Long, scheduledTasks: List<TaskState>): Pair<Long, Long> {
        // start checking for the best fit of the execution time slot of the current task on each CPUs of current host
        for (i in 1 until scheduledTasks.size){
            val prevFinishTime = scheduledTasks[i-1].task.metadata["finish-time"] as Long
            val nextStartTime = scheduledTasks[i].task.metadata["start-time"] as Long
            val gap = nextStartTime - prevFinishTime

            if (gap >= executionTime){ // first suitable CPU task-scheduling gap found
                return Pair(prevFinishTime, prevFinishTime + executionTime) // current task can start immediately after previous
            }
        }

        // if no suitable gap was found, execute the task after all other scheduled tasks
        val currStartTime = scheduledTasks.lastOrNull()?.let { it.task.metadata.getValue("finish-time") as Long } ?: 0L
        return Pair(currStartTime, currStartTime + executionTime)
    }

    private fun insertTaskInSchedule(task: TaskState, scheduledTasks: MutableList<TaskState>) {
        // for every CPU, ordered list of a task assigned in the sorted order of task scheduled
        val scheduledStartTime = task.task.metadata["start-time"]
        var insertIndex = 0
        for (i in scheduledTasks.indices){
            val prevFinishTime = scheduledTasks[i].task.metadata["finish-time"]
            if (prevFinishTime == scheduledStartTime) { // new task is executed immediately after its predecessor
                insertIndex = i + 1
                break
            }
        }
        scheduledTasks.add(insertIndex, task)
    }

    /**
     * Upward rank for each task would be calculated here
     * @param task task for which upward rank needs to be calculated
     */
    private fun addUpwardRankToMetadata(task: TaskState): Double {
        if (task.task.metadata.containsKey("upward-rank"))
            return task.task.metadata["upward-rank"] as Double

        val meanCommunicationCost = 0.0
        var maxChildRank: Double = 0.0

        for (childTask in task.dependents) {
            val childRank = addUpwardRankToMetadata(childTask)
            maxChildRank = maxOf(maxChildRank, childRank + meanCommunicationCost)
        }

        val upwardRank = getAverageComputationCost(task) + maxChildRank
        task.task.metadata["upward-rank"] = upwardRank
        return upwardRank
    }

    private fun getAverageComputationCost(task: TaskState) : Double {
        var totalComputationCost = 0.0
        var totalNumCores = 0

        val cpuCycles = task.task.metadata["cpu-cycles"] as Long

        val requiredCores: Int = task.task.metadata[WORKFLOW_TASK_CORES] as? Int ?: 1
        val potentialHosts = hosts.filter { it.model.cpus.count() >= requiredCores }
        for (host in potentialHosts){
            val numCores = host.model.cpus.size
            // Frequencies of cores of same host are equal
            val frequency = host.model.cpus.first().frequency
            totalComputationCost += (cpuCycles / frequency) * numCores
            totalNumCores += numCores
        }

        return totalComputationCost / totalNumCores
    }

    /**
     * Implementation/definition required for inheriting TaskOrderPolicy class
     */
    override fun invoke(scheduler: WorkflowServiceImpl): Comparator<TaskState> {
//        TODO("Would not be implemented!")
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
