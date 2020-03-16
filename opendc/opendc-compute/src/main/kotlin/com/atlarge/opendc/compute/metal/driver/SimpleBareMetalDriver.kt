/*
 * MIT License
 *
 * Copyright (c) 2020 atlarge-research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.atlarge.opendc.compute.metal.driver

import com.atlarge.odcsim.Domain
import com.atlarge.odcsim.signal.Signal
import com.atlarge.odcsim.simulationContext
import com.atlarge.opendc.compute.core.ProcessingUnit
import com.atlarge.opendc.compute.core.Server
import com.atlarge.opendc.compute.core.Flavor
import com.atlarge.opendc.compute.core.MemoryUnit
import com.atlarge.opendc.compute.core.ServerState
import com.atlarge.opendc.compute.core.execution.ServerManagementContext
import com.atlarge.opendc.compute.core.execution.ShutdownException
import com.atlarge.opendc.compute.core.execution.assertFailure
import com.atlarge.opendc.compute.core.image.EmptyImage
import com.atlarge.opendc.compute.core.image.Image
import com.atlarge.opendc.compute.metal.Node
import com.atlarge.opendc.compute.metal.NodeState
import com.atlarge.opendc.compute.metal.monitor.NodeMonitor
import com.atlarge.opendc.compute.metal.power.ConstantPowerModel
import com.atlarge.opendc.core.power.PowerModel
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import java.util.UUID
import kotlin.math.ceil
import kotlin.math.max
import kotlin.math.min
import kotlinx.coroutines.withContext
import java.lang.Exception

/**
 * A basic implementation of the [BareMetalDriver] that simulates an [Image] running on a bare-metal machine.
 *
 * @param domain The simulation domain the driver runs in.
 * @param uid The unique identifier of the machine.
 * @param name An optional name of the machine.
 * @param cpus The CPUs available to the bare metal machine.
 * @param memoryUnits The memory units in this machine.
 * @param powerModel The power model of this machine.
 */
public class SimpleBareMetalDriver(
    private val domain: Domain,
    uid: UUID,
    name: String,
    val cpus: List<ProcessingUnit>,
    val memoryUnits: List<MemoryUnit>,
    powerModel: PowerModel<SimpleBareMetalDriver> = ConstantPowerModel(0.0)
) : BareMetalDriver {
    /**
     * The monitor to use.
     */
    private lateinit var monitor: NodeMonitor

    /**
     * The machine state.
     */
    private var node: Node = Node(uid, name, mapOf("driver" to this), NodeState.SHUTOFF, EmptyImage, null)
        set(value) {
            if (field.state != value.state) {
                domain.launch {
                    monitor.onUpdate(value, field.state)
                }
            }

            if (field.server != null && value.server != null && field.server!!.state != value.server.state) {
                domain.launch {
                    monitor.onUpdate(value.server, field.server!!.state)
                }
            }

            field = value
        }

    /**
     * The flavor that corresponds to this machine.
     */
    private val flavor = Flavor(cpus.size, memoryUnits.map { it.size }.sum())

    /**
     * The current active server context.
     */
    private var serverContext: BareMetalServerContext? = null

    /**
     * The signal containing the load of the server.
     */
    private val usageSignal = Signal(0.0)

    override val usage: Flow<Double> = usageSignal

    override val powerDraw: Flow<Double> = powerModel(this)

    override suspend fun init(monitor: NodeMonitor): Node = withContext(domain.coroutineContext) {
        this@SimpleBareMetalDriver.monitor = monitor
        return@withContext node
    }

    override suspend fun start(): Node = withContext(domain.coroutineContext) {
        if (node.state != NodeState.SHUTOFF) {
            return@withContext node
        }

        val server = Server(
            UUID.randomUUID(),
            node.name,
            emptyMap(),
            flavor,
            node.image,
            ServerState.BUILD
        )

        server.serviceRegistry[BareMetalDriver.Key] = this@SimpleBareMetalDriver
        node = node.copy(state = NodeState.BOOT, server = server)
        serverContext = BareMetalServerContext()
        return@withContext node
    }

    override suspend fun stop(): Node = withContext(domain.coroutineContext) {
        if (node.state == NodeState.SHUTOFF) {
            return@withContext node
        }

        // We terminate the image running on the machine
        serverContext!!.cancel(fail = false)
        serverContext = null

        node = node.copy(state = NodeState.SHUTOFF, server = null)
        return@withContext node
    }

    override suspend fun reboot(): Node = withContext(domain.coroutineContext) {
        stop()
        start()
    }

    override suspend fun setImage(image: Image): Node = withContext(domain.coroutineContext) {
        node = node.copy(image = image)
        return@withContext node
    }

    override suspend fun refresh(): Node = withContext(domain.coroutineContext) { node }

    private inner class BareMetalServerContext : ServerManagementContext {
        private var finalized: Boolean = false

        override val cpus: List<ProcessingUnit> = this@SimpleBareMetalDriver.cpus

        override val server: Server
            get() = node.server!!

        private val job = domain.launch {
            init()
            try {
                server.image(this@BareMetalServerContext)
                exit()
            } catch (cause: Throwable) {
                exit(cause)
            }
        }

        /**
         * Cancel the image running on the machine.
         */
        suspend fun cancel(fail: Boolean) {
            if (fail)
                job.cancel(ShutdownException(cause = Exception("Random failure")))
            else
                job.cancel(ShutdownException())
            job.join()
        }

        override suspend fun init() {
            assert(!finalized) { "Machine is already finalized" }

            val server = server.copy(state = ServerState.ACTIVE)
            node = node.copy(state = NodeState.ACTIVE, server = server)
        }

        override suspend fun exit(cause: Throwable?) {
            finalized = true

            val serverState =
                if (cause == null || (cause is ShutdownException && cause.cause == null))
                    ServerState.SHUTOFF
                else
                    ServerState.ERROR
            val nodeState =
                if (cause == null || (cause is ShutdownException && cause.cause != null))
                    node.state
                else
                    NodeState.ERROR
            val server = server.copy(state = serverState)
            node = node.copy(state = nodeState, server = server)
        }

        private var flush: Job? = null

        override suspend fun run(burst: LongArray, limit: DoubleArray, deadline: Long) {
            require(burst.size == limit.size) { "Array dimensions do not match" }
            assert(!finalized) { "Server instance is already finalized" }

            // If run is called in at the same timestamp as the previous call, cancel the load flush
            flush?.cancel()
            flush = null

            val start = simulationContext.clock.millis()
            var duration = max(0, deadline - start)
            var totalUsage = 0.0

            // Determine the duration of the first CPU to finish
            for (i in 0 until min(cpus.size, burst.size)) {
                val cpu = cpus[i]
                val usage = min(limit[i], cpu.frequency)
                val cpuDuration = ceil(burst[i] / usage * 1000).toLong() // Convert from seconds to milliseconds

                totalUsage += usage / cpu.frequency

                if (cpuDuration != 0L) { // We only wait for processor cores with a non-zero burst
                    duration = min(duration, cpuDuration)
                }
            }

            usageSignal.value = totalUsage / cpus.size

            try {
                delay(duration)
            } catch (e: CancellationException) {
                // On non-failure cancellation, we compute and return the remaining burst
                e.assertFailure()
            }
            val end = simulationContext.clock.millis()

            // Flush the load if the do not receive a new run call for the same timestamp
            flush = domain.launch(job) {
                delay(1)
                usageSignal.value = 0.0
            }
            flush!!.invokeOnCompletion {
                flush = null
            }

            // Write back the remaining burst time
            for (i in 0 until min(cpus.size, burst.size)) {
                val usage = min(limit[i], cpus[i].frequency)
                val granted = ceil((end - start) / 1000.0 * usage).toLong()
                burst[i] = max(0, burst[i] - granted)
            }
        }
    }

    override val scope: CoroutineScope
        get() = domain

    override suspend fun fail() {
        withContext(domain.coroutineContext) {
            serverContext?.cancel(fail = true)
        }
    }
}
