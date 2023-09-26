package org.opendc.workflow.service

import com.fasterxml.jackson.annotation.JsonProperty

data class NodeConfig(
    @JsonProperty("total-nodes-count") var totalNodesCount: Int,
    @JsonProperty("node-built") var nodeBuilt: String,
    @JsonProperty("cpu-config") var cpuConfig: CPUConfig,
    @JsonProperty("memory-config") var memoryConfig: MemoryConfig,
    @JsonProperty("node-storage-capacity-gb") var nodeStorageCapacity: Int,
    @JsonProperty("node-interconnect-bandwidth") var nodeInterconnectBandwidth: String,
    @JsonProperty("gpu-config") var gpuConfig: GPUConfig
)

data class CPUConfig(
    @JsonProperty("cpu-vendor") var cpuVendor: String,
    @JsonProperty("cpu-model") var cpuModel: String,
    @JsonProperty("cpu-architecture") var cpuArch: String,
    @JsonProperty("cpu-cores") var cpuCores: Int,
    @JsonProperty("cpu-base-frequency-mhz") var cpuBaseFrequency: Double,
    @JsonProperty("cpu-max-frequency-mhz") var cpuMaxFrequency: Double
)

data class MemoryConfig(
    @JsonProperty("memory-vendor") var memoryVendor: String,
    @JsonProperty("memory-model") var memoryModel: String,
    @JsonProperty("memory-speed") var memorySpeed: Double,
    @JsonProperty("memory-size-mb") var memorySize: Long
)

data class GPUConfig(
    @JsonProperty("gpu-model") var gpuModel: String,
    @JsonProperty("gpu-count") var gpuCount: Int
)
