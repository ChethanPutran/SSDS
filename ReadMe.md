# Scaling Study on Apache Spark

## Project Overview

This project implements a comprehensive scaling study on Apache Spark to analyze performance characteristics under various configurations. The study focuses on three primary dimensions: strong scaling, weak scaling, and memory sensitivity analysis. The experiments are designed to provide quantitative insights into Spark's behavior with varying executor counts, data sizes, and memory allocations.

## Experiment Design

### Scaling Dimensions

#### Strong Scaling
Fixed dataset size with increasing executor count to measure how runtime decreases with additional resources.

- Dataset: 7.8 GB (small_2)
- Executors: 2, 4, 8, 16, 32
- Cores per executor: 4
- Memory per executor: 8 GB
- Expected behavior: Runtime decreases proportionally until resource contention occurs

#### Weak Scaling
Dataset size increases proportionally with executor count to maintain constant work per core.

- Executors: 2, 4, 8, 12, 16, 32
- Datasets: small_1 (3.9 GB) through small_16 (62.2 GB)
- Cores per executor: 4
- Memory per executor: 4 GB
- Expected behavior: Runtime remains constant in ideal scaling scenarios

#### Memory Sensitivity Analysis
Fixed executor count and dataset size with varying memory allocations to study memory-related performance impacts.

- Executors: 16
- Dataset: 31.0 GB (small_8)
- Memory values: 4, 6, 8, 10 GB
- Memory overhead: 1 GB for <=6 GB, 2 GB for >6 GB
- Expected behavior: Reduced spilling and GC overhead with increased memory until diminishing returns

## Methodology

### Data Description

The study uses parquet files of varying sizes:

| Dataset | Size | Replication Factor |
|---------|------|-------------------|
| small_1 | 3.9 GB | 11.7 GB |
| small_2 | 7.8 GB | 23.4 GB |
| small_4 | 15.7 GB | 47.0 GB |
| small_6 | 23.4 GB | 70.1 GB |
| small_8 | 31.0 GB | 92.9 GB |
| small_16 | 62.2 GB | 186.5 GB |

Note: Replication factor indicates HDFS replication for fault tolerance.

### Experimental Control

Each configuration is executed with multiple iterations (default: 3) to account for runtime variance. A 10-second cooldown period between runs ensures clean resource state and prevents interference between experiments.

## Implementation Details

### Core Components

1. **Experiment Orchestrator** (Bash script)
   - Manages experiment lifecycle
   - Handles resource allocation
   - Collects runtime metrics
   - Parses application logs

2. **Metrics Extraction** (Python)
   - Parses Spark event logs
   - Extracts detailed performance metrics
   - Calculates derived metrics (GC ratio, communication ratio, etc.)

### Collected Metrics

| Category | Metrics |
|----------|---------|
| Timing | Step-wise completion times, Total execution time, Application time |
| Resource | Parallelism, Peak heap memory, Memory spill, Disk spill |
| Performance | GC time and ratio, Shuffle read/write time, Communication time and ratio, Compute time and ratio |

### Spark Configuration

Core configuration parameters:
- Master: YARN
- Deploy mode: client
- Python environment: /opt/ds256_env/bin/python
- Queue: root.ds256.team8
- Event logging: Enabled with executor metrics

## Usage Instructions

### Prerequisites

1. Access to the DS256 cluster
2. Python environment with required dependencies
3. SSH access to master node (10.24.1.10)
4. HDFS access to data directories

### Setup

1. Copy the main script to the master node:
```bash
scp ./scalability_study/a1_v1.0.py chethan1@10.24.1.10:/scratch/chethan1/SSDS/
```

2. SSH into the master node:
```bash
ssh chethan1@10.24.1.10
```

3. Ensure data exists in HDFS at the specified paths

### Running Experiments

Execute the scaling study script:

```bash
./scripts/run_metrics.sh
```

The script automatically:
- Creates output directory for logs
- Generates timestamped result files
- Executes all three experiment types sequentially
- Performs multiple iterations per configuration

### Monitoring

During execution, you can monitor progress through:

1. **Live Terminal Output**: Watch step completion times and application IDs
2. **Spark Web UI** (via SSH tunnel): http://localhost:4040
3. **YARN ResourceManager**: http://10.24.1.10:8088
4. **Spark History Server**: http://10.24.1.10:18080

For local browser access to Spark UI:
```bash
ssh -L 8088:192.168.1.1:8088 chethan1@10.24.1.10
```

### Output Files

Results are stored in the `outputs/` directory:
- Log files: `outputs/{Type}_e{Execs}_m{Memory}_run{Run}.log`
- Consolidated results: `outputs/scaling_results_{timestamp}.csv`

## Results Analysis

### CSV Output Format

The results file contains detailed metrics for each run:

```
Type,Execs,Cores,TotalCores,MemGB,Data,AppID,S1,S2,S3,S4,S5a,S5b,S6a,S6b,TotalTime,AppTimeMs,ExecRunTimeMs,Parallelism,GCMs,GCRatio,ShuffleReadMs,ShuffleWriteMs,CommMs,CommRatio,ComputeMs,ComputeRatio,MemSpillMB,DiskSpillMB,PeakHeapMB,Run
```

### Expected Patterns

#### Strong Scaling Analysis
- Near-linear speedup for compute-intensive operations
- Plateau due to scheduling overhead or data skew
- Identify optimal resource configuration for fixed data size

#### Weak Scaling Analysis
- Constant runtime indicates good scalability
- Increasing runtime suggests communication overhead
- Evaluate partitioning strategy effectiveness

#### Memory Sensitivity Analysis
- Decreasing spill metrics with more memory
- GC overhead reduction up to optimal point
- Identify memory threshold for in-memory processing

## Troubleshooting

### Common Issues

1. **Container Killed (Exit Code 143)**
   - Check memory overhead configuration
   - Verify queue capacity with `yarn queue -status root.ds256.team8`

2. **Application Not Found in Logs**
   - Ensure event log directory exists in HDFS
   - Check spark.eventLog.enabled=true

3. **Slow Execution**
   - Monitor for excessive GC
   - Check for data skew in stages
   - Verify network bandwidth

### Resource Monitoring Commands

```bash
# Check cluster resources
yarn node -list -showDetails

# Check queue status
yarn queue -status root.ds256.team8

# View application logs
yarn logs -applicationId <application_id>

# List running applications
yarn application -list
```

## Contributing

When extending this study:

1. Add new experiment types in the main script
2. Include additional metrics in the extraction function
3. Document any configuration changes
4. Run multiple iterations for statistical significance
5. Validate results against expected scaling patterns

## References

- Spark Configuration Guide: https://spark.apache.org/docs/latest/configuration.html
- YARN Resource Management: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
- Performance Tuning Guide: https://spark.apache.org/docs/latest/tuning.html