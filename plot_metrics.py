import pandas as pd
import matplotlib.pyplot as plt

# Load the results
df = pd.read_csv("scaling_results_XXXXX.csv") # Update with your filename
df['TotalTime'] = pd.to_numeric(df['TotalTime'], errors='coerce')
df = df.dropna(subset=['TotalTime'])

# 1. STRONG SCALING: Speedup & Efficiency
strong = df[df['ScalingType'] == 'Strong'].sort_values('TotalCores')
if not strong.empty:
    t1 = strong.iloc[0]['TotalTime']
    p1 = strong.iloc[0]['TotalCores']
    
    strong['IdealSpeedup'] = strong['TotalCores'] / p1
    strong['ActualSpeedup'] = t1 / strong['TotalTime']
    strong['Efficiency'] = strong['ActualSpeedup'] / strong['IdealSpeedup']

    plt.figure(figsize=(10, 5))
    plt.subplot(1, 2, 1)
    plt.plot(strong['TotalCores'], strong['ActualSpeedup'], 'o-', label='Actual Speedup')
    plt.plot(strong['TotalCores'], strong['IdealSpeedup'], '--', label='Ideal (Linear)')
    plt.title('Strong Scaling: Speedup')
    plt.xlabel('Total Cores')
    plt.ylabel('Speedup')
    plt.legend()

# 2. WEAK SCALING: Throughput/Efficiency
weak = df[df['ScalingType'] == 'Weak'].sort_values('TotalCores')
if not weak.empty:
    t1_weak = weak.iloc[0]['TotalTime']
    weak['WeakEfficiency'] = (t1_weak / weak['TotalTime']) * 100

    plt.subplot(1, 2, 2)
    plt.plot(weak['TotalCores'], weak['WeakEfficiency'], 's-', color='green')
    plt.title('Weak Scaling: Efficiency (%)')
    plt.xlabel('Total Cores (Scaling with Data)')
    plt.ylabel('Efficiency %')
    plt.ylim(0, 110)

plt.tight_layout()
plt.savefig('scaling_plots.png')
plt.show()

# 3. MEMORY STUDY: Spill vs GC
mem_study = df[df['ScalingType'] == 'MemoryStudy'].sort_values('MemoryGB')
if not mem_study.empty:
    fig, ax1 = plt.subplots()
    ax1.plot(mem_study['MemoryGB'], mem_study['TotalTime'], 'b-o', label='Exec Time')
    ax2 = ax1.twinx()
    ax2.bar(mem_study['MemoryGB'], mem_study['SpillDisk'], alpha=0.3, color='red', label='Disk Spill')
    plt.title('Memory Sensitivity: Time vs Disk Spill')
    plt.savefig('memory_study.png')