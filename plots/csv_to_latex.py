import pandas as pd

# Convert each file
files = [
    ('table1_strong_performance.csv', 'Strong Scaling Performance'),
    ('table2_strong_breakdown.csv', 'Strong Scaling Breakdown'),
    ('table3_stage_strong_time_table.csv', 'Stage-wise Time Breakdown'),
    ('table4_weak_comparison.csv', 'Weak Scaling Comparison'),
    ('table5_memory_analysis.csv', 'Memory Analysis'),
    ('table6_best_configs.csv', 'Best Configurations'),
    ('table7_parallelism.csv', 'Parallelism Analysis'),
    ('table8_strong_spill.csv', 'Strong Spill Analysis'),
    ('table9_stage_weak_time_table.csv', 'Weak Stage-wise Time Breakdown'),
    ('table10_weak_shuffle_table.csv', 'Weak Shuffle Analysis')
]

for file, caption in files:
    df = pd.read_csv(f'report_tables/{file}')
    latex = df.to_latex(index=False, 
                        float_format="%.4f",
                        caption=caption,
                        label=f'tab:{file.replace(".csv", "")}')
    
    with open(f'tables/{file.replace(".csv", ".tex")}', 'w') as f:
        f.write(latex)
    print(f"Created tables/{file.replace('.csv', '.tex')}")