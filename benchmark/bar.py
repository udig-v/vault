import matplotlib.pyplot as plt
import pandas as pd

def strip_last(time): 
    if time[-2:] == "ms":
        time = float(time[:-2]) / 1000
        return time
    elif time[-1:] == "s":
        time = float(time[:-1])
        return time
    
machine_name = "torus"
k = 32
memory_limit = 16384

threads = [2, 4, 8, 16]
bar_width = 0.2

plt.figure(figsize=(12,6))

plt.xlabel("Number of threads")
plt.ylabel("Time (seconds)")
plt.title(f"Vault Time on {machine_name}, memory limit {memory_limit}MB with k={k}")
plt.xticks(ticks=list(range(len(threads))), labels=threads)
 
# Set up legend labels
plt.bar(0, 0, color="#cfe2f3ff", label="Gen&Flush")
plt.bar(0, 0, color="#e01e20", label="Sort")

for i, thread_num in enumerate(threads):
    csv_file = f"vault_csv/vault_{machine_name}_{k}_{thread_num}t.csv"
    df = pd.read_csv(csv_file)
    
    # df["Gen&Flush"] = df["Gen&Flush"].apply(strip_last)
    # df["Sort"] = df["Sort"].apply(strip_last)
            
    avg_gen_time = round(df["Gen&Flush"].mean(), 2)
    avg_write_time = round(df["Sort"].mean(), 2)
                
    # Adjusted positions
    plt.bar(i - bar_width / 2, avg_gen_time, width=bar_width, color="#cfe2f3ff", edgecolor="grey")
    plt.bar(i + bar_width / 2, avg_write_time, width=bar_width, color="#e01e20", edgecolor="grey")

    # Annotating bars
    plt.text(i - bar_width / 2 - 0.05, avg_gen_time + 0.05, f'{avg_gen_time}', ha="center", va="bottom")
    plt.text(i + bar_width / 2 + 0.05, avg_write_time + 0.05, f'{avg_write_time}', ha="center", va="bottom")

plt.legend()
plt.savefig(f"vault_plot/vault_{machine_name}_{k}_{memory_limit}.svg")