"""
plot_graphs.py - generates the 3 graphs required for PA5

Fill in the measured times below after running each scenario,
then run:  python plot_graphs.py

Requires: pip install matplotlib
"""

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# ── Scenario 1: running time vs number of URLs (4 workers, batch=100) ──────────
urls_x   = [100, 1000, 5000, 10000, 20000]
time_y1  = [9.5, 21.6, 334.6, 907.0, 1179.5]
urls_pos = list(range(len(urls_x)))

#Scenario 2: running time vs number of workers (10000 URLs, batch=100)
workers_x = [2, 3, 4]
time_y2   = [599.6, 634.8, 907.0]

#Scenario 3: running time vs batch size (10000 URLs, 4 workers)
batch_x  = [100, 200, 500]
time_y3  = [907.0, 279.1, 458.0]


def make_graph(ax, x, y, xlabel, title, color, tick_labels=None):
    ax.plot(x, y, marker='o', color=color, linewidth=2, markersize=7)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel("Running Time (seconds)", fontsize=12)
    ax.set_title(title, fontsize=13, fontweight='bold')
    ax.xaxis.set_major_locator(ticker.FixedLocator(x))
    if tick_labels is not None:
        ax.set_xticklabels(tick_labels)
    ax.grid(True, linestyle='--', alpha=0.6)


fig, axes = plt.subplots(1, 3, figsize=(15, 5))

make_graph(axes[0], urls_pos,  time_y1, "Number of URLs",     "Running Time vs URLs\n(4 workers, batch=100)",     "steelblue", urls_x)
make_graph(axes[1], workers_x, time_y2, "Number of Workers",  "Running Time vs Workers\n(10000 URLs, batch=100)",  "seagreen")
make_graph(axes[2], batch_x,   time_y3, "Batch Size",         "Running Time vs Batch Size\n(10000 URLs, 4 workers)", "tomato")

plt.tight_layout(pad=2.0)
plt.savefig("pa5_graphs.png", dpi=150)
print("saved pa5_graphs.png")
plt.show()
