# Streaming Frequent Items Estimation with PySpark

A streaming implementation of frequency estimation and heavy hitters algorithms (Sticky Sampling and Count-Min Sketch) built on Apache Spark Streaming to detect frequent items over a live network stream.

---

### Overview

Identifying heavy hitters (items with relative frequency >= phi) over fast, massive data streams is a fundamental problem where storing exact counts for all distinct elements becomes infeasible.

This project connects to a live TCP socket stream and compares two approximation techniques against the exact ground truth:
* Points/items arrive continuously over a network socket stream.
* The user specifies the target prefix length (`N`), frequency threshold (`PHI`), error tolerance (`EPSILON`), failure probability (`DELTA`), and Count-Min Sketch dimensions (`D`, `W`).
* The pipeline processes micro-batches in real-time, bounding space requirements via randomized sampling and hashing structures while evaluating accuracy against exact frequencies.

---

### Key Components

* **`process_batch`:** Micro-batch handler executed via `foreachRDD`. For each incoming element up to `N`, it incrementally updates:
  * Ground-truth exact counters (`true_counts`).
  * The Sticky Sampling frequency dictionary (`ss_counts`) using sampling probability `p = r / N`.
  * The Count-Min Sketch matrix (`cms_matrix`) across all `D` rows and adds items to `f_cm` if their frequency estimate reaches `PHI * N`.
* **`get_hash_value`:** Computes column indices using linear congruential hashing `((a * item + b) % p) % w` with row-specific coefficients.
* **Stopping Condition:** A thread-safe `threading.Event` triggered when the total stream length reaches `N`, gracefully halting the streaming context.

---

### Input Data Format

The script connects to a remote TCP socket stream (`algo.dei.unipd.it:<port>`). Each incoming line corresponds to a single integer token:

```text
42
108
42
7
```

### Requirements
* Python 3.8+
* Apache Spark (PySpark) 3.x

Install dependencies:
```bash
pip install pyspark
```

### Execution
Run the script via `spark-submit`:
```bash
spark-submit main.py <N> <PHI> <EPSILON> <DELTA> <D> <W> <portExp>
```


### Arguments

* **`N`**: Total number of items to process before stopping.
* **`PHI`**: Frequency threshold fraction for frequent items.
* **`EPSILON`**: Error tolerance parameter.
* **`DELTA`**: Failure probability.
* **`D`**: Number of rows (hash functions) in the Count-Min Sketch.
* **`W`**: Number of columns (buckets) in the Count-Min Sketch.
* **`portExp`**: TCP port number of the streaming server.

---

### Sample Output

```text
INPUT PARAMETERS: n = 1000000, phi = 0.01, epsilon = 0.002, delta = 0.05, d = 5, w = 1000, port = 8888

TRUE FREQUENT ITEMS:
Item = 42 True Freq = 15230
Item = 108 True Freq = 11045

STICKY SAMPLING:
Size of dictionary = 482
Item = 42 True Freq = 15230
Item = 108 True Freq = 11045

COUNT-MIN SKETCH:
Size of F_CM = 2
Item = 42 True Freq = 15230
Item = 108 True Freq = 11045
```
