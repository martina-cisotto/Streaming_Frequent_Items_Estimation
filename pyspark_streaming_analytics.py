from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random
import math


#INPUT PARAMETERS
N = -1
PHI = -1
EPSILON = -1
DELTA = -1
D = -1
W = -1

streamLength = [0]   #counter for the total number of items
true_counts = {}   #dictionary with the exact frequency of every item

#sticky sampling structures
ss_counts = {}   #dictionary with the sampled items and their frequency estimates
ss_prob = 0   #sampling probability p = r/n

#count-min sketch structures
cms_matrix = []   #sketch counters
cms_hash_a = []   #coefficients a
cms_hash_b = []   #coefficients b
f_cm = set()   #set of frequent items



def get_hash_value(item, row_index, w):
    #compute the column index for a item using hash functions
    p = 8191
    a = cms_hash_a[row_index]
    b = cms_hash_b[row_index]
    return ((a * item + b) % p) % w



#BATCH MANAGEMENT
def process_batch(time, batch):
    global streamLength, true_counts, ss_counts, cms_matrix, f_cm
    global N, PHI, ss_prob, stopping_condition, W, D

    if streamLength[0] >= N:
        return

    #collect all stream items
    batch_items = batch.collect()
    batch_size = len(batch_items)

    #if batch_size > 0:
    #    print("Batch size at time [{0}] is: {1}".format(time, batch_size))

    #iterate through each item in the batch
    for item_str in batch_items:
        if streamLength[0] >= N:
            break

        item = int(item_str)

        #update ground-truth frequencies
        true_counts[item] = true_counts.get(item, 0) + 1

        #update sticky sampling
        if item in ss_counts:
            #if the item is sampled, increase its counter
            ss_counts[item] += 1
        else:
            #if the item is new, sample it with probability p
            if random.random() <= ss_prob:
                ss_counts[item] = 1

        #update count min sketch
        #increase the counter in computed column for every row d 
        for j in range(D):
            col = get_hash_value(item, j, W)
            cms_matrix[j][col] += 1

        #compute current estimate of element
        current_estimate = min(cms_matrix[j][get_hash_value(item, j, W)] for j in range(D))
        if current_estimate >= (PHI * N):
            #add the item if its estimate reaches the frequent threshold
            f_cm.add(item)

        #increment global stream element counter
        streamLength[0] += 1

    #stop streaming once N elements are reached
    if streamLength[0] >= N:
        stopping_condition.set()




#MAIN
if __name__ == '__main__':
    if len(sys.argv) < 8:
        print("Missing input")
        sys.exit(1)

    N = int(sys.argv[1])
    PHI = float(sys.argv[2])
    EPSILON = float(sys.argv[3])
    DELTA = float(sys.argv[4])
    D = int(sys.argv[5])
    W = int(sys.argv[6])
    portExp = int(sys.argv[7])

    #compute parameters for sticky sampling
    r = math.log(1.0 / (DELTA * PHI)) / EPSILON   #sampling rate
    ss_prob = r / N   #probability p

    #initialize count min sketch matrix with zeros
    cms_matrix = [[0] * W for _ in range(D)]

    #coefficients for hash function
    p_hash = 8191
    for _ in range(D):
        cms_hash_a.append(random.randint(1, p_hash - 1))
        cms_hash_b.append(random.randint(0, p_hash - 1))

    #initialize spark and streaming context
    conf = SparkConf().setMaster("local[*]").setAppName("GxxHW2")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.1)
    ssc.sparkContext.setLogLevel("ERROR")

    stopping_condition = threading.Event()

    #connect to the data stream server
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))

    ssc.start()   #start the streaming computation
    stopping_condition.wait()
    ssc.stop(False, False)   #stop the streaming computation

    #compute ground_truth frequent items
    true_thresh = PHI * N
    exact_freq_items = [item for item, freq in true_counts.items() if freq >= true_thresh]
    exact_freq_items.sort()

    #compute sticky sampling output set F_SS
    ss_thresh = (PHI - EPSILON) * N
    f_ss = [item for item, count in ss_counts.items() if count >= ss_thresh]
    f_ss.sort()
    dict_size = len(ss_counts)

    #compute count min sketch output set F_CM
    sorted_f_cm = sorted(list(f_cm))
    f_cm_size = len(sorted_f_cm)



    print("INPUT PARAMETERS")
    print(f"n = {N}")
    print(f"phi = {PHI}")
    print(f"epsilon = {EPSILON}")
    print(f"delta = {DELTA}")
    print(f"d = {D}")
    print(f"w = {W}")
    print(f"port = {portExp}\n")

    print("TRUE FREQUENT ITEMS")
    for item in exact_freq_items:
        print(f"Item = {item} True Freq = {true_counts[item]}")
    print()

    print("STICKY SAMPLING")
    print(f"Size of dictionary = {dict_size}")
    for item in f_ss:
        print(f"Item = {item} True Freq = {true_counts.get(item, 0)}")
    print()

    print("COUNT-MIN SKETCH")
    print(f"Size of F_CM = {f_cm_size}")
    for item in sorted_f_cm:
        print(f"Item = {item} True Freq = {true_counts.get(item, 0)}")


