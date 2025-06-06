from pyspark import SparkConf, SparkContext
import sys
import re
import nltk
import os
from nltk.corpus import stopwords

# DATASET 2GB (file max 500KB)
        # partition di 1000 files --> 15 min (stage 0) 2.3 min (stage 1) --> 18.3 min [300060_0058]
        # partition di 500 files --> 14 min (stage 0) 3 min (stage 1) --> 17 min [300060_0059]
        # partition di 250 files --> 13 min (stage 0) 4 min (stage 1) --> 17 min [300060_00]    QUESTO
        # partition di 125 files -->  min (stage 0) min (stage 1) -->  min [300060_00]
        # HADOOOOOOOOOP -> 13 minuti e 10 sec [0060_0062]
        # NonParallel --> 32 minuti (19.8 files/s)

        # DATASET 1GB (file max 500KB)
        # partition di 1000 files --> 10 min (stage 0) 2 min (stage 1) --> 12 min [300060_0041]
        # partition di 500 files --> 8.6 min (stage 0) 1.7 min (stage 1) --> 10.1 min [300060_0043]
        # partition di 250 files --> 6.4 min (stage 0) 1.8 min (stage 1) --> 8.2 min [300060_0044]  QUESTO
        # partition di 125 files --> 6.1 min (stage 0) 2.5 min (stage 1) --> 8.6 min [300060_0045]

        # DATASET 300MB (file max 500KB)
        # partition di 1000 files --> 3.4 min (stage 0) 23 s (stage 1) --> 4.4 min [300060_0048]
        # partition di 500 files --> 3.1 min (stage 0)  33 s (stage 1) --> 3.7 min [300060_0049]
        # partition di 250 files --> 2 min (stage 0) 30 s (stage 1) --> 2.5 min [300060_0050]   QUESTO
        # partition di 125 files --> 1.9 min (stage 0) 34 s (stage 1) --> 2.4 min [300060_0051]

        # DATASET 100MB (file max 500KB)
        # partition di 1000 files --> 47 s (stage 0) 6 s (stage 1) --> 54 s [300060_0052] (2 task)
        # partition di 500 files --> 1.1 min (stage 0)  8 s (stage 1) --> 1.2 min  [300060_0053] (3 task)
        # partition di 250 files --> 37 s (stage 0) 9 s (stage 1) --> 47 s [300060_0055] (6 task)   QUESTO

        # DATASET 246KB
        # due partition da 3 files --> 6 s (stage 0) 1 s (stage 1) --> 7 s  [300060_0056] (2 task)      QUESTO

# 2 : 10 = x : 60      x = 120 / 10

# function that creates a contex of the application
def create_spark_context(app_name="invertedIndex"):
    conf = SparkConf().setAppName(app_name)
    return SparkContext(conf=conf)

# function that clean the text
def preprocessing(line):
    line = line.lower()
    # Remove the genitive saxon
    line = re.sub(r"'s\b", "", line)
    # remove all that is not characther
    line = re.sub(r"[^\w\s]", "", line.strip())
    # remove all multiple space
    line = re.sub(r"\s+", " ", line.strip())
    # tokenize the line
    tokens = line.split()
    # delete stop words
    filtered_tokens = [word for word in tokens if word not in stop_words]
    return filtered_tokens

def local_reduce(it):
    from collections import defaultdict
    counts = defaultdict(int)
    for k, v in it:
        counts[k] += v
    return counts.items()

nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

if __name__ == "__main__":
    # Lettura dei parametri di input
    if len(sys.argv) < 3:
        print("Use: spark-submit invertedIndex.py <input_file> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    # Inizializza il contesto Spark
    sc = create_spark_context()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    path = sc._jvm.org.apache.hadoop.fs.Path(input_dir)
    status = fs.listStatus(path)
    n_files = sum(1 for fileStatus in status if fileStatus.isFile())

    try:
        rdd = sc.wholeTextFiles(input_dir, minPartitions=(max(2, int(n_files / 250))))

        # x[0] = fileName
        # x[1] = text
        # [ ((il, file1), 1),  ((cane, file1), 1),  ((il, file1), 1) ]
        rdd_cleaned_words = rdd.flatMap(lambda x : [((word, os.path.basename(x[0])), 1) for word in preprocessing(x[1])])

        # [ ((il, file1), 1), ((il, file1), 1) ] -> ((il, file1), 2)
        # rdd_count_words = rdd_cleaned_words.reduceByKey(lambda x, y: x + y)
        rdd_count_words = rdd_cleaned_words.mapPartitions(local_reduce)

        # (il, file1), 2) -> (il, (file1:2))
        rdd_words_filecount = rdd_count_words.map(lambda x: (x[0][0], f"{x[0][1]}:{x[1]}"))

        # [((il, (file1:2)), (il, (file3:4))] -> (il, (file1:2, file3:4))
        # rdd_final = rdd_words_filecount.groupByKey().mapValues(lambda vals: " ".join(vals))
        rdd_final = rdd_words_filecount.reduceByKey(lambda a, b: a + " " + b)

        # save the result on the hdfs
        rdd_final.saveAsTextFile(output_dir)
        print(f"Word count saved in {output_dir}")

    except Exception as e:
        print(f"Error during execution: {e}")
    
    finally:
        sc.stop()
