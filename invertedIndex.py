from pyspark import SparkConf, SparkContext
import sys
import re
import nltk
import os
from nltk.corpus import stopwords

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

# function to do a local reducer to optimize the work
def local_reduce(it):
    from collections import defaultdict
    counts = defaultdict(int)
    for k, v in it:
        counts[k] += v
    return counts.items()

nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

if __name__ == "__main__":
    # reading of the input parameters
    if len(sys.argv) < 3:
        print("Use: spark-submit invertedIndex.py <input_file> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    # initialize the spark context
    sc = create_spark_context()
    # obtaining information regarding the number of file in the dataset
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    path = sc._jvm.org.apache.hadoop.fs.Path(input_dir)
    status = fs.listStatus(path)
    n_files = sum(1 for fileStatus in status if fileStatus.isFile())

    try:
        # split the dataset into partition of 750 files each
        # this number is choose in experimental way
        rdd = sc.wholeTextFiles(input_dir).coalesce(max(1, int(n_files / 750)))

        # x[0] = fileName
        # x[1] = text
        # [ ((il, file1), 1),  ((cane, file1), 1),  ((il, file1), 1) ]
        rdd_cleaned_words = rdd.flatMap(lambda x : [((word, os.path.basename(x[0])), 1) for word in preprocessing(x[1])])

        # [ ((il, file1), 1), ((il, file1), 1) ] -> ((il, file1), 2)
        # we are sure that each file are contains into only one partition
        # this avoid shuffle throught partitions
        rdd_count_words = rdd_cleaned_words.mapPartitions(local_reduce)

        # (il, file1), 2) -> (il, (file1:2))
        rdd_words_filecount = rdd_count_words.map(lambda x: (x[0][0], f"{x[0][1]}:{x[1]}"))

        # [((il, (file1:2)), (il, (file3:4))] -> (il, (file1:2, file3:4))
        # in this trasformation we have shuffle thought partitions
        rdd_final = rdd_words_filecount.groupByKey().mapValues(lambda vals: " ".join(vals))

        # save the result on the hdfs
        rdd_final.saveAsTextFile(output_dir)
        print(f"Word count saved in {output_dir}")
        
    except Exception as e:
        print(f"Error during execution: {e}")
    
    finally:
        sc.stop()
