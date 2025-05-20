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

    try:
        # read the files in the directory and split in some partitions maximum 128MB each
        rdd = sc.wholeTextFiles(input_dir)

        # x[0] = fileName
        # x[1] = text
        # [ ((il, file1), 1),  ((cane, file1), 1),  ((il, file1), 1) ]
        rdd_cleaned_words = rdd.flatMap(lambda x : [((word, os.path.basename(x[0])), 1) for word in preprocessing(x[1])])

        # [ ((il, file1), 1), ((il, file1), 1) ] -> ((il, file1), 2)
        rdd_count_words = rdd_cleaned_words.reduceByKey(lambda x, y: x + y)

        # (il, file1), 2) -> (il, (file1:2))
        rdd_words_filecount = rdd_count_words.map(lambda x: (x[0][0], f"{x[0][1]}:{x[1]}"))

        # [((il, file1), 2), ((il, file3), 4)] -> (il, (file1:2, file3:4))
        rdd_final = rdd_words_filecount.groupByKey().mapValues(lambda vals: " ".join(vals))
        rdd_final_sorted = rdd_final.sortByKey()

        # save the result on the hdfs
        rdd_final_sorted.saveAsTextFile(output_dir)
        print(f"✅ Word count saved in {output_dir}")

    except Exception as e:
        print(f"❌ Error during execution: {e}")
    
    finally:
        sc.stop()
