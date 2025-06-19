import os  
import sys  
from collections import defaultdict  
import re 
from lkt import StopwordList  

# Loading English stopwords using LKT library
stopwords = set(StopwordList(language="en").words())

# Function that scans the contents of the folder looking for .txt files (the inputs)
def analizza_cartella(inputFolder, outputFile):
    # Creating structure: dictionary of dictionaries
    occurrences = defaultdict(lambda: defaultdict(int))

    # Cycle through all files in the folder looking for .txt
    for fileName in os.listdir(inputFolder):
        if fileName.endswith(".txt"):
            # Full path to the file
            path_file = os.path.join(inputFolder, fileName)  
            try:
                # Opening files in reading mode and formatting words
                with open(path_file, "r", encoding="utf-8") as f:
                    # Lowercasing the text
                    text = f.read().lower()
                    # Deletion of characters (except apostrophes because the Saxon genitive must be eliminated)
                    text = re.sub(r"[^\w\s']", " ", text)
                    # Extracting words with regex
                    words = re.findall(r"\b\w+(?:'\w+)?\b", text)

                    for word in words:
                        # If it ends with 's, remove the Saxon genitive
                        if word.endswith("'s"):
                            word = word[:-2]
                        # Count the word if it is not a stopword (otherwise ignore it)
                        if word not in stopwords:
                            occurrences[word][fileName] += 1
            # If it fails to read a file for any reason, it says so
            except Exception as e:
                print(f"Error reading {fileName}: {e}")

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(outputFile), exist_ok=True)

    # Output file formatting:
    #    capra     file1:1     file3:4
    #    cavoli    file2:3     file3:1
    with open(outputFile, "w", encoding="utf-8") as out:
        # Sort words alphabetically
        for word in sorted(occurrences.keys()):
            fileRow = [f"{name}:{count}" for name, count in occurrences[word].items()]
            out.write(f"{word}\t" + "\t".join(fileRow) + "\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        # Message in case of incorrect parameter passing
        print("Error, use instead: python script.py <inputFolder> <outputFile.txt>")
        sys.exit(1)

    # First argument: input folder
    inputFolder = sys.argv[1]
    # Second argument: output file
    outputFile = sys.argv[2]

    # Function calling
    analizza_cartella(inputFolder, outputFile)
    print(f"Analysis completed. Result successfully saved to {outputFile}")
