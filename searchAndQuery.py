import re
import sys
import os

if __name__ == "__main__":
    if sys.argv.__len__() < 3:
        exit("There's an error with the input parameters")

    input_directory = sys.argv[1]
    words = sys.argv[2:]
    # remove the replicated words
    words = list(set(words))
    # store the number of the input words
    lenInputWords = len(words)

    if not os.path.isdir(input_directory):
        exit("The first parameter should be a directory")

    # we have to search all the output file of hadoop application (es: part-r-00000, part-r-00001, ecc.)
    pattern = re.compile(r"part-r-\d{5}")
    input_contents = []
    for filename in sorted(os.listdir(input_directory)):
        if pattern.match(filename):
            path_input = os.path.join(input_directory, filename)
            with open(path_input, "r") as input_file:
                content = input_file.read()
                input_contents.append(content)
    
    # create a single input file (as a string) to search the words into it
    # the file_input contains all the words sorted! (important)
    file_input = "\n".join(input_contents)

    fileCount = {}

    file_lines = file_input.split("\n")
    for file_line in file_lines:
        data = file_line.split("\t")
        word = data[0]
        files = data[1:]

        # the word is found
        if word in words :
            # increments the count of the finding files for the target word
            for file in files:
                # get only the filename, removing the number of the occurrences
                file = file.split(":")[0]
                fileCount[file] = fileCount.get(file, 0) + 1

            words.remove(word)

        if len(words) == 0:
            break

    # check the files containing all the input words
    found = False
    for file, count in fileCount.items():
        if count == lenInputWords:
            print(file)
            found = True

    if not found:
        print("No file contains all input words")







