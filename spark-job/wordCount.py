from pyspark.sql import SparkSession

def main(input_file):
    # Create a SparkSession
    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    # Read input data from a text file
    lines = spark.read.text(input_file).rdd.map(lambda x: x[0])

    # Split the lines into words and perform word count
    word_counts = lines.flatMap(lambda line: line.split(" ")).countByValue()

    # Print the word counts
    for word, count in word_counts.items():
        print(f"{word}: {count}")

    # Stop the SparkSession
    spark.stop()


