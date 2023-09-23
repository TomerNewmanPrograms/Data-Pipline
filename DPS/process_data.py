import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

spark = SparkSession.builder.appName("WordCount").getOrCreate()
logger = logging.Logger(__name__)


async def process_message(msg: str):
    try:
        df = spark.createDataFrame([(msg,)], ["message"])

        # Split the message into words
        df = df.withColumn("words", split(df.message, " "))

        # Explode the words into separate rows
        df = df.withColumn("word", explode(df.words))

        # Count the occurrences of each word
        word_counts = df.groupBy("word").count()

        # Find the most common word
        most_common_word = word_counts.orderBy(word_counts["count"].desc()).first()["word"]

        # Get the total number of words
        total_words = df.count()
        logger.info(f"The most common word is '{most_common_word}' and the number of words is {total_words}.")

    except Exception as e:
        logger.error(f"Exception: {e} at message {msg}")


