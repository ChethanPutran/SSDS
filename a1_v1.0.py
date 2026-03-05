'''
## DS256 - Scalable Systems for Data Science | Jan 2026
# Assignment 1: LLM Data Preprocessing Pipeline with Apache Spark

#### Posted on: 2026-02-07
#### Deadline: 2026-03-03 (For code submission), 2026-03-08 (For scalability/report)
#### Maximum Points: 100 (50 for code correctness, 50 for scalability/report)

### Changelog:
----
* v1: Initial release of questions.
* v2: Fixed validations; added detailed instructions.
* v3: Fixed import versions; updated record numbers and timings; updated instructions for steps 5b and 6; updated reference outputs for all steps.

### Common Instructions
----
* You must ONLY edit cells and regions within those cells that allow changes. **DO NOT MODIFY other cells**. This can cause the evaluation script to break and you **WILL** be penalized or get **ZERO** points.
* You MUST NOT use the string **###!@** anywhere in your code or comments. We will be using this special substring for pattern matching during evaluations.
* You may declare **all** your valid imports and user-defined functions in the cells that are provided. Otherwise, all parts of your answer must remain between the space provided for the solution to each question.
* You must only use transformations and actions over Spark DataFrames, Spark SQL, and Spark RDDs to solve the assignment (**Spark Core**). You **MUST NOT** use MLLib, etc, unless specified.
 <!-- * https://spark.apache.org/docs/latest/api/python/reference/pyspark.html#rdd-apis -->
* Most of your processing to solve the problem should be done **within Spark**. Minimal post processing may be done in the Python driver code.
* You **must not** use Numpy, Scipy, etc. within the **driver** code. But you may use standard Python libraries as part of lambda expressions or functions passed to Spark transformations and actions.
<!-- * You must not use the default statistics operation (RDD.stats()) available in the **Spark Numeric RDD**. -->
* Our evaluations will include **alternate input WARC files** that have the same format but different contents/sizes.
* We have provided **reference outputs** and the **number of output records** for the test inputs for the **small** input set. You can use these to verify the correctness of your solutions. We have also provided **reference execution time** taken by each step on **Colab**.
* The evaluation will be done by passing clean reference inputs to each problem, i.e., even if one of the steps fails, we will evaluate the subsequent steps on the reference output from the previous steps.
* You will get **bonus** marks if the *end-to-end pipeline* works correctly and also if your runtime is faster than the *reference execution time* we have provided.
* 50% of the assessment goes towards your scalability evaluation, plots and detailed report. You **must** complete and submit the code for correctness evaluation by the first deadline and spend time on the experiments, analysis of performance and the report for the second deadline.
* The report should include a detailed analysis of the strong and weak scaling behavior of the individual steps as well as the end-to-end pipeline supported by experimental evidence and plots.
* **NOTE (Trigger Warning):** *Part of the assignment involves filtering out banned URLs. Kindly take care when processing this data as it may have sensitive words that are (by definition) not polite and potentially upsetting.*
<br>

### **IMPORTANT:** Academic Integrity
----
 The assignment must be completed by yourself and your teammate without any assistance from others or from online sources, ChatGPT, Copilot, etc. Taking online help for standard API references or clearing simple doubts on Python/Spark is allowed. If **any cases of plagiarism are detected, you may get a failing grade in the course and/or be reported to the Institute for further action**. Please follow IISc's Policy on Academic Integrity, https://iisc.ac.in/about/student-corner/academic-integrity/ .

### Submission Guidelines
----

1. Copy the **LATEST** template notebook to your local Google drive. Change the name of the notebook to **assignment1_sol.ipynb**. Proceed to complete the assignment within colab.
2. Make edits only in cells and regions that are clearly marked for modification. **Do not change the template in any other way**. We will be automatically parsing relevant cells and functions during grading. If these instructions are not followed (e.g., even an extra space in an unauthorized part), you can get a **zero** for the Assignment.
3. After you've solved the questions, verify that the output that you generate passes the **validation check** of the data types, and matches the reference outputs that are provided for the sample inputs. Note that for **floats** (if relevant), only the first 3 digits of precision will be checked, but you should not make any changes to the default precision in your code. If the output data type is not valid as specified, you will get a zero for that problem.
4. Each of the problems should take no longer than 5x the reference time as given for our reference outputs. If it takes longer, the problem will not be evaluated.
5. When you are ready to submit the assignment, download the notebook as a **(.py)** file to your local machine. You can do so by going to **File > Download > Download (.py)** in your colab environment.
6. By the first deadline, upload the **pranjalnaman_a1.py** file to Moodle Assignment-1-Code, if your or your teammmate's IISc email address is pranjalnaman@iisc.ac.in. *Upload only 1 file per team!*
7. By the second deadline, upload **pranjalnaman_a1.pdf** to Moodle Assignment-1-Report.

### Overview
----
This assignment implements a complete **LLM Data Preprocessing Pipeline** using Apache Spark on a YARN cluster. The pipeline processes raw Common Crawl WARC data through 5 steps to produce clean, tokenized training data suitable for Large Language Model training.

### Pipeline Steps
| Step | Name | Description |
|-------|------|-------------|
| 1 | Warc to Parquet | Convert the warc files to parquet (5%) |
| 2 | Ingestion / Filter | Load WARC parquet files and filter blacklisted domains (15%) |
| 3 | Extraction | Extract clean text from HTML using Trafilatura (15%) |
| 4 | Language ID | Filter to English-only documents using FastText (15%) |
| 5 | Deduplication | Remove near-duplicate documents using MinHash LSH (30%) |
| 6 | Tokenization | Convert text to token sequences for model training (20%) |

### Common Instructions
----
* You must ONLY edit cells and regions within those cells that allow changes. **DO NOT MODIFY** cells marked with `DO NOT MODIFY`.
* All processing should be done **within Spark** using DataFrames, RDDs, and Spark SQL.
* Ensure your virtual environment Python is accessible to all YARN worker nodes.
'''

# ─────────────────────────────────────────────
# DO NOT MODIFY THIS SECTION (IMPORT STATEMENTS)
# ─────────────────────────────────────────────
import sys
import os
import subprocess
import time
import pyspark
from pyspark import SparkFiles
import fasttext
import hashlib

from urllib.parse import urlparse
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import Window
from pyspark.sql import SparkSession
# from google.colab import drive
from warcio.archiveiterator import ArchiveIterator
from pyspark.storagelevel import StorageLevel
# ─────────────────────────────────────────────
# DO NOT MODIFY THIS SECTION (IMPORT STATEMENTS)
# ─────────────────────────────────────────────

# Need to pass the input WARC directory as a command-line argument when running the script with spark-submit. 
# This allows us to test your code with different input sizes without modifying the code.
if len(sys.argv) != 2:
    print("Usage: spark-submit a1_v1.0.py <input_warc_dir>")
    sys.exit(1)
    
# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
BASE_DIR            = f"hdfs:////ds256_2026/{sys.argv[1]}"   # Change to small_4, small_8, small_16 for different input sizes
BLACKLIST_DOMAINS_DIR = "hdfs:////ds256_2026/blacklist/dest"
FASTTEXT_MODEL_BIN  = "/home/ds256_2026/lid.176.bin" 
PARQUET_DIR         = "hdfs:////user/chethan1/outputs" # Change to your own HDFS directory
# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

# ──────────────────────────────────────────────
# DO NOT MODIFY THIS SECTION (SparkSession)
# ──────────────────────────────────────────────
spark = SparkSession.builder.appName("LLM_Preprocessing_Pipeline").getOrCreate()
sc = spark.sparkContext
# ──────────────────────────────────────────────
# DO NOT MODIFY THIS SECTION (SparkSession)
# ──────────────────────────────────────────────


# ──────────────────────────────────────────────
# DO NOT MODIFY THIS SECTION (Schema helpers)
# ──────────────────────────────────────────────
STEP_2_SCHEMA = {"warc_id": "string", "url": "string", "date": "string", "html_content": "string"}
STEP_3_SCHEMA = {"warc_id": "string", "url": "string", "date": "string", "extracted_text": "string"}
STEP_4_SCHEMA = STEP_3_SCHEMA
STEP_5_SCHEMA = STEP_3_SCHEMA
STEP_6_SCHEMA = {"warc_id": "string", "tokens": "array<int>", "attention_mask": "array<int>"}


def validate_schema(df, expected_columns):
    df_cols = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    missing = [col for col in expected_columns if col not in df_cols]
    if missing:
        raise ValueError(f"Schema validation failed. Missing columns: {missing}")


def validate_step_schema(df, step_num):
    schemas = {2: STEP_2_SCHEMA, 3: STEP_3_SCHEMA, 4: STEP_4_SCHEMA, 5: STEP_5_SCHEMA, 6: STEP_6_SCHEMA}
    validate_schema(df, schemas.get(step_num, {}))
    print(f"✓ STEP {step_num} schema validation passed!")
# ──────────────────────────────────────────────
# DO NOT MODIFY THIS SECTION (Schema helpers)
# ──────────────────────────────────────────────


# ──────────────────────────────────────────────
# Step 1 - WARC to Parquet
# ──────────────────────────────────────────────
'''
## STEP 1: Convert the warc files to parquet ***(5 points)***
----

### Objective
Ingest raw WARC data from the HDFS file system, parse the records to extract HTML content, and save the result as a partitioned Parquet dataset.

The function returns nothing. It just writes the parquet files to OUTPUT_DIR.

### Reference execution time on Colab & Output Record Count on *Small* Dataset
#### Time: `80 seconds` for 4 executors (8 cores, 10GB memory per executor)
#### # of records: `43,334 records`

### Guidelines
1. List Files: Retrieve the list of .warc files from the BASE_DIR directory.
2. Stream each WARC file.
3. Parse records using warcio.ArchiveIterator.
4. Filter for records where rec_type is 'response' and Content-Type contains 'html'.
5. Write to Parquet: Save the DataFrame to OUTPUT_DIR in Parquet format, partitioned by the original WARC filename.

***The DataFrame should have the output schema defined below.***
### Parquet DataFrame Schema
| Column | Type | Description |
|--------|------|-------------|
| warc_id | string | Unique WARC record identifier |
| url | string | Source URL |
| date | string | Crawl timestamp |
| html_content | string | Raw HTML content |
| warc_filename | string | The name of the source WARC file (used for partitioning)
'''


#######################################
###!@1 START ANSWER STEP 1

### Q1 ###################################################
from pyspark.sql.types import StructType, StructField, StringType,BooleanType, ArrayType, IntegerType
import trafilatura
from time import time
import zlib
from pyspark.ml.feature import Tokenizer, NGram, HashingTF
from pyspark.ml import Pipeline
from pyspark.sql.functions import (
    array, udf, col, slice, lit, size, when, array_repeat,regexp_extract,
    monotonically_increasing_id,length,split,explode,concat_ws, broadcast,
    concat, when,size,trim, sequence, lower,transform,  md5
)
import re


HDFS_BIN = "/usr/local/hadoop/bin/hdfs"

# def get_warc_files(base_dir):
#     # Using the absolute path here too for consistency
#     cmd = f"{HDFS_BIN} dfs -ls -C {base_dir}"
#     try:
#         output = subprocess.check_output(cmd, shell=True).decode('utf-8')
#         # Return the FULL HDFS path so we don't have to join them later
#         full_paths = [line.strip() for line in output.splitlines() if line.endswith('.warc')]
#         return full_paths
#     except Exception as e:
#         print(f"Error listing HDFS: {e}")
#         return []

def get_warc_files(base_dir):
    # -C flag gives just the paths, which is perfect
    HDFS_BIN = "/usr/local/hadoop/bin/hdfs"
    cmd = f"{HDFS_BIN} dfs -ls -C {base_dir}/*.warc"
    try:
        output = subprocess.check_output(cmd, shell=True).decode('utf-8')
        # Return the full HDFS paths directly
        return [line.strip() for line in output.splitlines() if line.strip()]
    except Exception as e:
        print(f"Error listing HDFS: {e}")
        return []
    


def process_warc_file(file_path):
    """
    Worker function: Opens a WARC file from HDFS path and yields records.
    """
    from warcio.archiveiterator import ArchiveIterator
    import os
    import subprocess
    
    filename = os.path.basename(file_path)
    
    # Using the absolute path found on the master
    HDFS_BIN = "/usr/local/hadoop/bin/hdfs"
    
    cmd = [HDFS_BIN, "dfs", "-cat", file_path]
    
    # Start the subprocess
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        bufsize=1024*1024,
        stderr=subprocess.PIPE  # Capture stderr to debug 'Permission Denied' if it occurs
    )
    try:
    
        # ArchiveIterator reads directly from the process pipe (streaming)
        for record in ArchiveIterator(process.stdout):
            # We only want 'response' records (not requests or metadata)
            if record.rec_type == 'response':

                # We only want HTML content
                content_type = record.http_headers.get_header('Content-Type', '').lower()
                if 'html' in content_type:

                    # Extraction
                    warc_id = record.rec_headers.get_header('WARC-Record-ID')
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    date = record.rec_headers.get_header('WARC-Date')

                    # Read raw bytes and decode to string (handle errors!)
                    content_bytes = record.content_stream().read()
                    try:
                        html_content = content_bytes.decode('utf-8')
                    except UnicodeDecodeError:
                        # Try latin-1 fallback or skip
                        html_content = content_bytes.decode('latin-1', errors='replace')

                    # Yield the row
                    yield (warc_id, url, date, html_content, filename)

    except Exception as e:
            # Check stderr if the process failed
            _, stderr = process.communicate()
            print(f"Error processing {filename}: {e} | HDFS Stderr: {stderr.decode()}")
    finally:
        if process.stdout:
            process.stdout.close()
        process.terminate()

def step_1_warc_to_parquet():
    """
    Convert warc files at BASE_DIR and place the parquet file in PARQUET_DIR
    """
    ## start your edits here  =================
 
    # 1. Use SparkContext (RDD API) to bypass the 2GB DataFrame row limit
    # binaryFiles returns an RDD of (path, PortableDataStream)
    # This DOES NOT load the file into memory yet.
    warc_files = get_warc_files(BASE_DIR)

    # 2. Process using flatMap
    # The worker will stream the 3.9GB file record-by-record
    records_rdd = spark.sparkContext.parallelize(warc_files).flatMap(process_warc_file)

    # 3. Define the Schema``
    schema = StructType([
        StructField("warc_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("date", StringType(), True),
        StructField("html_content", StringType(), True),
        StructField("warc_filename", StringType(), True)
    ])

    # 4. Create DataFrame and Write
    df = spark.createDataFrame(records_rdd, schema)

    # 5. Repartition is CRITICAL here. 
    # One 3.9GB WARC file contains thousands of small HTML records.
    # If you don't repartition, Spark will try to write one giant 
    # parquet file from a single task, which will likely OOM.
    df.repartition(200).write.mode("overwrite") \
      .option("compression", "zstd") \
      .parquet(PARQUET_DIR)
    ## end your edits here  =================


###!@1 END ANSWER STEP 1



# ──────────────────────────────────────────────
# Step 2 - Ingestion / Filter
# ──────────────────────────────────────────────
'''
## STEP 2: Data Ingestion & Domain Filtering ***(15 points)***
----


### Objective
Load raw WARC data from parquet files and filter out records from blacklisted domains.

### Reference execution time on Colab & Output Record Count on *Small* Dataset
#### Time: `19 seconds` for 4 executors (8 cores, 10GB memory per executor)
#### # of records: `40,985 records`

### Input
- `input_path`: HDFS (Colab for this notebook) path to parquet files containing WARC records

### Processing Steps
1. **Construct Domain Blacklist**:
    - Recursively walk the `blacklist_root_dir`.
    - Identify files named `domains` or `urls`.
    - Read each file line-by-line, stripping whitespace.
    - Ignore lines starting with `#` (comments).
    - The aforementioned steps are guidelines. Ultimately we want the ouput in the format described below.
    - Output can look like - `{'altabu-db1.blogspot.hu',
 'biwaisms-putesdefoncees.blogspot.cz',
 'kamilla18.blogspot.jp',
 'batorsparadise.blogspot.pt', ...}`
2. Extract domain from URL using regex.
3. Drop the records where domain and/or subdomains matches blacklist.


### Output Schema
| Column | Type | Description |
|--------|------|-------------|
| warc_id | string | Unique WARC record identifier |
| url | string | Source URL |
| date | string | Crawl timestamp |
| html_content | string | Raw HTML content |
'''

#######################################
###!@2 START ANSWER STEP 2

### Q2 ###################################################

def load_blacklist_categories():
    """
    Reads domains from the blacklist directory structure.
    Returns a set of domains for fast lookup.
    """
    print("Loading blacklist categories...")
    blacklisted_domains = set()

    ## start your edits here  =================
    blacklisted_domains = spark.read.text(f"{BLACKLIST_DOMAINS_DIR}/**/{{domains,urls}}") \
        .select(lower(trim(col("value"))).alias("domain")) \
        .filter("domain != '' AND NOT domain LIKE '#%'") \
        .distinct() \
        .cache()
    ## end your edits here  =================

    return blacklisted_domains

def step_2_ingestion():
    """
    Load parquet files and filter out blacklisted domains.
    Returns DataFrame with columns: warc_id, url, date, html_content
    """

    blacklisted_domains = load_blacklist_categories()

    ## start your edits here  =================
    # Define the schema explicitly
    schema = StructType([
        StructField("warc_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("date", StringType(), True),
        StructField("html_content", StringType(), True),
        StructField("warc_filename", StringType(), True)
    ])

    # Load Parquet using the schema  and select only necessary columns to save I/O overhead
    df = spark.read.schema(schema).parquet(PARQUET_DIR).select("warc_id", "url", "date", "html_content")

    # 1. Extract Host using native Spark SQL regex (Executes at C++/JVM speed)
    df_with_host = df.withColumn("host", regexp_extract(col("url"), r'https?://([^/:]+)', 1))

    # 2. Split host into parts: 'sub.example.com' -> ['sub', 'example', 'com']
    df_parts = df_with_host.withColumn("parts", split(col("host"), "\\."))

    # 3. Generate all suffixes natively: ['sub.example.com', 'example.com']
    df_suffixes = df_parts.withColumn(
    "suffixes",
    when(
        size(col("parts")) > 1,
        transform(
            sequence(lit(1), size(col("parts")) - 1),
            lambda i: concat_ws(
                ".",
                slice(col("parts"), i, size(col("parts")) - i + 1)
            )
        )
    ).otherwise(array())
    )
    # df_suffixes = df_parts.withColumn(
    #     "suffixes",
    #     transform(
    #         sequence(lit(1), size(col("parts")) - 1),
    #         lambda i: concat_ws(".", slice(col("parts"), i, size(col("parts"))))
    #     )
    # )

    # 4. Explode suffixes so each potential parent domain gets its own row
    df_exploded = df_suffixes.withColumn("match_domain", explode(col("suffixes")))

    # Repartition to distribute the exploded rows evenly across available CPU cores
    num_cores = spark.sparkContext.defaultParallelism
    df_exploded = df_exploded.repartition(num_cores, "match_domain")

    # 5. Identify blacklisted warc_ids via an inner join
    blacklisted_ids = df_exploded.join(
        broadcast(blacklisted_domains),
        df_exploded.match_domain == blacklisted_domains.domain,
        "inner"
    ).select("warc_id").distinct()

    # 6. Filter out the bad records using a highly optimized Broadcast Left-Anti Join
    output_df = df.join(broadcast(blacklisted_ids), "warc_id", "left_anti")

    ## end your edits here  =================

    return output_df

###!@2 END ANSWER STEP 2


# ──────────────────────────────────────────────
# Step 3 - Text Extraction
# ──────────────────────────────────────────────

'''
---
## Step 3: Text Extraction ***(15 points)***
----

### Objective
Extract clean, readable text content from raw HTML using the Trafilatura library.

### Reference execution time on Colab & Output Record Count on *Small* Dataset
#### Time: `168 seconds` for 4 executors (8 cores, 10GB memory per executor)
#### # of records: `39,984 records`

### Input
- DataFrame with `html_content` column (from Step 2)

### Processing Steps (Guidelines)
1. Apply `trafilatura` extraction. You can exclude comments, include tables, and have `no_fallback=False`
2. Drop records that cannot be processed by `trafilatura`.
3. It is possible that `trafilatura` returns nothing. Drop such records, i.e., only records with valid extracted text must be retained.


### Output Schema
| Column | Type | Description |
|--------|------|-------------|
| warc_id | string | Unique WARC record identifier |
| url | string | Source URL |
| date | string | Crawl timestamp |
| extracted_text | string | Clean text extracted from HTML |
'''

#######################################
###!@3 START ANSWER STEP 3

### Q3 ###################################################

def step_3_extraction(input_df):
    """
    Args:
        input_df: DataFrame with 'html_content' (string).
    Returns:
        DataFrame: With 'extracted_text' (string) replacing html_content.
    """
    ## start your edits here  =================
    def extract_partition(iterator):
        # Import inside the partition function so it loads locally on the executor node
        import trafilatura

        for row in iterator:
            html_str = row.html_content

            # Skip empty HTML
            if not html_str:
                continue

            try:
                # Extract text using trafilatura
                text = trafilatura.extract(
                    html_str,
                    include_comments=False,
                    include_tables=True,
                    no_fallback=False
                )

                # Only yield if extraction was successful and not empty whitespace
                if text and text.strip():
                    # Yield a tuple matching the required output schema
                    yield (row.warc_id, row.url, row.date, text)

            except Exception:
                # If trafilatura crashes on a malformed HTML string, skip the record
                continue

    # 1. Define the explicit output schema expected by Step 3
    output_schema = StructType([
        StructField("warc_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("date", StringType(), True),
        StructField("extracted_text", StringType(), True)
    ])

    # 2. Convert DataFrame to RDD, apply the partition-level mapping, and convert back
    # This completely bypasses the row-by-row UDF bottleneck
    rdd = input_df.rdd.mapPartitions(extract_partition)
    output_df = spark.createDataFrame(rdd, schema=output_schema)

    ## end your edits here  =================

    return output_df

###!@3 END ANSWER STEP 3


# ──────────────────────────────────────────────
# Step 4 - Language Identification
# ──────────────────────────────────────────────

'''
---
## STEP 4: Language Identification ***(15 points)***
----

### Objective
Filter documents to retain only English content using FastText language identification.

### Reference execution time on Colab & Output Record Count on *Small* Dataset
#### Time: `1.4 seconds` for 4 executors (8 cores, 10GB memory per executor)
#### # of records: `14,288 records`

### Input
- DataFrame with `extracted_text` column (from Step 3)
- `threshold`: Minimum probability for English classification (default: 0.6). We will test your code with different thresholds.

### Processing Steps
1. Predict language for each document (`Fasttext` requires single line inputs. Handle line breaks in extracted text.)
2. Filter to retain only documents classified as English (`__label__en`) with probability ≥ threshold

### Output Schema
Same as input (records that pass the English filter)
'''

#######################################
###!@4 START ANSWER STEP 4

### Q4 ###################################################

def step_4_lang_id(input_df, threshold=0.6):
    """
    Args:
        input_df: DataFrame with 'extracted_text' column.
        threshold: Minimum probability for English classification (default: 0.6).
    Returns:
        DataFrame: English-only documents.
    """

    ## start your edits here  =================
    bc_model_path = spark.sparkContext.broadcast(FASTTEXT_MODEL_BIN)
    bc_threshold = spark.sparkContext.broadcast(threshold)

    def is_english(text):
      if not text:
          return False
      try:
          import fasttext, warnings
          warnings.filterwarnings("ignore")

          # Cache the model at module level so each worker loads it only once
          global _ft_model
          if '_ft_model' not in globals() or _ft_model is None:
              _ft_model = fasttext.load_model(bc_model_path.value)

          single_line = text.replace('\n', ' ').replace('\r', ' ').strip()
          if not single_line:
              return False

          labels, probs = _ft_model.predict(single_line, k=1)
          return labels[0] == '__label__en' and float(probs[0]) >= bc_threshold.value
      except Exception:
          return False

    lang_udf = udf(is_english, BooleanType())

    output_df = input_df.filter(lang_udf(col("extracted_text")))

    ## end your edits here  =================

    return output_df

###!@4 END ANSWER STEP 4

# ──────────────────────────────────────────────
# Step 5 - Deduplication
# ──────────────────────────────────────────────

'''
---
## STEP 5: Deduplication ***(30 points)***
----

### Objective
Duplicate records can be detrimental to LLM training and therefore need to be dropped. 
Here, you will write 2 different deduplication algorithms, one approximate and one exact, and compare the two in your report.
'''

# ──────────────────────────────────────────────
# Step 5a - Deduplication (MinHash LSH)
# ──────────────────────────────────────────────

'''
---
## STEP 5a: Near-Duplicate Deduplication ***(15 points)***
----

### Objective
Remove near-duplicate documents using MinHash Locality-Sensitive Hashing (LSH).

### Reference execution time on Colab & Output Record Count on *Small* Dataset
#### Time: `11 seconds` for 4 executors (8 cores, 10GB memory per executor)
#### # of records: `12,808 records`

### Input
- DataFrame with `extracted_text` column (from Step 4)

### Guidelines
1. **Character 5-grams**: Convert text to character-level 5-grams, hash each to a vocabulary index
2. **MinHash Signatures**: Generate 24 MinHash signatures using Spark MLlib's MinHashLSH
3. **Banding**: Split signatures into 8 bands of 3 hashes each for similarity detection
4. **Clustering**: Find similar document pairs with Jaccard distance ≤ 0.5
5. **Filtering**: Keep only the longest document from each cluster

### Configuration
| Parameter | Value | Description |
|-----------|-------|-------------|
| NGRAM_SIZE | 5 | Character n-gram length |
| NUM_HASHES | 24 | Total MinHash signatures |
| NUM_BANDS | 8 | Number of LSH bands |
| VOCAB_SIZE | 2^18 | Hash space size (262,144) |

### Output Schema
Same as input (deduplicated records)
'''

#######################################
###!@5a START ANSWER STEP 5a

### Q5a ###################################################
def step_5a_deduplication(input_df):
    """
    Args:
        input_df: English text dataframe.
    Returns:
        DataFrame: Deduplicated dataframe using MinHash LSH.
    """
    ## start your edits here  =================
    NGRAM_SIZE = 5
    NUM_HASHES = 24
    VOCAB_SIZE = 2 ** 18  # 262,144

    # 1. Add doc_id and compute text length natively in Spark C-engine
    # This avoids calculating length in Python and saves us from doing 2 joins later.
    df_base = input_df.withColumn("doc_id", monotonically_increasing_id()) \
                      .withColumn("text_len", length(col("extracted_text"))) \
                      .persist(StorageLevel.DISK_ONLY)

    # 2. Optimized UDF with Stable Hashing
    tokenizer = Tokenizer(
        inputCol="extracted_text",
        outputCol="tokens"
    )

    ngram = NGram(
        n=NGRAM_SIZE,
        inputCol="tokens",
        outputCol="ngrams"
    )

    hashingTF = HashingTF(
        inputCol="ngrams",
        outputCol="features",
        numFeatures=VOCAB_SIZE,
        binary=True
    )

    pipeline = Pipeline(stages=[tokenizer, ngram, hashingTF])
    pipeline_model = pipeline.fit(df_base)

    df_features = pipeline_model.transform(df_base) \
        .filter(size(col("ngrams")) > 0) \
        .select("doc_id", "text_len", "features")
    # 4. Fit and Transform
    mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=NUM_HASHES)
    model = mh.fit(df_features)

    # Persist ONLY the transformed dataframe, and use MEMORY_AND_DISK
    # since we query it twice for the self-join.
    df_hashed = model.transform(df_features).persist(StorageLevel.MEMORY_AND_DISK)

    # 5. Approximate Similarity Join
    # datasetA and datasetB structs automatically carry our 'text_len' column!
    similar_pairs = model.approxSimilarityJoin(
        df_hashed,
        df_hashed,
        threshold=0.5,
        distCol="distance"
    ).filter(col("datasetA.doc_id") < col("datasetB.doc_id"))

    # 6. Determine which document to remove directly from the structs
    # No extra joins required!
    to_remove = similar_pairs.withColumn(
        "remove_id",
        when(col("datasetA.text_len") >= col("datasetB.text_len"), col("datasetB.doc_id"))
         .otherwise(col("datasetA.doc_id"))
    ).select("remove_id").distinct()

    # 7. Final Anti-Join
    output_df = df_base.join(
        to_remove,
        df_base.doc_id == to_remove.remove_id,
        how="left_anti"
    ).select("warc_id", "url", "date", "extracted_text")

    # Cleanup memory
    df_hashed.unpersist()


    ## end your edits here  =================

    return output_df

###!@5a END ANSWER STEP 5a

# ──────────────────────────────────────────────
# Step 5b - Deduplication (MD5)
# ──────────────────────────────────────────────

'''
---
## STEP 5b: Exact Deduplication ***(15 points)***
----

### Objective
Remove duplicate documents using exact deduplication using an MD5 fingerprint.

### Reference execution time on Colab & Output Record Count on *Small* Dataset
#### Time: `0.9 seconds` for 4 executors (8 cores, 10GB memory per executor)
#### # of records: `12,949 records`

### Input
- DataFrame with `extracted_text` column (from Step 4)

### Algorithm
1. Calculate the MD5 hash for each record.
2. Retain only one record per hash group.
3. In case of multiple records per group, retain the record with the *earliest* date.


### Output Schema
Same as input (deduplicated records)
'''

#######################################
###!@5b START ANSWER STEP 5b

### Q5b ###################################################
def step_5b_deduplication(input_df):
    """
    Args:
        input_df: English text dataframe.
    Returns:
        DataFrame: Deduplicated dataframe.
    """

    ## start your edits here  =================
    df_with_hash = input_df.withColumn("fingerprint", md5(col("extracted_text")))

    # Retain only one record per hash group i.e dropDuplicates on the fingerprint column is an optimized shuffle-based operation
    output_df = df_with_hash.dropDuplicates(["fingerprint"]).drop("fingerprint")

    ## end your edits here  =================

    return output_df

###!@5b END ANSWER STEP 5b


# ──────────────────────────────────────────────
# Step 6 - Tokenization
# ──────────────────────────────────────────────

'''
---
## STEP 6: Tokenization ***(20 points)***
----

### Objective
Convert cleaned text into *fixed-length* token sequences suitable for LLM training.

### Reference execution time on Colab & Output Record Count on *Small* Dataset
#### Time: `0.9 seconds (for each of the LSH and MD5)` for 4 executors (8 cores, 10GB memory per executor)
#### # of records: `12,808 (LSH) or 12,949 (MD5) records`

### Input
- DataFrame with text column (`extracted_text`, `cleaned_text`, or `text`)

### Guidelines
1. Perform simple word-level tokenization using regex (alphanumeric word boundaries only, ignore punctuation).
2. If extracted_text is null, treat it as an empty string.
3. Each word must be converted into a token ID using the MD5 hashing algorithm from *hashlib* library.
4. As mentioned below, 0 is only for padding (ensure no token ID is 0 and no token ID >= VOCAB_SIZE)
5. If the number of tokens exceeds MAX_LENGTH, truncate the sequence to exactly MAX_LENGTH (pad if less, final length must always be MAX_LENGTH).
6. Generate an attention mask (1 for real token, 0 for padding)

### Tokenization Backends
| Backend | Description |
|---------|-------------|
| `regex` | Word-level tokenization using `\b\w+\b` pattern, hash-based IDs (default) |

### Parameters
| Parameter | Default | Description |
|-----------|---------|-------------|
| max_length | 512 | Maximum sequence length |
| vocab_size | 50000 | Hash space for token IDs |
| pad_id | 0 | Padding token ID |

### Output Schema
| Column | Type | Description |
|--------|------|-------------|
| warc_id | string | Unique WARC record identifier |
| tokens | array<int> | Token IDs (padded to max_length) |
| attention_mask | array<int> | 1 for real tokens, 0 for padding |
'''

#######################################
###!@6 START ANSWER STEP 6

### Q6 ###################################################
# Parameters
MAX_LENGTH = 512
VOCAB_SIZE = 50000
PAD_ID = 0
TOKEN_BACKEND = "regex"

def get_optimized_tokenizer():
    """
    Returns a closure-based UDF. For SentencePiece, it uses a singleton
    pattern to avoid reloading the model for every single row.
    """
    regex_pattern = re.compile(r'\b\w+\b')
    def tokenize(text):
        if not text: return []
        tokens = regex_pattern.findall(text.lower())
        # Faster hashing using built-in hash() with fixed seed logic
        return [(abs(hash(t)) % (VOCAB_SIZE - 1) + 1) for t in tokens][:MAX_LENGTH]
    return udf(tokenize, ArrayType(IntegerType()))
    
def step_6_tokenization(input_df):
    """
    Tokenize text. Stdlib-only (no external libs) or sentencepiece.
    Args:
        input_df: Cleaned text dataframe.
    Returns:
        DataFrame: Columns ['warc_id', 'tokens', 'attention_mask']
    """

    ## start your edits here  =================
    # 1. Initialize the specific tokenizer UDF
    token_udf = get_optimized_tokenizer()

    # 2. Tokenize and Truncate in one step within the UDF to save a JVM projection
    df = input_df.withColumn("truncated_ids", token_udf(col("extracted_text")))

    # 3. Calculate metrics for padding
    df = df.withColumn("curr_len", size(col("truncated_ids")))

    # Ensure we don't have nulls which break concat
    df = df.withColumn("pad_len", lit(MAX_LENGTH) - col("curr_len"))

    # 4. JVM-Native Padding & Masking (Zero UDF overhead here)
    # We use array_repeat and concat which are highly optimized in Spark 3.x
    df = df.withColumn("tokens",
        when(col("pad_len") > 0,
             concat(col("truncated_ids"), array_repeat(lit(PAD_ID), col("pad_len"))))
        .otherwise(col("truncated_ids"))
    )

    df = df.withColumn("attention_mask",
        when(col("pad_len") > 0,
             concat(array_repeat(lit(1), col("curr_len")), array_repeat(lit(0), col("pad_len"))))
        .otherwise(array_repeat(lit(1), MAX_LENGTH))
    )
    # 5. Final Selection and localCheckpoint
    # Final Selection
    output_df = df.select("warc_id", "tokens", "attention_mask")


    ## end your edits here  =================

    return output_df

###!@6 END ANSWER STEP 6


# ──────────────────────────────────────────────
# Main pipeline
# ──────────────────────────────────────────────
if __name__ == "__main__":

    print(spark.sparkContext.getConf().getAll())

    print(">>> Starting Pipeline Execution...")
    t0 = time()

    step_1_warc_to_parquet()
    
    t1 = time()
    print(f">>> Step 1 completed in {t1 - t0:.6f} seconds")
    df_s2 = step_2_ingestion().cache()
    validate_step_schema(df_s2, 2)
    print("@@@S2:", df_s2.count())
    print(f">>> Step 2 completed in {time() - t1:.6f} seconds")
    
    t2 = time()
    df_s3 = step_3_extraction(df_s2).cache()
    validate_step_schema(df_s3, 3)
    print("@@@S3:", df_s3.count())
    print(f">>> Step 3 completed in {time() - t2:.6f} seconds")

    t3 = time()
    df_s4 = step_4_lang_id(df_s3).cache()
    validate_step_schema(df_s4, 4)
    print("@@@S4:", df_s4.count())
    print(f">>> Step 4 completed in {time() - t3:.6f} seconds")


    t4 = time()
    df_s5a = step_5a_deduplication(df_s4).cache()
    validate_step_schema(df_s5a, 5)
    print("@@@S5a:", df_s5a.count())
    print(f">>> Step 5a completed in {time() - t4:.6f} seconds")


    t4 = time()
    df_s5b = step_5b_deduplication(df_s4).cache()
    validate_step_schema(df_s5b, 5)
    print("@@@S5b:", df_s5b.count())
    print(f">>> Step 5b completed in {time() - t4:.6f} seconds")

    t5 = time()
    df_final_a = step_6_tokenization(df_s5a)
    validate_step_schema(df_final_a, 6)
    print("@@@S6a:", df_final_a.count())
    print(f">>> Step 6a completed in {time() - t5:.6f} seconds")

    t6 = time()
    df_final_b = step_6_tokenization(df_s5b)
    validate_step_schema(df_final_b, 6)
    print("@@@S6b:", df_final_b.count())
    print(f">>> Step 6b completed in {time() - t6:.6f} seconds")

    spark.stop()
    t1 = time()
    print(f"Total execution time: {t1 - t0:.6f} seconds")
# ──────────────────────────────────────────────
# Main pipeline
# ──────────────────────────────────────────────



# ──────────────────────────────────────────────
# GUIDELINES TO RUN THE CODE
# ──────────────────────────────────────────────
'''
YARN Queue Details:
The cluster has 8 nodes with 72 vcores (36 physical cores * 2 vcores per physical core) per node, 128GiB memory per node.

Global Queue: ds256
 - Timeout: 2 hours
 - Max container size: 12GB, 8 vcores (4 cores)
 - Subqueues: 12, 1 per team (ds256.team1, ds256.team2, ...)
 - Each subqueue can have maximum of 2 in-flight applications, any more get killed
 - Each subqueue has a default capacity of 8.33% of the total cluster capacity, but can expand to a max of 25% if other subqueues are idle.
 - Configured queue mapping for all users. If someone tries to use another team's queue, the jobs get redirected to their own queue.


Spark Submission Details:
spark-submit --master yarn \ # DO NOT CHANGE THIS
    --deploy-mode client \ # DO NOT CHANGE THIS
    --num-executors <num_executors> \ # adjust this to your needs
    --executor-cores 8 \ # adjust this to your needs (8 is the max vcores (4 physical cores) per container, you can use fewer vcores if you want)
    --executor-memory 10g \ # adjust this to your needs (10g is the max, 2GB for Java heap and overhead combined reaches 12g per container, you can use less if you want)
    --conf spark.yarn.queue=root.ds256.team8 \ # adjust this to your team's queue (team1, team2, ...)
    --conf spark.pyspark.python=/opt/ds256_env/bin/python \ # DO NOT CHANGE THIS
    --conf spark.pyspark.driver.python=/opt/ds256_env/bin/python \ # DO NOT CHANGE THIS
    assignment.py
'''
# ──────────────────────────────────────────────
# GUIDELINES TO RUN THE CODE
# ──────────────────────────────────────────────