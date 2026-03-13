import os, sys
import json
import math
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType, DoubleType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, CountVectorizerModel, IDF, PCA, NGram, SQLTransformer, Normalizer
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.functions import vector_to_array
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import DenseVector, SparseVector

# Force Spark to use this exact Python for both driver & workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

INPUT = sys.argv[1] if len(sys.argv) > 1 else "articles_tech.jsonl"
OUTPUT = sys.argv[2] if len(sys.argv) > 2 else "out/tech_clusters_3d.csv"
K = int(sys.argv[3]) if len(sys.argv) > 3 else 5
TOP_N = int(sys.argv[4]) if len(sys.argv) > 4 else 10

spark = (
    SparkSession.builder
    .appName("NewsTechClustering")
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", sys.executable)
    .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
    .getOrCreate()
)

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "32")

#Load
df = spark.read.json(INPUT)
df = df.select("title", "text", "url").dropna(subset=["text"]).dropDuplicates(["url"])
df = df.filter(F.length("text") >= 120)
doc_count = df.count()
# use a proportional floor for small corpora to avoid singleton/event clusters
min_df_param = max(4, int(math.ceil(0.02 * doc_count))) if doc_count < 300 else 0.01

# Clean obvious HTML & boilerplate before tokenization
df = (
    df
    .withColumn("text_clean", F.regexp_replace(F.col("text"), "<[^>]+>", " "))  # strip HTML tags
    .withColumn("text_clean", F.regexp_replace(F.col("text_clean"), "&[a-z]{2,6};", " "))  # HTML entities
    .withColumn("text_clean", F.regexp_replace(F.col("text_clean"), r"https?://\\S+", " "))  # URLs
    .withColumn("text_clean", F.regexp_replace(F.col("text_clean"), r"\n|\r", " "))  # newlines
    .withColumn("text_clean", F.regexp_replace(F.col("text_clean"), r"\[\+\d+\s+chars\]", " "))  # NewsAPI tail like [+123 chars]
    .withColumn("text_clean", F.regexp_replace(F.col("text_clean"), r"\s+", " "))  # collapse spaces
)

# Also clean the title; we'll weight title tokens higher in the vocabulary
df = (
    df
    .withColumn("title_clean", F.regexp_replace(F.col("title"), "<[^>]+>", " "))
    .withColumn("title_clean", F.regexp_replace(F.col("title_clean"), "&[a-z]{2,6};", " "))
    .withColumn("title_clean", F.regexp_replace(F.col("title_clean"), r"https?://\\S+", " "))
    .withColumn("title_clean", F.regexp_replace(F.col("title_clean"), r"\n|\r", " "))
    .withColumn("title_clean", F.regexp_replace(F.col("title_clean"), r"\[\+\d+\s+chars\]", " "))
    .withColumn("title_clean", F.regexp_replace(F.col("title_clean"), r"\s+", " "))
)

# NLP pipeline -> TF-IDF
# Tokenize on letters only so pure numbers/symbols are dropped; keep short tokens like 'ai'
tokenizer = RegexTokenizer(
    inputCol="text_clean",
    outputCol="words",
    gaps=True,
    pattern="[^A-Za-z]+",
    minTokenLength=2,
    toLowercase=True,
)

extra_stops = [
    "li","ul","amp","nbsp","mdash","ndash","ldquo","rdquo","quot","apos",
    "us","news","new","also","says","said","get","got","time","week","weeks","today",
    "one","first","make","change","life","key","takeaways","company","users","year","chars","zdnet",
    "every","best","features","whether","like","comes","winner","available","already","announced","sources","getting","build","business","media","social","feature","model","models","techcrunch","kerry","wan","world","even"
]
default_stops = StopWordsRemover.loadDefaultStopWords("english")
remover = StopWordsRemover(inputCol="words", outputCol="filtered", stopWords=default_stops + extra_stops)

title_tokenizer = RegexTokenizer(
    inputCol="title_clean",
    outputCol="title_words",
    gaps=True,
    pattern="[^A-Za-z]+",
    minTokenLength=2,
    toLowercase=True,
)
title_remover = StopWordsRemover(inputCol="title_words", outputCol="title_filtered", stopWords=default_stops + extra_stops)

 # Add bigrams and combine with unigrams (keeps counts; doesn't dedupe)
ngram2 = NGram(n=2, inputCol="filtered", outputCol="bigrams")
concat_tokens = SQLTransformer(statement="SELECT *, concat(filtered, bigrams, bigrams, bigrams, title_filtered, title_filtered) AS tokens_all FROM __THIS__")

cv = CountVectorizer(inputCol="tokens_all", outputCol="tf", vocabSize=50000, minDF=min_df_param)
idf = IDF(inputCol="tf", outputCol="features")
normalizer = Normalizer(inputCol="features", outputCol="norm_features")


# Fit PCA on a sample to avoid Java heap OOM; override with env PCA_SAMPLE_FRAC=0.3 etc.
PCA_SAMPLE_FRAC = float(os.getenv("PCA_SAMPLE_FRAC", "0.3"))

# Build features once (no KMeans/PCA yet)
base_stages = [tokenizer, remover, title_tokenizer, title_remover, ngram2, concat_tokens, cv, idf, normalizer]
base_pipeline = Pipeline(stages=base_stages)
base_model = base_pipeline.fit(df)
feat_df = base_model.transform(df).select("title", "url", "norm_features")

# Fit PCA on a sample to reduce memory pressure
min_pca_rows = 200
if PCA_SAMPLE_FRAC >= 0.999:
    pca_fit_df = feat_df
else:
    pca_fit_df = feat_df.sample(False, PCA_SAMPLE_FRAC, seed=42)
    if pca_fit_df.count() < min_pca_rows:
        pca_fit_df = feat_df
pca_model = PCA(k=3, inputCol="norm_features", outputCol="pca3").fit(pca_fit_df)

evaluator = ClusteringEvaluator(featuresCol="norm_features", predictionCol="cluster")

if K == 0:
    candidates = [4, 5, 6, 7, 8, 9, 10, 12]
    bestScore = None
    bestK = None
    bestKmModel = None
    bestPreds = None
    for k in candidates:
        km_try = KMeans(k=k, seed=42, featuresCol="norm_features", predictionCol="cluster")
        km_model_try = km_try.fit(feat_df)
        preds_try = km_model_try.transform(feat_df)
        score = evaluator.evaluate(preds_try)
        print(f"[silhouette] k={k}: {score:.4f}")
        if bestScore is None or score > bestScore:
            bestScore, bestK, bestKmModel, bestPreds = score, k, km_model_try, preds_try
    print(f"[chosen] k={bestK} (silhouette={bestScore:.4f})")
    km_model = bestKmModel
    out = pca_model.transform(bestPreds)
    model = None  # keep a placeholder var name so downstream references don't break
else:
    km = KMeans(k=K, seed=42, featuresCol="norm_features", predictionCol="cluster")
    km_model = km.fit(feat_df)
    preds = km_model.transform(feat_df)
    out = pca_model.transform(preds)
    model = None

out = out.select("title", "url", "cluster", "pca3", "norm_features")

# Show cluster sizes
print("\n=== Cluster sizes ===")
sizes_df = out.groupBy("cluster").count().orderBy(F.desc("count"))
sizes_df.show(truncate=False)

# Mark tiny clusters as outliers for downstream viz (configurable)
MIN_CLUSTER_SIZE = int(os.getenv("MIN_CLUSTER_SIZE", "3"))
small_ids = [int(r["cluster"]) for r in sizes_df.filter(F.col("count") < MIN_CLUSTER_SIZE).select("cluster").collect()]
out = out.withColumn("outlier", F.when(F.col("cluster").isin(small_ids), F.lit(1)).otherwise(F.lit(0)))

# Prepare 3D coords for Plotly (we'll write after we build labels)
coords = (
    out
    .withColumn("pca3_array", vector_to_array("pca3"))
    .select(
        "title", "url", "cluster", "outlier",
        F.col("pca3_array")[0].alias("x"),
        F.col("pca3_array")[1].alias("y"),
        F.col("pca3_array")[2].alias("z"),
    )
)

# ---- Topic terms per cluster (center‑based, no UDF) ----
# Get fitted models
cv_model = next(s for s in base_model.stages if isinstance(s, CountVectorizerModel))
# km_model already defined above
vocab = cv_model.vocabulary
centers = km_model.clusterCenters()

def top_terms_from_center(center_vec, top_n):
    vals = list(center_vec)  # handles DenseVector by iteration
    idxs = sorted(range(len(vals)), key=lambda i: vals[i], reverse=True)[:top_n]
    return [(i, vocab[i], float(vals[i])) for i in idxs]

print("\n=== Top terms per cluster (center‑based) ===")
labels_rows = []
for cid, cvec in enumerate(centers):
    rows_center = top_terms_from_center(cvec, TOP_N)
    print(f"\nCluster {cid}:")
    for rank, (idx, term, score) in enumerate(rows_center, start=1):
        print(f"  {rank:>2}. {term} (score={score:.3f})")
    label = " / ".join([term for _, term, _ in rows_center[:3]])
    labels_rows.append((cid, label))

labels_df = spark.createDataFrame(labels_rows, ["cluster", "cluster_label"])

# Show the labels in the console
print("\n=== Cluster labels ===")
for row in labels_df.orderBy("cluster").collect():
    print(f"Cluster {row['cluster']}: {row['cluster_label']}")

# ---- Sample titles per cluster (closest to centroid) ----
def _make_dist_udf(center_arr):
    def _dist(v):
        if v is None:
            return None
        # norm_features are L2-normalized; squared Euclidean = 2 - 2*dot
        if isinstance(v, SparseVector):
            dot = 0.0
            for i, val in zip(v.indices, v.values):
                dot += val * center_arr[i]
        else:
            dot = float(sum(a*b for a, b in zip(v, center_arr)))
        return float(2.0 - 2.0 * dot)
    return F.udf(_dist, DoubleType())

samples = None
for cid, cvec in enumerate(centers):
    center_arr = list(cvec)
    dist_udf = _make_dist_udf(center_arr)
    cdf = (out
           .filter(F.col("cluster") == cid)
           .withColumn("dist2", dist_udf(F.col("norm_features")))
           .select("cluster", "title", "url", "dist2")
           .orderBy(F.col("dist2").asc())
           .limit(5))
    samples = cdf if samples is None else samples.unionByName(cdf, allowMissingColumns=True)

if samples is not None:
    summary_df = (samples
                  .join(labels_df, on="cluster", how="left")
                  .select("cluster", "cluster_label", "dist2", "title", "url")
                  .orderBy("cluster", "dist2"))
    print("\n=== Sample titles per cluster (closest to centroid) ===")
    for r in summary_df.collect():
        print(f"[C{r['cluster']}] {r['cluster_label']} :: {r['dist2']:.4f} :: {r['title']} -> {r['url']}")

    # write a CSV folder alongside the main output
    summary_path = os.path.join(OUTPUT, "cluster_summary.csv")
    (summary_df
        .coalesce(1)
        .write.mode("overwrite").option("header", True).csv(summary_path))
    print(f"\nWrote cluster summary to {summary_path}")

# Join labels into coords and write the CSV
coords_labeled = coords.join(labels_df, on="cluster", how="left")
(coords_labeled
    .select("title", "url", "cluster", "cluster_label", "outlier", "x", "y", "z")
    .coalesce(1)
    .write.mode("overwrite").option("header", True).csv(OUTPUT)
)

# Write run metadata
try:
    # Determine chosen k and silhouette
    chosen_k = bestK if 'bestK' in locals() and bestK is not None else K
    try:
        sil_score = bestScore if 'bestScore' in locals() and bestScore is not None else evaluator.evaluate(out)
    except Exception:
        sil_score = None

    vocab_size = len(vocab)
    sizes = {int(r['cluster']): int(r['count']) for r in sizes_df.collect()}
    meta = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "chosen_k": int(chosen_k),
        "silhouette": (float(sil_score) if sil_score is not None else None),
        "vocab_size": int(vocab_size),
        "minDF": float(cv.getMinDF()),
        "cluster_sizes": sizes,
        "output_path": OUTPUT,
    }
    import os
    os.makedirs(OUTPUT, exist_ok=True)
    with open(os.path.join(OUTPUT, "metadata.json"), "w") as f:
        json.dump(meta, f, indent=2)
    print(f"\nWrote metadata to {os.path.join(OUTPUT, 'metadata.json')}")
except Exception as e:
    print(f"[warn] Could not write metadata.json: {e}")

print(f"\nWrote 3D coordinates with labels to {OUTPUT} (Spark CSV; one part file inside).")

spark.stop()