#!/bin/bash
# =============================================================
# setup_flink_jars.sh (Mac/Linux version)
# =============================================================

STREAMING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYFLINK_LIB=$(python -c "import pyflink, os; print(os.path.join(os.path.dirname(pyflink.__file__), 'lib'))")

echo "PyFlink lib dir: $PYFLINK_LIB"
echo "Streaming dir:   $STREAMING_DIR"
echo ""

# Use a standard array to ensure macOS Bash 3.2 compatibility
URLS=(
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar"
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/1.18.1/flink-connector-base-1.18.1.jar"
    "https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.18.1/flink-parquet-1.18.1.jar"
    "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar"
    "https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop-bundle/1.13.1/parquet-hadoop-bundle-1.13.1.jar"
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar"
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.4/hadoop-mapreduce-client-core-3.3.4.jar"
)

for url in "${URLS[@]}"; do
    # Extract the filename from the URL
    filename=$(basename "$url")
    dest="$STREAMING_DIR/$filename"
    
    # 1. Download
    if [ -f "$dest" ]; then
        echo "[SKIP] $filename already exists"
    else
        echo "[DOWNLOAD] $filename ..."
        curl -o "$dest" "$url"
        echo "[OK] Downloaded $filename"
    fi
    
    # 2. Copy to PyFlink
    dst="$PYFLINK_LIB/$filename"
    if [ -f "$dst" ]; then
        echo "[SKIP] $filename already in pyflink/lib"
    else
        cp "$dest" "$dst"
        echo "[COPIED] $filename -> pyflink/lib"
    fi
done

echo ""
echo "Done! You can now run: python src/streaming/flink_arb_pipeline.py"