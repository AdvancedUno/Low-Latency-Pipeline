#!/bin/bash


# Get the directory where this script lives
STREAMING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ask Python where the PyFlink lib directory is
PYFLINK_LIB=$(python -c "import pyflink, os; print(os.path.join(os.path.dirname(pyflink.__file__), 'lib'))")

echo "PyFlink lib dir: $PYFLINK_LIB"
echo "Streaming dir:   $STREAMING_DIR"
echo ""

# Declare the JARs and their download URLs
declare -A JARS=(
    ["flink-sql-connector-kafka-3.0.1-1.18.jar"]="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar"
    ["flink-connector-base-1.18.1.jar"]="https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/1.18.1/flink-connector-base-1.18.1.jar"
    ["flink-parquet-1.18.1.jar"]="https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.18.1/flink-parquet-1.18.1.jar"
    ["flink-s3-fs-hadoop-1.18.1.jar"]="https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar"
    ["parquet-hadoop-bundle-1.13.1.jar"]="https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop-bundle/1.13.1/parquet-hadoop-bundle-1.13.1.jar"
    ["hadoop-common-3.3.4.jar"]="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar"
    ["hadoop-mapreduce-client-core-3.3.4.jar"]="https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.4/hadoop-mapreduce-client-core-3.3.4.jar"
)

# 1. Download JARs if not already present
for filename in "${!JARS[@]}"; do
    dest="$STREAMING_DIR/$filename"
    if [ -f "$dest" ]; then
        echo "[SKIP] $filename already exists"
    else
        echo "[DOWNLOAD] $filename ..."
        curl -o "$dest" "${JARS[$filename]}"
        echo "[OK] Downloaded $filename"
    fi
done

echo ""

# 2. Copy all JARs into pyflink/lib/
for filename in "${!JARS[@]}"; do
    src="$STREAMING_DIR/$filename"
    dst="$PYFLINK_LIB/$filename"
    if [ -f "$dst" ]; then
        echo "[SKIP] $filename already in pyflink/lib"
    else
        cp "$src" "$dst"
        echo "[COPIED] $filename -> pyflink/lib"
    fi
done

echo ""
echo "Done! You can now run: python src/streaming/flink_arb_pipeline.py"