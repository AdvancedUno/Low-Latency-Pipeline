
$streaming_dir = $PSScriptRoot  # directory where this script lives
$pyflink_lib = python -c "import pyflink, os; print(os.path.join(os.path.dirname(pyflink.__file__), 'lib'))"

Write-Host "PyFlink lib dir: $pyflink_lib"
Write-Host "Streaming dir:   $streaming_dir"
Write-Host ""

#  1. Download JARs if not already present 
$jars = @{
    "flink-sql-connector-kafka-3.0.1-1.18.jar"  = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar"
    "flink-connector-base-1.18.1.jar"            = "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/1.18.1/flink-connector-base-1.18.1.jar"
    "flink-parquet-1.18.1.jar"                   = "https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.18.1/flink-parquet-1.18.1.jar"
    "flink-s3-fs-hadoop-1.18.1.jar"              = "https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/flink-s3-fs-hadoop-1.18.1.jar"
    "parquet-hadoop-bundle-1.13.1.jar"           = "https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop-bundle/1.13.1/parquet-hadoop-bundle-1.13.1.jar"
    "hadoop-common-3.3.4.jar"                    = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar"
}

foreach ($filename in $jars.Keys) {
    $dest = Join-Path $streaming_dir $filename
    if (Test-Path $dest) {
        Write-Host "[SKIP] $filename already exists"
    } else {
        Write-Host "[DOWNLOAD] $filename ..."
        Invoke-WebRequest -Uri $jars[$filename] -OutFile $dest
        Write-Host "[OK] Downloaded $filename"
    }
}

Write-Host ""

#  2. Copy all JARs into pyflink/lib/
foreach ($filename in $jars.Keys) {
    $src  = Join-Path $streaming_dir $filename
    $dst  = Join-Path $pyflink_lib   $filename
    if (Test-Path $dst) {
        Write-Host "[SKIP] $filename already in pyflink/lib"
    } else {
        Copy-Item $src $dst
        Write-Host "[COPIED] $filename -> pyflink/lib"
    }
}

Write-Host ""
Write-Host "Done! You can now run: python streaming/flink_arb_pipeline.py"