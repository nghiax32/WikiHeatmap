#!/bin/sh
# Create the project's S3 buckets on SeaweedFS. Safe to run repeatedly.
#
# Runs inside the seaweedfs-init service on `docker compose up`, or by hand:
#     docker compose run --rm seaweedfs-init
set -eu

MASTER="${SEAWEEDFS_MASTER:-seaweedfs:9333}"

create_bucket() {
    # s3.bucket.create fails on an existing bucket, so check first.
    if echo "s3.bucket.list" | weed shell -master="$MASTER" | awk '{print $1}' | grep -qx "$1"; then
        echo "bucket $1 already exists"
    else
        echo "s3.bucket.create -name $1" | weed shell -master="$MASTER"
    fi
}

# Raw event archive (prefix raw/), later also Parquet from Spark.
create_bucket wikiheatmap
