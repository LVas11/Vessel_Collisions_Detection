#!/bin/bash
set -e

DATA_GLOB="${DATA_GLOB:-/data/*.csv}"
OUTPUT_DIR="${OUTPUT_DIR:-/output}"

if [ "$#" -eq 0 ]; then
    set -- --data "$DATA_GLOB" --output "$OUTPUT_DIR"
fi

echo "============================================="
echo "  AIS Vessel Collision Detector"
echo "  Arguments: $@"
echo "============================================="

python3 /app/src/collision_detector.py "$@"

echo ""
echo "2/2 Running visualization..."
python3 /app/src/visualize.py --output "$OUTPUT_DIR"

echo ""
echo "Done. Results in $OUTPUT_DIR/"
ls -lh "$OUTPUT_DIR/"