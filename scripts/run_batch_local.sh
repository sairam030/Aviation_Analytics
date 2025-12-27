#!/bin/bash
#
# Script to run batch processing locally (outside Docker)
# Usage: ./scripts/run_batch_local.sh [--limit N]
#

set -e

# Default values
LIMIT=""
OUTPUT_DIR="/tmp/aviation"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --limit)
            LIMIT="$2"
            shift 2
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}Aviation Data Pipeline - Batch Processing${NC}"
echo -e "${GREEN}================================================${NC}"

# Check if data exists
DATA_PATH="/media/D/data/aviation_data"
if [ ! -d "$DATA_PATH" ]; then
    echo -e "${RED}Error: Data path not found: $DATA_PATH${NC}"
    exit 1
fi

echo -e "${YELLOW}Data path: $DATA_PATH${NC}"
echo -e "${YELLOW}Output dir: $OUTPUT_DIR${NC}"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Add project to PYTHONPATH
export PYTHONPATH="$PROJECT_DIR:$PYTHONPATH"

echo -e "\n${GREEN}Starting batch pipeline...${NC}"

# Run the pipeline
cd "$PROJECT_DIR"

if [ -n "$LIMIT" ]; then
    echo -e "${YELLOW}Processing limited to $LIMIT files${NC}"
    python -c "
from src.batch.batch_pipeline import AviationBatchPipeline

pipeline = AviationBatchPipeline(
    use_minio=False,
    local_output_base='$OUTPUT_DIR'
)

try:
    stats = pipeline.run_full_pipeline(
        process_states=True,
        create_mappings=True,
        enrich_data=True,
        create_analytics=True,
        states_limit=$LIMIT
    )
    print('Pipeline completed successfully!')
    print(f'Status: {stats}')
finally:
    pipeline.stop()
"
else
    python -m src.batch.batch_pipeline
fi

echo -e "\n${GREEN}================================================${NC}"
echo -e "${GREEN}Batch processing complete!${NC}"
echo -e "${GREEN}Output location: $OUTPUT_DIR${NC}"
echo -e "${GREEN}================================================${NC}"

# Show output summary
echo -e "\n${YELLOW}Output summary:${NC}"
du -sh "$OUTPUT_DIR"/* 2>/dev/null || echo "No output files yet"
