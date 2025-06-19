#!/bin/bash

# Check if directory path is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <directory_path>"
    echo "Example: $0 /path/to/your/directory"
    exit 1
fi

start_time=$(date +"%s")

DIR_PATH="$1"

# Check if the provided path is a valid directory
if [ ! -d "$DIR_PATH" ]; then
    echo "Error: '$DIR_PATH' is not a valid directory"
    exit 1
fi

echo "Processing directory: $DIR_PATH"
echo "=========================================="

# Step 1: Run file_grouper.sh
echo "Step 1: Running file_grouper.sh..."
if ! ./file_grouper.sh "$DIR_PATH"; then
    echo "Error: file_grouper.sh failed"
    exit 1
fi
echo "✓ file_grouper.sh completed successfully"

# Step 2: Run check_same_device.py
echo "Step 2: Running check_same_device.py..."
if ! python3 check_same_device.py "$DIR_PATH"; then
    echo "Error: check_same_device.py failed"
    exit 1
fi
echo "✓ check_same_device.py completed successfully"

# Step 3: Run fit_to_csv.py on each subdirectory
echo "Step 3: Running fit_to_csv.py on each subdirectory..."

# Check if there are any subdirectories
subdirs_found=false
for subdir in "$DIR_PATH"*/; do
    if [ -d "$subdir" ]; then
        subdirs_found=true
        echo "Processing subdirectory: $subdir"
        if ! python3 fit_to_csv.py "$subdir"; then
            echo "Error: fit_to_csv.py failed for $subdir"
            exit 1
        fi
        echo "✓ fit_to_csv.py completed for $subdir"
    fi
done

if [ "$subdirs_found" = false ]; then
    echo "Warning: No subdirectories found in $DIR_PATH"
fi

end_time=$(date +"%s")
elapsed_time=$((end_time - start_time))
elapsed_hours=$(echo "scale=2; $elapsed_time / 3600" | bc)

echo "=========================================="
echo "All operations completed successfully in total time of: $elapsed_hours hours"
