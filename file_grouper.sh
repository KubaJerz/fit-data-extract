#!/bin/bash

# Function to convert timestamp to seconds since epoch
timestamp_to_seconds() {
    local timestamp="$1"
    # Extract date and time parts: 2025-06-17-14-52-47
    local year=$(echo "$timestamp" | cut -d'-' -f1)
    local month=$(echo "$timestamp" | cut -d'-' -f2)
    local day=$(echo "$timestamp" | cut -d'-' -f3)
    local hour=$(echo "$timestamp" | cut -d'-' -f4)
    local minute=$(echo "$timestamp" | cut -d'-' -f5)
    local second=$(echo "$timestamp" | cut -d'-' -f6)
    
    # Convert to seconds since epoch
    date -d "$year-$month-$day $hour:$minute:$second" +%s 2>/dev/null
}

# Function to extract timestamp from filename
extract_timestamp() {
    local filename="$1"
    # Remove .fit extension and extract timestamp
    echo "${filename%.fit}"
}

# Check if directory path is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <directory_path>"
    exit 1
fi

DIR_PATH="$1"

# Check if directory exists
if [ ! -d "$DIR_PATH" ]; then
    echo "Error: Directory '$DIR_PATH' does not exist"
    exit 1
fi

echo "Processing files in: $DIR_PATH"
echo "----------------------------------------"

# Get all .fit files and sort them
files=($(ls "$DIR_PATH"/*.fit 2>/dev/null | sort))

if [ ${#files[@]} -eq 0 ]; then
    echo "No .fit files found in directory"
    exit 1
fi

echo "Found ${#files[@]} .fit files"
echo

# Group files
groups=()
current_group=()
group_count=0

for file in "${files[@]}"; do
    filename=$(basename "$file")
    timestamp=$(extract_timestamp "$filename")
    current_seconds=$(timestamp_to_seconds "$timestamp")
    
    if [ -z "$current_seconds" ]; then
        echo "Warning: Could not parse timestamp for $filename"
        continue
    fi
    
    # If this is the first file or current group is empty, start new group
    if [ ${#current_group[@]} -eq 0 ]; then
        current_group=("$filename")
        last_seconds=$current_seconds
    else
	# Check if this file belongs to current group (within 300 seconds (5min) + 2 sec)
        time_diff=$((current_seconds - last_seconds))
        if [ $time_diff -le 302 ] && [ $time_diff -ge 300 ]; then
            # Add to current group
            current_group+=("$filename")
            last_seconds=$current_seconds
        else
            # Save current group and start new one
            groups[group_count]="${current_group[*]}"
            ((group_count++))
            current_group=("$filename")
            last_seconds=$current_seconds
        fi
    fi
done

# Don't forget the last group
if [ ${#current_group[@]} -gt 0 ]; then
    groups[group_count]="${current_group[*]}"
    ((group_count++))
fi

# Display groups
echo "Found $group_count groups:"
echo "========================="

for i in $(seq 0 $((group_count-1))); do
    group_files=(${groups[i]})
    echo "Group $((i+1)): ${#group_files[@]} files"
    for file in "${group_files[@]}"; do
        echo "  - $file"
    done
    echo
done

# Ask user for confirmation
echo -n "Is this grouping acceptable? (y/n): "
read -r response

if [[ "$response" =~ ^[Yy]$ ]]; then
    echo "Proceeding with grouping..."
    
    # Create group directories and move files
    for i in $(seq 0 $((group_count-1))); do
        #group_dir="$DIR_PATH/group_$((i+1))"

	group_files=(${groups[i]})
	first_file="${group_files[0]}"
	group_dir=$(extract_timestamp "$first_file")
	group_dir_path="$DIR_PATH/$group_dir"

	#check that the dir does not exist
	if [ -d "$group_dir_path" ]; then
    		echo "DANGER: Directory $group_dir_path already exists. Exiting to not over write"
		exit 1
	fi
	
	mkdir -p "$group_dir_path"

        group_files=(${groups[i]})
        echo "Moving ${#group_files[@]} files to $group_dir"
        
        for file in "${group_files[@]}"; do
            mv "$DIR_PATH/$file" "$group_dir/"
        done
    done
    
    echo "Grouping completed!"
else
    echo "Grouping cancelled."
fi
