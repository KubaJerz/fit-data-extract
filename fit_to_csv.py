#!/usr/bin/env python3
"""
 FIT to CSV Converter Pipeline

This version uses numpy and pandas for significantly faster processing of large FIT files.

Usage:
    python fit_to_csv_converter_fast.py /path/to/fit/files
"""

import os
import sys
import datetime
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

from garmin_fit_sdk import Decoder, Stream

VALID_SELF_REPORT_EVENT_TYPES = {1, 2} # 1 == "cigarette"  2 == "vape"



class FastFitFileProcessor:
    """ processor for FIT files using numpy and pandas."""
    
    def __init__(self, file_path: Path):
        """Initialize the processor with a FIT file path."""
        self.file_path = file_path
        self.messages = None
        self.errors = None
    
    def decode_file(self) -> bool:
        """Decode the FIT file using the Garmin SDK."""
        try:
            stream = Stream.from_file(str(self.file_path))
            decoder = Decoder(stream)
            self.messages, self.errors = decoder.read()
            
            if self.errors:
                print(f"Warning: Errors found while decoding {self.file_path.name}: {self.errors}")
            
            return True
        except Exception as e:
            print(f"Error decoding {self.file_path.name}: {str(e)}")
            return False
    
    def _extract_sensor_data_fast(self, sensor_type: str) -> Optional[pd.DataFrame]:
        """
        extract sensor data 
        
        Args:
            sensor_type: Either 'accelerometer' or 'gyroscope'
            
        Returns:
            DataFrame with sensor data or None if no data
        """
        # Determine the message key and axis prefixes
        if sensor_type == 'accelerometer':
            message_key = 'accelerometer_data_mesgs'
            axis_prefix = 'calibrated_accel'
        elif sensor_type == 'gyroscope':
            message_key = 'gyroscope_data_mesgs'
            axis_prefix = 'calibrated_gyro'
        else:
            raise ValueError(f"Unknown sensor type: {sensor_type}")
        
        # Check if the sensor data exists
        if message_key not in self.messages:
            print(f"No {sensor_type} data found in {self.file_path.name}")
            return None
        
        sensor_messages = self.messages[message_key]
        
        all_timestamps = []
        all_x = []
        all_y = []
        all_z = []
        
        # Process each group
        for group in sensor_messages:
            try:
                base_timestamp_ms = group.get('timestamp_ms', 0)
                sample_offsets = group.get('sample_time_offset', [])
                
                x_values = group.get(f'{axis_prefix}_x', [])
                y_values = group.get(f'{axis_prefix}_y', [])
                z_values = group.get(f'{axis_prefix}_z', [])
                
                # Skip if data is inconsistent
                if not (len(x_values) == len(y_values) == len(z_values) == len(sample_offsets)):
                    continue
                
                # Calculate all timestamps for this group at once
                base_timestamp = datetime.datetime.fromtimestamp(base_timestamp_ms / 1000.0)
                timestamps = [base_timestamp + datetime.timedelta(milliseconds=offset) for offset in sample_offsets]
                
                # Extend our lists
                all_timestamps.extend(timestamps)
                all_x.extend(x_values)
                all_y.extend(y_values)
                all_z.extend(z_values)
                
            except Exception as e:
                continue
        
        if not all_timestamps:
            return None
        
        df = pd.DataFrame({
            'timestamp': all_timestamps,
            'x': all_x,
            'y': all_y,
            'z': all_z
        })
        
        return df
    
    def _process_self_reports(self, timestamp, event_type, df_event_self_reports, is_event_active, is_event_active_Field):
        if event_type in VALID_SELF_REPORT_EVENT_TYPES:

            if (not is_event_active) and (is_event_active_Field == 1): #just switched the event on
                with warnings.catch_warnings():
                    warnings.simplefilter(action='ignore', category=FutureWarning)
                    df_event_self_reports = pd.concat([df_event_self_reports, pd.DataFrame([{'start_timestamp': timestamp, 'end_timestamp': 'NAN', 'event_type':event_type}])], ignore_index=True )
                
                is_event_active =  True

            elif (is_event_active) and (is_event_active_Field == 0): #jsut switched the even off
                df_event_self_reports.at[df_event_self_reports.index[-1], 'end_timestamp'] = timestamp
                is_event_active = False

        return df_event_self_reports, is_event_active

    def _extract_record_data_fast(self) -> Optional[pd.DataFrame]:
        """
        Extract record data using pandas for faster processing.
        
        Returns:
            DataFrame with record data or None if no data
        """
        if 'record_mesgs' not in self.messages:
            print(f"No record messages found in {self.file_path.name}")
            return None
        
        record_messages = self.messages['record_mesgs']
        
        # Pre-allocate lists
        timestamps = []
        heart_rates = []
        developer_fields = []

        #df for self reports
        df_event_self_reports = pd.DataFrame(columns=['start_timestamp', 'end_timestamp', 'event_type'])
        is_event_active = False

        for record in record_messages:
            try:
                # Get timestamp
                timestamp_raw = record.get('timestamp')
                if timestamp_raw is None:
                    continue
                
                # Convert timestamp
                if isinstance(timestamp_raw, int):
                    timestamp = datetime.datetime.fromtimestamp(timestamp_raw)
                elif isinstance(timestamp_raw, datetime.datetime):
                    timestamp = timestamp_raw
                else:
                    timestamp = datetime.datetime.fromtimestamp(int(timestamp_raw))
                
                # Get heart rate
                heart_rate = record.get('heart_rate')
                
                # Get first developer field
                dev_fields = record.get('developer_fields', [])
                dev_field = dev_fields[0] if dev_fields else None
                
                timestamps.append(timestamp)
                heart_rates.append(heart_rate)
                developer_fields.append(str(dev_field) if dev_field is not None else None)

                #process the self reports
                is_event_active_Field =  dev_fields[1] #is_event_active_Field this valie is one if we are recroding and anythin else if we are not recording
                event_type = dev_fields[2] # event type
                df_event_self_reports, is_event_active = self._process_self_reports(timestamp, event_type, df_event_self_reports, is_event_active, is_event_active_Field)

                
            except Exception:
                continue
        
        if not timestamps:
            return None
        
        # Create DataFrame
        df_records = pd.DataFrame({
            'timestamp': timestamps,
            'heart_rate': heart_rates,
            'developer_field': developer_fields
        })
        
        return df_records, df_event_self_reports
    
    def process_to_csv(self, output_dir: Path) -> Tuple[Optional[Path], Optional[Path], Optional[Path]]:
        """Process the FIT file and save data to CSV files using pandas."""
        start_time = time.time()
        
        if not self.decode_file():
            return None, None, None
        
        # Create output file paths
        base_name = self.file_path.stem
        accel_csv_path = output_dir / f"{base_name}_accelerometer.csv"
        gyro_csv_path = output_dir / f"{base_name}_gyroscope.csv"
        record_csv_path = output_dir / f"{base_name}_records.csv"
        self_report_csv_path = output_dir / f"{base_name}_self_reports.csv"

        
        paths = []
        
        # Process accelerometer data
        accel_df = self._extract_sensor_data_fast('accelerometer')
        if accel_df is not None:
            accel_df.to_csv(accel_csv_path, index=False)
            print(f"Wrote {len(accel_df)} accelerometer samples to {accel_csv_path.name}")
            paths.append(accel_csv_path)
        else:
            paths.append(None)
        
        # Process gyroscope data
        gyro_df = self._extract_sensor_data_fast('gyroscope')
        if gyro_df is not None:
            gyro_df.to_csv(gyro_csv_path, index=False)
            print(f"Wrote {len(gyro_df)} gyroscope samples to {gyro_csv_path.name}")
            paths.append(gyro_csv_path)
        else:
            paths.append(None)
        
        # Process record data and self reports
        record_df, self_report_event_df = self._extract_record_data_fast()
        
        #save records df
        if record_df is not None:
            record_df.to_csv(record_csv_path, index=False, na_rep='')
            print(f"Wrote {len(record_df)} record samples to {record_csv_path.name}")
            paths.append(record_csv_path)
        else:
            paths.append(None)

        #save self reports
        if self_report_event_df is not None:
            self_report_event_df.to_csv(self_report_csv_path, index=False, na_rep='')
            print(f"Wrote {len(self_report_event_df)} self report to {self_report_csv_path.name}")
            paths.append(self_report_csv_path)
        else:
            paths.append(None)
        
        elapsed = time.time() - start_time
        print(f"Processing time for {self.file_path.name}: {elapsed:.2f} seconds")
        
        return tuple(paths)


def process_single_file(fit_file: Path, output_dir: Path) -> Tuple[Path, bool]:
    """Process a single FIT file (used for multiprocessing)."""
    processor = FastFitFileProcessor(fit_file)
    results = processor.process_to_csv(output_dir)
    success = any(path is not None for path in results)
    return fit_file, success


class FastFitToCsvPipeline:
    """ pipeline using multiprocessing for multiple files."""
    
    def __init__(self, input_dir: Path, output_dir: Optional[Path] = None):
        """Initialize the pipeline."""
        self.input_dir = input_dir
        self.output_dir = output_dir or input_dir / "csv_output"
        self.output_dir.mkdir(exist_ok=True)
    
    def find_fit_files(self) -> List[Path]:
        """Find all FIT files in the input directory."""
        fit_files = list(self.input_dir.glob("*.fit")) + list(self.input_dir.glob("*.FIT"))
        return fit_files
    
    def process_all_files(self, use_multiprocessing: bool = True):
        """Process all FIT files, optionally using multiprocessing."""
        fit_files = self.find_fit_files()
        
        if not fit_files:
            print(f"No FIT files found in {self.input_dir}")
            return
        
        print(f"Found {len(fit_files)} FIT file(s) to process")
        print(f"Output directory: {self.output_dir}")
        print(f"Multiprocessing: {'Enabled' if use_multiprocessing else 'Disabled'}")
        print("-" * 50)
        
        start_time = time.time()
        success_count = 0
        
        if use_multiprocessing and len(fit_files) > 1:
            # Use multiprocessing for multiple files
            max_workers = min(len(fit_files), os.cpu_count() or 1)
            
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                # Submit all files for processing
                future_to_file = {
                    executor.submit(process_single_file, fit_file, self.output_dir): fit_file
                    for fit_file in fit_files
                }
                
                # Process completed files
                for future in as_completed(future_to_file):
                    fit_file = future_to_file[future]
                    try:
                        _, success = future.result()
                        if success:
                            success_count += 1
                    except Exception as e:
                        print(f"Error processing {fit_file.name}: {str(e)}")
        else:
            # Process files sequentially
            for fit_file in fit_files:
                print(f"\nProcessing: {fit_file.name}")
                _, success = process_single_file(fit_file, self.output_dir)
                if success:
                    success_count += 1
        
        total_elapsed = time.time() - start_time
        print(f"\nTotal processing time: {total_elapsed:.2f} seconds")
        print(f"Average time per file: {total_elapsed/len(fit_files):.2f} seconds")
        print(f"Successfully processed {success_count}/{len(fit_files)} files.")


def main(): 

    """Main entry point for the script."""
    if len(sys.argv) < 2:
        print("Usage: python fit_to_csv_converter_fast.py /path/to/fit/files [--no-multiprocessing]")
        sys.exit(1)
    
    input_path = Path(sys.argv[1])
    use_multiprocessing = "--no-multiprocessing" not in sys.argv
    
    if not input_path.exists():
        print(f"Error: Path '{input_path}' does not exist")
        sys.exit(1)
    
    if not input_path.is_dir():
        print(f"Error: Path '{input_path}' is not a directory")
        sys.exit(1)
    
    pipeline = FastFitToCsvPipeline(input_path)
    pipeline.process_all_files(use_multiprocessing)


if __name__ == "__main__":
    main()