# This script checks that every fit file has the same Unit ID in the Sub Folders

import os 
import sys
from garmin_fit_sdk import Decoder, Stream
from tqdm import tqdm

def extract_id(file_path):
	stream = Stream.from_file(file_path)
	decoder = Decoder(stream)
	messages, errors = decoder.read()
	
	serial_number = messages['file_id_mesgs'][0]['serial_number']
	
	return serial_number

def check_sub_dir(sub_dir_path):
	first_file_unit_id = 'N/A'
	for file in tqdm(sorted(os.listdir(sub_dir_path))):
		if not file.endswith(".fit"):
			continue
		else:
			file_id = extract_id(sub_dir_path+'/'+file)
			if first_file_unit_id == 'N/A':
				first_file_unit_id = file_id
			else:
				if first_file_unit_id != file_id:
					print(f"[MISMATCH] File '{file}' has ID '{file_id}', expected '{first_file_unit_id}' in folder {sub_dir_path}")
				



def main():
	if len(sys.argv) != 2:
		print("Usage: python check_same_device.py path/to/dir/containing/subdirs/")
		sys.exit(1)

	PARENT_DIR_PATH =  sys.argv[1]

	for sub_dir in sorted(os.listdir(PARENT_DIR_PATH)):
		if os.path.isdir(PARENT_DIR_PATH+'/'+sub_dir):
			print(f"Checking sub dir: {sub_dir}")
			check_sub_dir(PARENT_DIR_PATH+'/'+sub_dir)


if __name__ == "__main__":
	main() 
