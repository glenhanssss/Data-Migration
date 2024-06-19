import os
 
# directory the script python file exists
folder_path = r'_____'

# List of specific files to run
specific_files = [
    '_DB_CONNECTOR.py',
    '_Example_CALLING_DB.py',
    '____',
    '____',
    '____',
    '____',
]

# run all specific file scripts
for script in specific_files:
    script_path = os.path.join(folder_path, script)
    try:
        print(f"Running script: {script_path}")
        os.system(f"python {script_path}")
    except Exception as e:
        print(f"Error running script {script_path}: {e}")
