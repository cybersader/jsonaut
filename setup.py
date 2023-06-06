import subprocess
import sys
import argparse
import shutil

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--run', action='store_true', help='Run the application after building')
# parser.add_argument('--onefile', action='store_true', help='Build as a single executable file')
args = parser.parse_args()

# Install PyInstaller
subprocess.run([sys.executable, "-m", "pip", "install", "pyinstaller"])

# Determine the spec file to use based on the build option
# if args.onefile:
#     spec_file = 'jsonaut.spec'
# else:
#     spec_file = 'jsonaut2.spec'
spec_file = 'jsonaut.spec'

# Build the executable using PyInstaller and the selected spec file
subprocess.run(["pyinstaller", spec_file])

# Move the jsonaut.exe file to the root folder if built as a single executable file
# if args.onefile:
#     shutil.move('dist/jsonaut.exe', 'jsonaut.exe')
shutil.move('dist/jsonaut.exe', 'jsonaut.exe')

# Run the built executable if the '--run' flag is provided
if args.run:
    subprocess.run(["./dist/jsonaut.exe"])
