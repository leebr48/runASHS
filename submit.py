#!/bin/python3

# This script was written by Brandon F. Lee (bflee at princeton dot edu).

# Import necessary modules
import pandas as pd
import argparse as ap
import os
import subprocess as sp
from glob import glob

# Set up command line arguments
# The defualts can be changed as desired.
desc = 'Automatically read subject IDs from the first column of a CSV file (with no header) and run ASHS jobs for them using Slurm. ' + \
       'Please note that the "[]" symbols around the defaults listed below are visual only - ' + \
       'they should not be present in your inputs. ' + \
       'ALSO NOTE: this script must be run with python3, and you must have the pandas module installed.'
parser = ap.ArgumentParser(description=desc, formatter_class=ap.ArgumentDefaultsHelpFormatter)
parser.add_argument('csv', type=str, nargs=1, help='CSV file with subject IDs to be run in the first column (with no header). Include path if necessary.')
parser.add_argument('-root', type=str, nargs=1, required=False, default=[None], help='Specifies the root directory for ASHS. By default, this information will be pulled from the environment variable "ASHS_ROOT".')
parser.add_argument('-atlas', type=str, nargs=1, required=False, default=['ashs_atlas_upennpmc_20170810'], help='The name of the ASHS atlas to be used. This is assumed to reside in the top level of the ASHS "root" directory.')
parser.add_argument('-data', type=str, nargs=1, required=False, default=[None], help='Data directory for subjects. This should contain subdirectories whose names match the subject IDs in the CSV file. Each of these subdirectories must contain *ONE* 3D gradient echo MRI file (MPRAGE_[0-9]*.nii.*) and *ONE* 2D focal fast spin echo MRI file (HighResHippo_[0-9]*.nii.*). This option defaults to the "data" subdirectory in the top level of the ASHS "root" directory.')
parser.add_argument('-out', type=str, nargs=1, required=False, default=[None], help='Specifies the directory in which the outputs will be stored. More specifically, outputs for each subject will be stored in subdirectories within this directory named by the subject IDs. This option defaults to the "outputs" subdirectory in the top level of the ASHS "root" directory.')
parser.add_argument('-tidy', action='store_true', default=False, help='If this option is used, ASHS is instructed to clean up files once they are not needed.')
parser.add_argument('-nProcs', type=int, nargs=1, required=False, default=[1], help='Total number of processors allocated for each ASHS run.')
parser.add_argument('-mem', type=float, nargs=1, required=False, default=['4'], help='Total amount of memory in gigabytes allocated for each ASHS run.')
parser.add_argument('-time', type=str, nargs=1, required=False, default=['04:00:00'], help='Wall clock time limit for each ASHS run. Format is HH:MM:SS.')
parser.add_argument('-email', type=str, nargs=1, required=False, default=[None], help='If specified, this address will receive an email when something goes wrong with an ASHS calculation (that the computer automatically detects).')
parser.add_argument('-noSubmit', action='store_true', default=False, help='If this option is used, Slurm scripts will be written but not submitted. This is useful for testing and troubleshooting.')
args = parser.parse_args()

# Validate command line arguments and assign internal variable names
stdComplain = ' Please format it like the default setting.'

if os.path.exists(args.csv[0]):
    subjectsToRunFile = os.path.abspath(args.csv[0])
else:
    raise IOError('It appears that the specified CSV file does not exist.')

nProcs = args.nProcs[0]

mem = str(args.mem[0]) + 'G'

stripTime = args.time[0].strip()
timeHourSplit = stripTime.split(':')
try:
    _ = [int(elem) for elem in timeHourSplit]
except ValueError:
    raise IOError('It appears that at least one of the "hours", "minutes", or "seconds" portions of the time input is incorrect.' + stdComplain)
time = stripTime

if (args.email[0] is not None) and ('@' not in args.email[0]):
    raise IOError('It appears that the input email address is incorrectly formatted.')
email = args.email[0]

noSubmit = args.noSubmit

if args.root[0] is None:
    try:
        ashsRoot = os.environ['ASHS_ROOT']
    except KeyError:
        raise IOError('You must either specify the "ASHS_ROOT" environment variable or use the "root" option for this script.')
else:
    if not os.path.exists(args.root[0]):
        raise IOError('The "root" directory that was specified for ASHS does not appear to exist.')
    ashsRoot = os.path.abspath(args.root[0])

ashsAtlasName = args.atlas[0]
ashsAtlasPath = os.path.join(ashsRoot, ashsAtlasName)
if not os.path.exists(ashsAtlasPath):
    raise IOError('The specified ASHS atlas does not appear to exist.')

if args.data[0] is None:
    dataDir = os.path.join(ashsRoot, 'data')
else:
    dataDir = os.path.abspath(args.data[0])
if not os.path.exists(dataDir):
    raise IOError('The specified "data" directory does not appear to exist.')

if args.out[0] is None:
    parentOutDir = os.path.join(ashsRoot, 'outputs')
else:
    parentOutDir = os.path.abspath(args.out[0])
os.makedirs(parentOutDir, exist_ok=True)

tidy = args.tidy

# Read first column of CSV file and extract subject IDs
df = pd.read_csv(subjectsToRunFile, usecols=[0], header=None)
subjects = list(df.to_numpy().flatten())

# Write files for Slurm and submit them to the scheduler
s = '#SBATCH '
e = '\n'

for subject in subjects:

    # Ensure data directories exist and don't contain ambiguous files
    gradientFiles = glob(os.path.join(dataDir, subject) + '/MPRAGE_[0-9]*.nii.*')
    focalFiles = glob(os.path.join(dataDir, subject) + '/HighResHippo_[0-9]*.nii.*')

    if len(gradientFiles) != 1 or len(focalFiles) != 1:
        msg = 'WARNING: ' + \
              'It appears that there is not exactly *ONE* of each data file type ' + \
              '(3D gradient echo MRI and 2D focal fast spin echo MRI) ' + \
              'in the data directory for subject %s. '%subject + \
              'You may need to run ASHS manually for this subject. ' + \
              'Press the Enter key to acknowledge this message and skip this subject. >>>'
        input(msg)
        continue

    subjectOutDir = os.path.join(parentOutDir, subject)
    os.makedirs(subjectOutDir, exist_ok=True)
    
    # Declare some variables
    sbatchFileName = 'sbatch_%s.script'%subject
    sbatchFilePath = os.path.join(subjectOutDir, sbatchFileName)
    ashsCmd = 'srun ' + os.path.join(ashsRoot, 'bin/ashs_main.sh') + ' ' + \
              '-a '+ ashsAtlasPath + ' ' + \
              '-g ' + gradientFiles[0] + ' ' + \
              '-f ' + focalFiles[0] + ' ' + \
              '-w ' + subjectOutDir + ' ' + \
              '-I %s'%subject
    if args.tidy:
        ashsCmd += ' -T'
    
    # Write the batch file for Slurm
    with open(sbatchFilePath, 'w') as f:
        
        f.write('#!/bin/bash' + e)
        f.write(e)
        f.write('#Slurm directives:' + e)
        f.write(e)
        f.write(s + '-n ' + str(nProcs) + e)
        f.write(s + '--mem=%s'%mem + e)
        f.write(s + '--time ' + time + e)
        f.write(e)
        f.write(s + '-J ' + 'ASHS_%s'%subject + e)
        f.write(s + '-o ' + 'ASHS_%s-%%j.out'%subject + e)
        f.write(s + '-e ' + 'ASHS_%s-%%j.err'%subject + e)
        f.write(e)

        if email is not None:
            f.write(s + '--mail-type=FAIL,REQUEUE' + e)
            f.write(s + '--mail-user=' + email + e)
            f.write(e)

        f.write('#Commands to run:' + e)
        f.write(e)
        f.write(ashsCmd + e)

    # Submit the job to the scheduler, if desired
    if not noSubmit:
        p = sp.Popen(['sbatch', sbatchFileName], cwd=subjectOutDir)
        p.wait()
