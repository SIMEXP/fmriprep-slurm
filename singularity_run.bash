#!/bin/bash

PROJECT_PATH="/lustre03/project/6003287"
DATASET_PATH=$1
DATASET_FOLDER="${DATASET_PATH%/*}"
DATASET_NAME=${DATASET_PATH##*/}
OUTPUT_DIR=$SCRATCH/$DATASET_NAME/$(date +%s)
TEMPLATEFLOW_DIR=/home/$USER/.cache/templateflow
PYTHON_CMD="python /fmriprep-slurm/fmriprep-slurm/main.py "$@" --output-path "$OUTPUT_DIR

echo "###"
echo "singularity_run.bash "$@
export SINGULARITYENV_TEMPLATEFLOW_HOME=/templateflow
module load singularity/3.6

echo "Creating templateflow directory in "$TEMPLATEFLOW_DIR
mkdir -p $TEMPLATEFLOW_DIR

echo "Running fmriprep-slurm inside singularity with the following command"
echo $PYTHON_CMD
mkdir -p $OUTPUT_DIR
echo $PYTHON_CMD | xargs singularity exec -B $PROJECT_PATH/fmriprep-slurm:/fmriprep-slurm \
  -B $DATASET_FOLDER:/DATA \
  -B /etc/pki:/etc/pki \
  -B $TEMPLATEFLOW_DIR:/templateflow \
  -B $OUTPUT_DIR:/OUTPUT \
  $PROJECT_PATH/containers/fmriprep-20.2.1lts.sif

echo ""
echo "Finished!"
echo ""
echo "Please review your slurm scripts in "$OUTPUT_DIR" before submitting them with:"
echo "find "$OUTPUT_DIR"/.slurm/smriprep_sub-*.sh -type f | while read file; do sbatch \"$file\"; done"
echo "###"