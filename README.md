# fmriprep-slurm
Generate and run [fMRIPrep](https://fmriprep.org/en/stable/) [SLURM](https://slurm.schedmd.com/documentation.html) jobs on HPCs.

It is carefully designed and optimized to run the preprocessing on a `BIDS <https://bids-specification.readthedocs.io/en/stable/>`_ dataset.
It has also the advantage to prepare the dataset by checking the integrity, and caching some of the BIDS database artifacts.


Originally from https://github.com/courtois-neuromod/ds_prep/blob/master/derivatives/fmriprep/fmriprep.py

# Usage

## Arguments

positional arguments:  
  bids_path             BIDS folder to run fmriprep on.  

  derivatives_name      name of the output folder in derivatives.  

optional arguments:  
  -h, --help            show this help message and exit

  --preproc PREPROC     anat, func or all (default: all)

  --slurm-account SLURM_ACCOUNT
                        SLURM account for job submission (default: rrg-pbellec)

  --email EMAIL         email for SLURM notifications

  --container CONTAINER
                        fmriprep singularity container (default: fmriprep-20.2.1lts)

  --participant-label PARTICIPANT_LABEL [PARTICIPANT_LABEL ...]
                        a space delimited list of participant identifiers or a single identifier (the sub- prefix can be removed)

  --output-spaces OUTPUT_SPACES [OUTPUT_SPACES ...]
                        a space delimited list of templates as defined by templateflow (default: ["MNI152NLin2009cAsym, MNI152NLin6Asym"])

  --fmriprep-args FMRIPREP_ARGS
                        additionnal arguments to the fmriprep command as a string (ex: --fmriprep-args="--fs-no-reconall --use-aroma")

  --session-label SESSION_LABEL [SESSION_LABEL ...]
                        a space delimited list of session identifiers or a single identifier (the ses- prefix can be removed)

  --force-reindex       Force pyBIDS reset_database and reindexing

  --submit              Submit SLURM jobs

  --bids-filter BIDS_FILTER
                        Path to an optionnal bids_filter.json template

  --time TIME           Time duration for the slurm job in slurm format (dd-)hh:mm:ss (default: 24h structural, 12h functionnal)

  --mem-per-cpu MEM_PER_CPU
                        upper bound memory limit for fMRIPrep processes(default: 4096MB)
                        
  --cpus CPUS           maximum number of cpus for all processes(default: 16) 

Templateflow valid identifiers can be found at https://github.com/templateflow/templateflow

## Default fmriprep command

By default, we use the following fMRIPrep arguments:

`--participant-label` dependning on the user argument choice.

`--bids-database-dir` which takes as an input the cached from pybids.

`--bids-filter-file` dependning on the user argument choice.

`--notrack` since beluga compute nodes does not have access to internet, and reduce computation burden.

`--skip_bids_validation` since it was already done inside `fmriprep-slum`.

`--write-graph` for debugging.

`--omp-nthreads`, `--nprocs` and `--mem_mb` dependning on the user argument choice.

`--resource-monitor` for debugging.

For more information on each of these option, you can check the [documentation](https://simexp-documentation.readthedocs.io/en/latest/giga_preprocessing/preprocessing.html).