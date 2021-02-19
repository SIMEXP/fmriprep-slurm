# fmriprep-slurm
Generate and run [fMRIPrep](https://fmriprep.org/en/stable/) [SLURM](https://slurm.schedmd.com/documentation.html) jobs on HPCs.

It is carefully designed and optimized to run the preprocessing on a `BIDS <https://bids-specification.readthedocs.io/en/stable/>`_ dataset.
It has also the advantage to prepare the dataset by checking the integrity, and caching some of the BIDS components


Originally from https://github.com/courtois-neuromod/ds_prep/blob/master/derivatives/fmriprep/fmriprep.py

# Usage

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
                        a space delimited list of templates as defined by templateflow (default: ["MNI152NLin2009cAsym", "MNI152NLin6Asym"])  
                        
  --session-label SESSION_LABEL [SESSION_LABEL ...]  
                        a space delimited list of session identifiers or a single identifier (the ses- prefix can be removed)  
                        
  --force-reindex       Force pyBIDS reset_database and reindexing  
  
  --submit              Submit SLURM jobs  
  
  --bids-filter BIDS_FILTER  
                        Path to an optionnal bids_filter.json template
                        
  --time TIME           Time duration for the slurm job in slurm format (dd-)hh:mm:ss (default: 24h structural, 12h functionnal)  


templateflow: valid identifiers https://github.com/templateflow/templateflow
