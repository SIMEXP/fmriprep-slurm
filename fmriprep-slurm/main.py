#!/usr/bin/python3

import os
import sys
import argparse
import bids
import subprocess
import json
import re
import fnmatch
import templateflow.api as tf_api

SCRIPT_DIR = os.path.dirname(__file__)

SLURM_JOB_DIR = ".slurm"

SMRIPREP_REQ = {"cpus": 16, "mem_per_cpu": 4096,
                "time": "24:00:00", "omp_nthreads": 8}
FMRIPREP_REQ = {"cpus": 16, "mem_per_cpu": 4096,
                "time": "12:00:00", "omp_nthreads": 8}

SINGULARITY_DATA_PATH = "/DATA"
SINGULARITY_OUTPUT_PATH = "/OUTPUT"
FMRIPREP_DEFAULT_VERSION = "fmriprep-20.2.1lts"
FMRIPREP_DEFAULT_SINGULARITY_FOLDER = "/lustre03/project/6003287/containers"
OUTPUT_SPACES_DEFAULT = ["MNI152NLin2009cAsym", "MNI152NLin6Asym"]
SLURM_ACCOUNT_DEFAULT = "rrg-pbellec"
PREPROC_DEFAULT = "all"
TEMPLATEFLOW_HOME = os.path.join(os.path.join(
    os.environ["HOME"], ".cache"), "templateflow",)
SINGULARITY_CMD_BASE = " ".join(
    [
        "singularity run",
        "--cleanenv",
        f"-B $SLURM_TMPDIR:{SINGULARITY_DATA_PATH}",
        f"-B {TEMPLATEFLOW_HOME}:/templateflow",
        "-B /etc/pki:/etc/pki/",
        "-B {original_output}:{SINGULARITY_OUTPUT_PATH}"
    ]
)

slurm_preamble = """#!/bin/bash
#SBATCH --account={slurm_account}
#SBATCH --job-name={jobname}.job
#SBATCH --output={original_output}/{jobname}.out
#SBATCH --error={original_output}/{jobname}.err
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem-per-cpu={mem_per_cpu}M
#SBATCH --mail-user={email}
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

export SINGULARITYENV_FS_LICENSE=$HOME/.freesurfer.txt
export SINGULARITYENV_TEMPLATEFLOW_HOME=/templateflow

module load singularity/3.8

#copying input dataset into local scratch space
rsync -rltv --info=progress2 --exclude="sub*" --exclude="derivatives" {bids_root} ${SLURM_TMPDIR}
rsync -rltv --info=progress2 {bids_root}/{participant} ${SLURM_TMPDIR}/{bids_basename}
rsync -rltv --info=progress2 {bids_root}/{participant} ${SLURM_TMPDIR}/

"""

#default fmriprep bids filter
BIDS_FILTERS = {
  'fmap': {'datatype': 'fmap'},
  'bold': {'datatype': 'func', 'suffix': 'bold'},
  'sbref': {'datatype': 'func', 'suffix': 'sbref'},
  'flair': {'datatype': 'anat', 'suffix': 'FLAIR'},
  't2w': {'datatype': 'anat', 'suffix': 'T2w'},
  't1w': {'datatype': 'anat', 'suffix': 'T1w'},
  'roi': {'datatype': 'anat', 'suffix': 'roi'},
}
# old bids filter
# BIDS_FILTERS = {"t1w": {"reconstruction": None, "acquisition": None}, "t2w": {
#     "reconstruction": None, "acquisition": None}, "bold": {}}


def load_bidsignore(bids_root):
    """Load .bidsignore file from a BIDS dataset, returns list of regexps"""
    bids_ignore_path = os.path.join(bids_root, ".bidsignore")
    if os.path.exists(bids_ignore_path):
        with open(bids_ignore_path) as f:
            bids_ignores = f.read().splitlines()
        return tuple(
            [
                re.compile(fnmatch.translate(bi))
                for bi in bids_ignores
                if len(bi) and bi.strip()[0] != "#"
            ]
        )
    return tuple()


def write_job_footer(fd, jobname, bids_path, fmriprep_workdir, derivatives_name, output_path):
    fd.write("fmriprep_exitcode=$?\n")
    dataset_name = os.path.basename(bids_path)
    local_derivative_dir = os.path.join(
        "$SLURM_TMPDIR", dataset_name, "derivatives", derivatives_name)
    fd.write(
        f"if [ $fmriprep_exitcode -ne 0 ] ; then rsync -rltv --info=progress2 {fmriprep_workdir} {output_path}/{jobname}.workdir ; fi \n"
    )
    fd.write(
        f"if [ $fmriprep_exitcode -eq 0 ] ; then rsync -rltv --info=progress2 {fmriprep_workdir}/fmriprep_wf/resource_monitor.json {output_path}/{jobname}_resource_monitor.json ; fi \n"
    )
    fd.write(
        f"if [ $fmriprep_exitcode -eq 0 ] ; then mkdir -p {output_path}/{derivatives_name} ; fi \n"
    )
    fd.write(
        f"if [ $fmriprep_exitcode -eq 0 ] ; then rsync -rltv --info=progress2 {local_derivative_dir}/* {output_path}/{derivatives_name}/ ; fi \n"
    )
    fd.write("exit $fmriprep_exitcode \n")


def write_fmriprep_job(layout, subject, args, anat_only=True):
    job_specs = dict(
        original_output=args.output_path,
        slurm_account=args.slurm_account,
        jobname=f"smriprep_sub-{subject}",
        email=args.email,
        bids_root=os.path.realpath(args.bids_path),
        bids_basename=os.path.basename(os.path.realpath(args.bids_path)),
        participant=f"sub-{subject}",
    )
    job_specs.update(SMRIPREP_REQ)
    if args.time:
        job_specs.update({"time": args.time, })
    if args.mem_per_cpu:
        job_specs.update({"mem_per_cpu": int(args.mem_per_cpu), })
    if args.cpus:
        job_specs.update({"cpus": int(args.cpus), })

    job_path = os.path.join(SINGULARITY_OUTPUT_PATH,
                            SLURM_JOB_DIR, f"{job_specs['jobname']}.sh")

    derivatives_path = os.path.join(
        layout.root,
        "derivatives",
        args.derivatives_name
    )

    # use json load/dump to copy filters (and validate json in the meantime)
    bids_filters_path = os.path.join(
        SINGULARITY_OUTPUT_PATH,
        "bids_filters.json")

    # checking if bids_filter path provided by user is valid, if not default bids_filter is used
    if args.bids_filter:
        if os.path.exists(args.bids_filter):
            bids_filters = json.load(open(args.bids_filter))
    else:
        bids_filters = BIDS_FILTERS
    with open(bids_filters_path, "w") as f:
        json.dump(bids_filters, f)

    fmriprep_singularity_path = os.path.join(
        FMRIPREP_DEFAULT_SINGULARITY_FOLDER, args.container + ".sif")
    sing_fmriprep_workdir = os.path.join(
        SINGULARITY_DATA_PATH, "fmriprep_work")

    with open(job_path, "w") as f:
        f.write(slurm_preamble.format(**job_specs))
        f.write(
            " ".join(
                [
                    SINGULARITY_CMD_BASE.format(
                        original_output=args.output_path, SINGULARITY_OUTPUT_PATH=SINGULARITY_OUTPUT_PATH),
                    fmriprep_singularity_path,
                    f"-w {sing_fmriprep_workdir}",
                    f"--participant-label {subject}",
                    "--anat-only" if anat_only else "",
                    f" --bids-filter-file {bids_filters_path}",
                    " ".join(args.fmriprep_args),
                    " --output-spaces",
                    *args.output_spaces,
                    "--output-layout bids",
                    "--notrack",
                    "--skip_bids_validation",
                    "--write-graph",
                    f"--omp-nthreads {job_specs['omp_nthreads']}",
                    f"--nprocs {job_specs['cpus']}",
                    f"--mem_mb {job_specs['mem_per_cpu']*job_specs['cpus']}",
                    # monitor resources to design a heuristic for runtime/cpu/ram
                    "--resource-monitor",
                    layout.root,
                    derivatives_path,
                    "participant",
                    "\n",
                ]
            )
        )
        fmriprep_workdir = os.path.join("$SLURM_TMPDIR", "fmriprep_work")
        write_job_footer(f, job_specs["jobname"], os.path.realpath(
            args.bids_path), fmriprep_workdir, args.derivatives_name, args.output_path)
    return job_path


def write_func_job(layout, subject, session, args):
    outputs_exist = False
    study = os.path.basename(layout.root)
    anat_path = os.path.join(
        SINGULARITY_DATA_PATH,
        "anat",
        "derivatives",
        args.derivatives_name,
    )
    derivatives_path = os.path.join(
        layout.root,
        "derivatives",
        args.derivatives_name)

    bold_runs = layout.get(
        subject=subject, session=session, extension=[".nii", ".nii.gz"], suffix="bold"
    )

    bold_derivatives = []
    for bold_run in bold_runs:
        entities = bold_run.entities
        entities = [
            (ent, entities[ent])
            for ent in ["subject", "session", "task", "run"]
            if ent in entities
        ]
        preproc_entities = entities + [
            ("space", args.output_spaces[0]),
            ("desc", "preproc"),
        ]
        dtseries_entities = entities + [("space", "fsLR"), ("den", "91k")]
        func_path = os.path.join(
            derivatives_path,
            "fmriprep",
            f"sub-{subject}",
            f"ses-{session}",
            "func",
        )
        preproc_path = os.path.join(
            func_path,
            "_".join(
                [
                    "%s-%s" % (k[:3] if k in ["subject", "session"] else k, v)
                    for k, v in preproc_entities
                ]
            )
            + "_bold.nii.gz",
        )
        dtseries_path = os.path.join(
            func_path,
            "_".join(
                [
                    "%s-%s" % (k[:3] if k in ["subject", "session"] else k, v)
                    for k, v in dtseries_entities
                ]
            )
            + "_bold.dtseries.nii",
        )
        # test if file or symlink (even broken if git-annex and not pulled)
        bold_deriv = os.path.lexists(
            preproc_path) and os.path.lexists(dtseries_path)
        if bold_deriv:
            print(
                f"found existing derivatives for {bold_run.path} : {preproc_path}, {dtseries_path}"
            )
        bold_derivatives.append(bold_deriv)
    outputs_exist = all(bold_derivatives)
    # n_runs = len(bold_runs)
    # run_shapes = [run.get_image().shape for run in bold_runs]
    # run_lengths = [rs[-1] for rs in run_shapes]

    job_specs = dict(
        slurm_account=args.slurm_account,
        jobname=f"fmriprep_study-{study}_sub-{subject}_ses-{session}",
        email=args.email,
        bids_root=os.path.realpath(args.bids_path),
        bids_basename=os.path.basename(os.path.realpath(args.bids_path)),
        participant=f"sub-{subject}",
    )
    job_specs.update(FMRIPREP_REQ)
    if args.time:
        job_specs.update({"time": args.time, })
    if args.mem_per_cpu:
        job_specs.update({"mem_per_cpu": int(args.mem_per_cpu), })
    if args.cpus:
        job_specs.update({"cpus": int(args.cpus), })

    job_path = os.path.join(SINGULARITY_OUTPUT_PATH,
                            SLURM_JOB_DIR, f"{job_specs['jobname']}.sh")
    bids_filters_path = os.path.join(
        SINGULARITY_OUTPUT_PATH,
        f"{job_specs['jobname']}_bids_filters.json"
    )

    # checking if bids_filter path provided by user is valid, if not default bids_filter is used
    if args.bids_filter:
        if os.path.exists(args.bids_filter):
            bids_filters = json.load(open(args.bids_filter))
    else:
        bids_filters = BIDS_FILTERS
    # filter for session
    bids_filters["bold"].update({"session": session})
    with open(bids_filters_path, "w") as f:
        json.dump(bids_filters, f)

    fmriprep_singularity_path = os.path.join(
        FMRIPREP_DEFAULT_SINGULARITY_FOLDER, args.container + ".sif")
    sing_fmriprep_workdir = os.path.join(
        SINGULARITY_DATA_PATH, "fmriprep_work")

    with open(job_path, "w") as f:
        f.write(slurm_preamble.format(**job_specs))
        f.write(
            " ".join(
                [
                    SINGULARITY_CMD_BASE.format(
                        original_output=args.output_path, SINGULARITY_OUTPUT_PATH=SINGULARITY_OUTPUT_PATH),
                    fmriprep_singularity_path,
                    f"-w {sing_fmriprep_workdir}",
                    f"--participant-label {subject}",
                    f"--anat-derivatives {anat_path}/fmriprep",
                    f"--fs-subjects-dir {anat_path}/freesurfer",
                    f" --bids-filter-file {bids_filters_path}",
                    " --ignore slicetiming",
                    "--use-syn-sdc",
                    " ".join(args.fmriprep_args),
                    "--output-spaces",
                    *args.output_spaces,
                    "--output-layout bids",
                    "--notrack",
                    "--write-graph",
                    "--skip_bids_validation",
                    f"--omp-nthreads {job_specs['omp_nthreads']}",
                    f"--nprocs {job_specs['cpus']}",
                    f"--mem_mb {job_specs['mem_per_cpu']*job_specs['cpus']}",
                    # monitor resources to design a heuristic for runtime/cpu/ram
                    "--resource-monitor",
                    layout.root,
                    derivatives_path,
                    "participant",
                    "\n",
                ]
            )
        )
        fmriprep_workdir = os.path.join("$SLURM_TMPDIR", "fmriprep_work")
        write_job_footer(f, job_specs["jobname"], os.path.realpath(
            args.bids_path), fmriprep_workdir, args.derivatives_name, args.output_path)

    return job_path, outputs_exist


def submit_slurm_job(job_path):
    return subprocess.run(["sbatch", job_path])


def run_fmriprep(layout, args):

    subjects = args.participant_label
    if not subjects:
        subjects = layout.get_subjects()

    for subject in subjects:
        print(f"\t {subject}")
        if args.session_label:
            sessions = args.session_label
        else:
            sessions = layout.get_sessions(subject=subject)

        if args.preproc == "func":
            for session in sessions:
                # TODO: check if derivative already exists for that subject
                job_path, outputs_exist = write_func_job(
                    layout, subject, session, args)
                if outputs_exist:
                    print(
                        f"all output already exists for sub-{subject} ses-{session}, not rerunning"
                    )
                    continue
                yield job_path
        elif args.preproc == "anat":
            yield write_fmriprep_job(layout, subject, args, anat_only=True)
        elif args.preproc == "all":
            yield write_fmriprep_job(layout, subject, args, anat_only=False)


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="create fmriprep jobs scripts",
    )
    parser.add_argument(
        "bids_path",
        help="BIDS folder to run fmriprep on."
    )
    parser.add_argument(
        "derivatives_name",
        help="name of the output folder in derivatives.",
    )
    parser.add_argument(
        "--output-path",
        action="store",
        type=str,
        help="output path for SLURM files, logs and also bids filters"
    )
    parser.add_argument(
        "--preproc",
        action="store",
        default=PREPROC_DEFAULT,
        help="anat, func or all (default: all)"
    )
    parser.add_argument(
        "--slurm-account",
        action="store",
        default=SLURM_ACCOUNT_DEFAULT,
        help="SLURM account for job submission (default: rrg-pbellec)",
    )
    parser.add_argument(
        "--email",
        action="store",
        help="email for SLURM notifications"
    )
    parser.add_argument(
        "--container",
        action="store",
        default=FMRIPREP_DEFAULT_VERSION,
        help="fmriprep singularity container (default: fmriprep-20.2.1lts)"
    )
    parser.add_argument(
        "--participant-label",
        action="store",
        nargs="+",
        help="a space delimited list of participant identifiers or a single "
        "identifier (the sub- prefix can be removed)",
    )
    parser.add_argument(
        "--output-spaces",
        action="store",
        nargs="+",
        default=OUTPUT_SPACES_DEFAULT,
        help="a space delimited list of templates as defined by templateflow "
        "(default: [\"MNI152NLin2009cAsym\", \"MNI152NLin6Asym\"])",
    )
    parser.add_argument(
        "--fmriprep-args",
        action="store",
        type=str,
        nargs='+',
        default="",
        help="additionnal arguments to the fmriprep command as a string (ex: --fmriprep-args=\"--fs-no-reconall --use-aroma\") ",
    )
    parser.add_argument(
        "--session-label",
        action="store",
        nargs="+",
        help="a space delimited list of session identifiers or a single "
        "identifier (the ses- prefix can be removed)",
    )
    parser.add_argument(
        "--force-reindex",
        action="store_true",
        help="Force pyBIDS reset_database and reindexing",
    )
    parser.add_argument(
        "--submit",
        action="store_true",
        help="Submit SLURM jobs",
    )
    parser.add_argument(
        "--bids-filter",
        action="store",
        help="Path to an optionnal bids_filter.json template",
    )
    parser.add_argument(
        "--time",
        action="store",
        help="Time duration for the slurm job in slurm format (dd-)hh:mm:ss "
        "(default: 24h structural, 12h functionnal)",
    )
    parser.add_argument(
        "--mem-per-cpu",
        action="store",
        help="upper bound memory limit for fMRIPrep processes"
        "(default: 4096MB)",
    )
    parser.add_argument(
        "--cpus",
        action="store",
        help="maximum number of cpus for all processes"
        "(default: 16)",
    )

    return parser.parse_args()


def main():

    args = parse_args()
    print("\n### Running fmriprep-slurm\n")
    print(vars(args))

    print("\n# Loading pyBIDS database (it might take few hours for a big dataset)...\n")
    sing_bids_path = os.path.join(
        SINGULARITY_DATA_PATH, os.path.basename(args.bids_path))
    layout = bids.BIDSLayout(
        sing_bids_path,
        reset_database=args.force_reindex,
        ignore=(
            "code",
            "stimuli",
            "sourcedata",
            "models",
            re.compile(r"^\."),
        )
        + load_bidsignore(sing_bids_path),
    )
    job_path = os.path.join(SINGULARITY_OUTPUT_PATH, SLURM_JOB_DIR)
    if not os.path.exists(job_path):
        os.mkdir(job_path)
    
    print("\n# Prefectch templateflow templates ...\n")
    # prefectch templateflow templates
    os.environ["TEMPLATEFLOW_HOME"] = TEMPLATEFLOW_HOME
    tf_api.get(args.output_spaces + ["OASIS30ANTs", "fsLR", "fsaverage"])

    print("\n# Processing slurm files into {}\n".format(
        os.path.join(args.output_path, SLURM_JOB_DIR)))
    for job_file in run_fmriprep(layout, args):
        if args.submit:
            submit_slurm_job(job_file)


if __name__ == "__main__":
    main()
