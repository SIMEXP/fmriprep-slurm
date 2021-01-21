import os
import sys
import argparse
import bids
import subprocess
import json
import re
import pathlib

script_dir = os.path.dirname(__file__)

PYBIDS_CACHE_PATH = ".pybids_cache"
SLURM_JOB_DIR = ".slurm"

SMRIPREP_REQ = {"cpus": 16, "mem_per_cpu": 4096, "time": "24:00:00", "omp_nthreads": 8}
FMRIPREP_REQ = {"cpus": 16, "mem_per_cpu": 4096, "time": "12:00:00", "omp_nthreads": 8}

FMRIPREP_DEFAULT_VERSION = "fmriprep-20.2.1lts"
FMRIPREP_DEFAULT_SINGULARITY_PATH = os.path.abspath(
    os.path.join(script_dir, f"../../containers/{FMRIPREP_DEFAULT_VERSION}.sif")
)
BIDS_FILTERS_FILE = os.path.join(script_dir, "bids_filters.json")
TEMPLATEFLOW_HOME = os.path.join(
    os.environ.get("SCRATCH", os.path.join(os.environ["HOME"], ".cache")),
    "templateflow",
)
OUTPUT_TEMPLATES = ["MNI152NLin2009cAsym", "MNI152NLin6Asym"]
SINGULARITY_CMD_BASE = " ".join(
    [
        "singularity run",
        "--cleanenv",
        "-B $SLURM_TMPDIR:/work",  # use SLURM_TMPDIR to overcome scratch file number limit
        # f"-B /scratch/{os.environ['USER']}:/work",
        f"-B {TEMPLATEFLOW_HOME}:/templateflow",
        "-B /etc/pki:/etc/pki/",
    ]
)

slurm_preamble = """#!/bin/bash
#SBATCH --account={slurm_account}
#SBATCH --job-name={jobname}.job
#SBATCH --output={bids_root}/.slurm/{jobname}.out
#SBATCH --error={bids_root}/.slurm/{jobname}.err
#SBATCH --time={time}
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem-per-cpu={mem_per_cpu}M
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL
#SBATCH --mail-user={email}

export SINGULARITYENV_FS_LICENSE=$HOME/.freesurfer.txt
export SINGULARITYENV_TEMPLATEFLOW_HOME=/templateflow
"""


def load_bidsignore(bids_root):
    """Load .bidsignore file from a BIDS dataset, returns list of regexps"""
    bids_ignore_path = bids_root / ".bidsignore"
    if bids_ignore_path.exists():
        import re
        import fnmatch

        bids_ignores = bids_ignore_path.read_text().splitlines()
        return tuple(
            [
                re.compile(fnmatch.translate(bi))
                for bi in bids_ignores
                if len(bi) and bi.strip()[0] != "#"
            ]
        )
    return tuple()


def write_job_footer(fd, jobname):
    fd.write("fmriprep_exitcode=$?\n")
    # TODO: copy resource monitor output
    fd.write(
        f"cp $SLURM_TMPDIR/fmriprep_wf/resource_monitor.json /scratch/{os.environ['USER']}/{jobname}_resource_monitor.json \n"
    )
    fd.write(
        f"if [ $fmriprep_exitcode -ne 0 ] ; then cp -R $SLURM_TMPDIR /scratch/{os.environ['USER']}/{jobname}.workdir ; fi \n"
    )
    fd.write("exit $fmriprep_exitcode \n")


def write_fmriprep_job(layout, subject, args, anat_only=True):
    job_specs = dict(
        slurm_account=args.slurm_account,
        jobname=f"smriprep_sub-{subject}",
        email=args.email,
        bids_root=layout.root,
    )
    job_specs.update(SMRIPREP_REQ)
    job_path = os.path.join(layout.root, SLURM_JOB_DIR, f"{job_specs['jobname']}.sh")

    derivatives_path = os.path.join(layout.root, "derivatives", args.derivatives_name)

    # use json load/dump to copy filters (and validate json in the meantime)
    bids_filters_path = os.path.join(
        layout.root,
        SLURM_JOB_DIR,
        "bids_filters.json")
    bids_filters = json.load(open(BIDS_FILTERS_FILE))
    with open(bids_filters_path, 'w') as f:
        json.dump(bids_filters, f)

    pybids_cache_path = os.path.join(layout.root, PYBIDS_CACHE_PATH)

    fmriprep_singularity_path = args.container or FMRIPREP_DEFAULT_SINGULARITY_PATH

    with open(job_path, "w") as f:
        f.write(slurm_preamble.format(**job_specs))

        f.write(
            " ".join(
                [
                    SINGULARITY_CMD_BASE,
                    f"-B {layout.root}:{layout.root}",
                    fmriprep_singularity_path,
                    "-w /work",
                    f"--participant-label {subject}",
                    "--anat-only" if anat_only else "",
                    f"--bids-database-dir {pybids_cache_path}",
                    f"--bids-filter-file {bids_filters_path}",
                    "--output-spaces",
                    " ".join(OUTPUT_TEMPLATES),
                    "--cifti-output 91k",
                    "--skip_bids_validation",
                    "--write-graph",
                    f"--omp-nthreads {job_specs['omp_nthreads']}",
                    f"--nprocs {job_specs['cpus']}",
                    f"--mem_mb {job_specs['mem_per_cpu']*job_specs['cpus']}",
                    layout.root,
                    derivatives_path,
                    "participant",
                    "\n",
                ]
            )
        )
        write_job_footer(f, job_specs["jobname"])
    return job_path


def write_func_job(layout, subject, session, args):
    outputs_exist = False
    study = os.path.basename(layout.root)
    anat_path = os.path.join(
        os.path.dirname(layout.root),
        "anat",
        "derivatives",
        args.derivatives_name,
    )
    derivatives_path = os.path.join(layout.root, "derivatives", args.derivatives_name)

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
            ("space", OUTPUT_TEMPLATES[0]),
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
        bold_deriv = os.path.lexists(preproc_path) and os.path.lexists(dtseries_path)
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
        bids_root=layout.root,
    )
    job_specs.update(FMRIPREP_REQ)

    job_path = os.path.join(layout.root, SLURM_JOB_DIR, f"{job_specs['jobname']}.sh")
    bids_filters_path = os.path.join(
        layout.root,
        SLURM_JOB_DIR,
        f"{job_specs['jobname']}_bids_filters.json"
    )

    pybids_cache_path = os.path.join(layout.root, PYBIDS_CACHE_PATH)

    # filter for session
    bids_filters = json.load(open(BIDS_FILTERS_FILE))
    bids_filters["bold"].update({"session": session})
    with open(bids_filters_path, "w") as f:
        json.dump(bids_filters, f)

    fmriprep_singularity_path = args.container or FMRIPREP_DEFAULT_SINGULARITY_PATH

    with open(job_path, "w") as f:
        f.write(slurm_preamble.format(**job_specs))
        f.write(
            " ".join(
                [
                    SINGULARITY_CMD_BASE,
                    f"-B {layout.root}:{layout.root}",
                    f"-B {anat_path}:/anat",
                    fmriprep_singularity_path,
                    "-w /work",
                    f"--participant-label {subject}",
                    "--anat-derivatives /anat/fmriprep",
                    "--fs-subjects-dir /anat/freesurfer",
                    f"--bids-database-dir {pybids_cache_path}",
                    f"--bids-filter-file {bids_filters_path}",
                    "--ignore slicetiming",
                    "--use-syn-sdc",
                    "--output-spaces",
                    *OUTPUT_TEMPLATES,
                    "--cifti-output 91k",
                    "--notrack",
                    "--write-graph",
                    "--skip_bids_validation",
                    f"--omp-nthreads {job_specs['omp_nthreads']}",
                    f"--nprocs {job_specs['cpus']}",
                    f"--mem_mb {job_specs['mem_per_cpu']*job_specs['cpus']}",
                    # monitor resources to design a heuristic for runtime/cpu/ram of func data
                    "--resource-monitor",
                    layout.root,
                    derivatives_path,
                    "participant",
                    "\n",
                ]
            )
        )
        write_job_footer(f, job_specs["jobname"])

    return job_path, outputs_exist


def submit_slurm_job(job_path):
    return subprocess.run(["sbatch", job_path])


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="submit smriprep jobs",
    )
    parser.add_argument(
        "bids_path", type=pathlib.Path, help="BIDS folder to run smriprep on."
    )
    parser.add_argument(
        "derivatives_name",
        type=pathlib.Path,
        help="name of the output folder in derivatives.",
    )
    parser.add_argument("preproc", help="anat or func")
    parser.add_argument(
        "--slurm-account",
        action="store",
        default="rrg-pbellec",
        help="SLURM account for job submission",
    )
    parser.add_argument("--email", action="store", help="email for SLURM notifications")
    parser.add_argument(
        "--container", action="store", help="fmriprep singularity container"
    )
    parser.add_argument(
        "--participant-label",
        action="store",
        nargs="+",
        help="a space delimited list of participant identifiers or a single "
        "identifier (the sub- prefix can be removed)",
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
        "--no-submit",
        action="store_true",
        help="Generate scripts, do not submit SLURM jobs, for testing.",
    )
    return parser.parse_args()


def run_fmriprep(layout, args, pipe="all"):

    subjects = args.participant_label
    if not subjects:
        subjects = layout.get_subjects()

    for subject in subjects:
        if args.session_label:
            sessions = args.session_label
        else:
            sessions = layout.get_sessions(subject=subject)

        if pipe == "func":
            for session in sessions:
                # if TODO: check if derivative already exists for that subject
                job_path, outputs_exist = write_func_job(layout, subject, session, args)
                if outputs_exist:
                    print(
                        f"all output already exists for sub-{subject} ses-{session}, not rerunning"
                    )
                    continue
                yield job_path
        elif pipe == "anat":
            yield write_fmriprep_job(layout, subject, args, anat_only=True)
        elif pipe == "all":
            yield write_fmriprep_job(layout, subject, args, anat_only=False)


def main():

    args = parse_args()

    pybids_cache_path = os.path.join(args.bids_path, PYBIDS_CACHE_PATH)

    layout = bids.BIDSLayout(
        args.bids_path,
        database_path=pybids_cache_path,
        reset_database=args.force_reindex,
        ignore=(
            "code",
            "stimuli",
            "sourcedata",
            "models",
            re.compile(r"^\."),
        )
        + load_bidsignore(args.bids_path),
    )

    job_path = os.path.join(layout.root, SLURM_JOB_DIR)
    if not os.path.exists(job_path):
        os.mkdir(job_path)
        # add .slurm to .gitignore
        with open(os.path.join(layout.root, ".gitignore"), "a+") as f:
            f.seek(0)
            if not any([SLURM_JOB_DIR in l for l in f.readlines()]):
                f.write(f"{SLURM_JOB_DIR}\n")

    # prefectch templateflow templates
    os.environ["TEMPLATEFLOW_HOME"] = TEMPLATEFLOW_HOME
    import templateflow.api as tf_api

    tf_api.get(OUTPUT_TEMPLATES + ["OASIS30ANTs", "fsLR", "fsaverage"])

    for job_file in run_fmriprep(layout, args, args.preproc):
        if not args.no_submit:
            submit_slurm_job(job_file)


if __name__ == "__main__":
    main()
