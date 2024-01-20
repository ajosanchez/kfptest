from pathlib import Path
import tempfile
from typing import Optional
import pickle
from collections import namedtuple
from copy import copy, deepcopy
import inspect
import shutil
import importlib
import logging
import sys

import kfp
from kfp.dsl import *


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
TestStepOutput = namedtuple('TestStepOutput', ['output', 'outputs', 'obj'])

def prep_container_run(io_dir: str|Path, component_path: str, component_name: str):
    """Prep and kickoff TestStep.run() from within a container
    
    Parameters:
    io_dir -- the directory inside the container to load the component run args and python code
    component_path -- path the python file which contains the component code
    component_name -- name of the component to load from the module at the component_path
    """
    io_dir = Path(io_dir)

    # Load component: it cannot be pickled so it must be loaded via import in the container
    module_name = f".{Path(component_path).stem}"
    module = importlib.import_module(name=module_name, package="code")
    component = getattr(module, component_name)

    # load run arguments from filesystem, add component 
    with open(io_dir / "run_args", "rb") as f:
        run_args = pickle.load(f)
        run_args["kfp_component"] = component

    # run the component
    component_run = TestStep(**run_args).run()

    # persist binary of output obj to filesystem
    # since these are not the actual outputs, but a ref to them, this should go in the io_dir
    with open(io_dir / "component_run", "wb") as f:
        pickle.dump(component_run, f)

def is_kfp_artifact(obj):
    """Check if object is a kfp Atrifact"""
    return obj.__class__.__base__.__name__ == 'Artifact'


class ContainerTestStep():
    """Todo: place class docstring here"""

    def __init__(self, kfp_component, component_args: dict, base_path: Optional[Path|str]=None):
        """Init a ContainerTestStep which can run a kfp component within a docker container
        
        Parameters:
        kfp_component -- component to run within a container
        component_args -- parameter values to pass to the component at runtime
        base_path -- directory in which to store the component output folder as well as the io_dir (e.g., a test's tmpdir)
        """
        import docker

        client = docker.from_env()
        self.component = kfp_component
        self.component_args = component_args

        # Set local base_path where args, code, outputs should be written
        if base_path is not None:
            self.base_path = Path(base_path).absolute()
        else:
            self.base_path = Path(tempfile.mkdtemp()).absolute()

        # Create local folders for arguments and code
        self.local_io_dir = Path(tempfile.mkdtemp(dir=str(self.base_path)))
        self.local_code_dir = Path(tempfile.mkdtemp(dir=str(self.base_path)))
        
        # get container's working_dir to know where to mount volumes from
        self.image = kfp_component.component_spec.implementation.container.image
        self.container_working_dir = client.images.get(self.image).attrs.get("Config").get("WorkingDir")
        
        # Find filename of file containing the component function
        self.component_path = Path(inspect.getabsfile(self.component.python_func)) # path to .py file containing component
    

    def _write_container_requirements(self):
        """
        Write component_args to binary so it can be loaded at container exec and copy the kfptest package as well
        as the component function's .py file.
        """
        # Write arguments to pickle: write arguments for containertized-component-run to local filesystem
        with open(self.local_io_dir / "run_args", "wb") as f:
            run_args = {"component_args": self.containerized_component_args, "base_path": "component_output"}
            pickle.dump(run_args, f)
            logging.info("component args written to %s", str(self.local_io_dir / "run_args"))
        
        # Copy kfptest package
        kfp_dir = Path(inspect.getabsfile(TestStep)).parents[2]
        logging.info("kfptest package code found at %s", kfp_dir)
        shutil.copytree(str(kfp_dir), f"{self.local_code_dir}/kfptest", dirs_exist_ok=True)
        logging.info("kfptest package code copied to %s/kfptest", self.local_code_dir)
        
        # Copy .py file containging component function
        logging.info("component code file found at %s", self.component_path)
        shutil.copyfile(self.component_path, self.local_code_dir / self.component_path.name)
        logging.info("component code file copied to %s", str(self.local_code_dir / self.component_path.name))

        # Create an __init__.py file:
        with open(self.local_code_dir / "__init__.py", "w") as f:
            pass
        
    def _process_outputs(self):
        # Remove s3:// or gs:// prefixes from artifacts for local filesystem testing
        kfp.dsl.types.artifact_types._GCS_LOCAL_MOUNT_PREFIX = ""
        kfp.dsl.types.artifact_types._S3_LOCAL_MOUNT_PREFIX = ""
        kfp.dsl.types.artifact_types._MINIO_LOCAL_MOUNT_PREFIX = ""
        
        # Load container outputs into Python obj
        logging.info("Loading container output: %s", str(self.local_io_dir / "component_run"))
        with open(self.local_io_dir / "component_run", "rb") as f:
            component_run = pickle.load(f)

        # Fix artifact paths from container paths to local filesystem paths
        outputs = copy(component_run.outputs)
        for k, v in outputs.items():
            if is_kfp_artifact(v):
                container_path = Path(component_run.outputs[k].path)
                component_run.outputs[k].uri = f"gs://{self.base_path}/{container_path.parts[-2]}/{container_path.parts[-1]}"

        return component_run

    def run(self):
        """Run the component inside its docker conatiner:
          1. create docker volume bindings for io, output and code
          2. translate local input artifact paths into container paths
          3. populate io_dir with component args and the code_dir with code files
          4. run the component inside its container
          5. load container output and translate container output artifact paths to local paths
        """
        import docker
        client = docker.from_env()
        
        # prep for container run
        volumes = {}

        # create io dir mapping
        container_io_dir = f"{self.container_working_dir}/component_io"
        io_dir_mapping = {self.local_io_dir: {"bind": container_io_dir}}
        volumes.update(io_dir_mapping)

        # create output dir mapping
        local_base_dir = str(self.base_path) # container will create an output folder here which we can access later
        container_outputs_dir = f"{self.container_working_dir}/component_output"
        outputs_dir_mapping = {local_base_dir: {"bind": container_outputs_dir}}
        volumes.update(outputs_dir_mapping)

        # create code dir mapping
        container_code_dir = f"{self.container_working_dir}/code"
        #container_code_dir = f"{self.container_working_dir}/mnt"
        code_dir_mapping = {self.local_code_dir: {"bind": container_code_dir}}
        volumes.update(code_dir_mapping)

        # convert artifact paths from local to container
        self.containerized_component_args = deepcopy(self.component_args)

        kfp.dsl.types.artifact_types._GCS_LOCAL_MOUNT_PREFIX = ""
        kfp.dsl.types.artifact_types._S3_LOCAL_MOUNT_PREFIX = ""
        kfp.dsl.types.artifact_types._MINIO_LOCAL_MOUNT_PREFIX = ""

        for name, input_ in self.component_args.items():
            if is_kfp_artifact(input_):
                # change everything up to the last folder to be the container_outputs_dir
                local_artifact_path = Path(input_.path)
                container_path = f"{container_outputs_dir}/{local_artifact_path.parts[-2]}/{local_artifact_path.parts[-1]}"
                self.containerized_component_args[name].uri = f"gs://{container_path}"

        # write component_args and copy code to proper folders
        self._write_container_requirements()

        # run component in container
        component_path = f"{container_code_dir}/{self.component_path.name}"
        component_name = self.component.python_func.__name__ #kfp will replace "_" with "-" so component.name won't work all the time
        pip_command = f"install kfp==2.3.0 {container_code_dir}/kfptest"
        python_command = f'\'from kfptest import prep_container_run;prep_container_run(io_dir="{container_io_dir}", component_path="{component_path}", component_name="{component_name}")\''
        command = ['/bin/bash', '-c', f"pip {pip_command} && python -c {python_command}"]
        logging.info("Running component in container with command: %s", command)
        logging.info("Conatiner volume mappings: %s", str(volumes))
        client.containers.run(self.image, command, volumes=volumes)

        # conver artifact paths from container to local
        component_run = self._process_outputs()
        return component_run


class TestStep():
    def __init__(self, kfp_component, component_args, base_path: Optional[Path|str]=None):
        """Init a TestStep
        
        Parameters:
        kfp_component -- kfp component to run
        component_args -- parameter values to pass to the component at runtime
        base_path -- directory in which to store the component output folder as well as the io_dir (e.g., a test's tmpdir)
        """
        self.component = kfp_component
        self.component_args = component_args
        self.base_path = Path(base_path or tempfile.mkdtemp())
        self.inputs  = kfp_component.component_spec.inputs  # anything that's a component input
        self.outputs = kfp_component.component_spec.outputs # anything that's a component output
        self.input_artifacts   = {name: spec.type for name, spec in self.inputs.items()  if spec.type.startswith("system.")}
        self.output_artifacts  = {name: spec.type for name, spec in self.outputs.items() if spec.type.startswith("system.")}
        self.input_artifacts_dir  = Path(tempfile.mkdtemp(dir=str(self.base_path.absolute())))
        self.output_artifacts_dir = Path(tempfile.mkdtemp(dir=str(self.base_path.absolute())))
        self.non_artifact_args = {k: v for k, v in self.component_args.items() if k not in list(self.input_artifacts)}

    def _is_kfp_artifact(self, obj) -> bool:
        return obj.__class__.__base__.__name__ == 'Artifact'

    def _make_kfp_artifact(self, type_: str, path: Optional[str|Path]=None, name: Optional[str]=None):
        """Makes a kfp artifact for any kfp Input passed to the component as a python object"""
        kfp.dsl.types.artifact_types._GCS_LOCAL_MOUNT_PREFIX = ""
        artifact_type = type_.replace('@', '.').split('.')[1]
        artifact = globals()[artifact_type]()
        if path is not None:
            artifact.uri = f"gs://{path}/{name}"
        return artifact
    
    def _make_exec_args(self) -> dict:
        """Creates a dict containing all arguments (Inputs, Outputs, parameters) required to execute the component"""
        exec_args = {}
        
        # add all input artifacts to exec_args
        # convert any non-kfp artifacts to kfp artifacts and write them to filesystem
        for name in self.input_artifacts:
            artifact = self.component_args[name]
        
            if self._is_kfp_artifact(artifact): # if input is an artifact from a prev run
                exec_args[name] = artifact
            else:
                artifact_type = self.input_artifacts[name]
                kfp_artifact = self._make_kfp_artifact(artifact_type, self.input_artifacts_dir, name)
                exec_args[name] = kfp_artifact
                with open(kfp_artifact.path, "wb") as f: # write the python obj to the kfp-artifact path
                    pickle.dump(artifact, f)

        # add all input parameters to exec_args
        for name, value in self.non_artifact_args.items():
            exec_args[name] = value

        # add all output artifacts to exec_args
        for name, artifact_type in self.output_artifacts.items():
            kfp_artifact = self._make_kfp_artifact(artifact_type, self.output_artifacts_dir, name)
            exec_args[name] = kfp_artifact

        return exec_args
    
    def run(self):
        """Runs the component locally, loads any outputs and returns them as a NamedTuple similar to existing kfp pattern"""
        kwargs = self._make_exec_args()
        response = self.component.execute(**kwargs)

        if isinstance(response, tuple):
            outputs = response._asdict()
            output = None
        else:
            outputs = {}
            output = response
        
        # create an obj dict to hold outputs as python objects in memory
        obj = copy(outputs)

        # add output kfp artifacts to final output and outputs as objects to obj
        for name in self.output_artifacts:
            # update with the kfp artifact
            outputs.update({name: kwargs[name]})
            # load artifact from filesystem into obj dict
            with open(kwargs[name].path, "rb") as f:
                obj[name] = pickle.load(f)
        
        return TestStepOutput(output, outputs, obj)
