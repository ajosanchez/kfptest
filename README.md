# kfptest
A module for running your kfp components locally and making them easy to test. Components can be run using a local environment or from their base_image Docker container.

## Some nice features of kfptest you might enjoy
* The output of a `TestStep` or `ContainerTestStep` run is a `NamedTuple` with `.output` and/or `.outputs` attributes that work just like kfp components
* The output of a `TestStep` or `ContainerTestStep` also has a `.obj` attribute which returns a dict of the outputs as the actual Python objects for easy testing and debugging
* Every `TestStep` or `ContainerTestStep` takes a base_path which can be a pytest `temp_path` for easy and clean testing

## Some things to keep in mind
In order to test a component in its base_image container:
1. You need to have the image already built on your machine
2. kfptest must have kfp installed in the container when testing the component. By default, kfptest will attempt to `pip install kfp` before running the component which takes about 15 seconds if it's not already installed. This also means that pip must be available for use in the base_image. 

## Usage
### Run a component using your local environment
```python
from kfptest import TestStep
from my_components import my_component

input_data = [5,4,3,2,1]
step = TestStep(my_component, {"name": "value", "input_dataset": input_data})
step_run = step.run()
print(step_run.obj)
```

### Run a component using its base_image Docker container
```python
from kfptest import ContainerTestStep
from my_components import my_component

input_data = [5,4,3,2,1]
step = ContainerTestStep(my_component, {"name": "value", "input_dataset": input_data})
step_run = step.run()
print(step_run.obj)
```

### Run a component using another component's output as input
```python
from kfptest import ContainerTestStep
from my_components import my_component

# Note: to chain components, you must use the same base_path for each component

input_data = [5,4,3,2,1]
step_1 = ContainerTestStep(sample, {"name": "value", "input_dataset": input_data}, base_path="mydir")
step_1_run = step_1.run()

step_2 = ContainerTestStep(sample, {"name": "Alex", "input_dataset": step_1_run.outputs["output_dataset"]}, base_path="mydir")
step_2_run = step_2.run()
print(step_2_run.obj)
```
