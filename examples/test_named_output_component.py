from example_components import named_output_component
from kfptest import TestStep, ContainerTestStep


def test_local_env_example_component(tmp_path):
    component_args = {
    "firstname": "Jeffrey",
    "lastname": "Lebowski",
    "input_dataset": [1,2,3,4,5]
    }
    test_step = TestStep(named_output_component, component_args, tmp_path)
    test_run = test_step.run()
    assert test_run.obj["full_name"] == "Jeffrey Lebowski"
    assert test_run.obj["squared_dataset"] == [1, 4, 9, 16, 25]

def test_containerized_example_component(tmp_path):
    component_args = {
    "firstname": "Jeffrey",
    "lastname": "Lebowski",
    "input_dataset": [1,2,3,4,5]
    }
    test_step = ContainerTestStep(named_output_component, component_args, tmp_path)
    test_run = test_step.run()
    assert test_run.obj["full_name"] == "Jeffrey Lebowski"
    assert test_run.obj["squared_dataset"] == [1, 4, 9, 16, 25]
