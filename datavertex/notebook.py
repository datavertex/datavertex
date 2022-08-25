from __future__ import annotations
from dataclasses import dataclass, field
import os
from typing import Any, Iterable, TYPE_CHECKING, Optional
from ._datavertex import Flow, FlowProcessor, Resource, ResourceManager, ResourceProcessor, Workspace
import pandas as pd
import jsonlines
import fnmatch

@dataclass
class Resources:

    notebook: Notebook 

    def __getattr__(self, key):
        return self.notebook.get_flow().resources[key]

    def __dir__(self) -> Iterable[str]:
        return ["a", "b"] #list(self.notebook.get_flow().resources.keys())

@dataclass
class Notebook:
    id: str
    outputs: dict[str, ResourceProcessor] = field(default_factory=dict, init=False)
    inputs: dict[str, ResourceProcessor] = field(default_factory=dict, init=False)
    parameters: dict[str, Any] = field(default_factory=dict, init=False)
    workspace: Workspace = field(default_factory=Workspace, init=False)
    resource_manager: ResourceManager = field(init=False)

    def __post_init__(self):
        self.resource_manager = ResourceManager(self.workspace)
        self.resource_manager.refresh()

        if os.path.exists('workspace_temp.json'):
            print("Loading workspace information from file.")
            with jsonlines.open('workspace_temp.json') as f:
                workspace_json = f.read()

            print("Workspace:")
            print(workspace_json)
            self.workspace.variables = workspace_json['variables']
            self.workspace.data_root_location = workspace_json['data_root_location']
            self.workspace.reports_root_location = workspace_json['reports_root_location']
        else:
            print("Workspace not found. Using default.")
        #global __notebook
        #if __notebook is None:
        #    raise Exception("Notebook is already initialized")
        #__notebook = self

    def param(self, name: str, value):
        self.parameters[name] = value
        for resource in self.resource_manager.resources.values():
            resource.execution_context.register_parameter(name, value)

    def add_inputs(self, resource_id_list: Iterable[str]):
        
        print("Input Resources:")
        for id in resource_id_list:

            matches = fnmatch.filter(self.resource_manager.resources.keys(), id)
            if len(matches) == 0:
                raise KeyError(id)

            for matched_id in matches:
                self.inputs[matched_id] = self.resource_manager.resources[matched_id]
                print(self.inputs[matched_id])
        return

    def add_outputs(self, resource_id_list: Iterable[str]):
        print("Output Resources:")
        for id in resource_id_list:

            matches = fnmatch.filter(self.resource_manager.resources.keys(), id)
            if len(matches) == 0:
                raise KeyError(id)

            for matched_id in matches:
                self.outputs[matched_id] = self.resource_manager.resources[matched_id]
                print(self.outputs[matched_id])
            # self.outputs[id] = self.resource_manager.resources[id]
            
        return

    def get_flow(self) -> Flow:
        import pipeline
        for v in filter(lambda x: not '__' in x, dir(pipeline)):
            flow: Flow = getattr(pipeline, v)
            if (type(flow) != Flow):
                continue
            return flow
        raise Exception("Flow not found")

    #def execute_notebook()
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        return

def main():
    if __name__== "__main__" :
        pass
main()