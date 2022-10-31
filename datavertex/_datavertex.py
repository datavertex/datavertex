from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
import json
from textwrap import shorten
from typing import Any, Iterable, Optional, Union, TYPE_CHECKING
import pandas as pd
from enum import Enum
import jsonlines
import os
import papermill
from glob import glob
from datetime_truncate import truncate as truncate_date
import importlib

from .internal import find_notebook_in_local_dir

from uritemplate import variables

if TYPE_CHECKING:
    from datavertex.notebook import Notebook
    from pandas import DataFrame

class ResourceConnectionType(Enum):
    LOCAL_FILESYSTEM = 100,
    AWS_S3 = 200,
    DATABASE = 300

# TODO: Rework this to single location field with ability to put custom ResourceLocation class instead of string
class LocationRoot(Enum):
    """Determines base directory or location to calculate paths.

        DATA:
            Local Mode: [PWD]/local/data/
            Production Mode: must be supplied via workspace

        REPORTS:
            Local Mode: [PWD]/local/reports/
            Production Mode: must be supplied via workspace
    
        WORKING_DIR:
            Local Mode: [PWD]/
            Production Mode: [PWD]/

        TEMP:
            Local Mode: Temporary directory allocated by current OS
            Production Mode: Temporary directory allocated by current OS
    """
    AUTO = 0,
    DATA = 100,
    REPORTS = 200,
    WORKING_DIR = 300,
    TEMP = 900

class ResourceFormat(Enum):
    AUTO = 0
    CSV = 1
    JSON = 2
    JSONLINES = 3
    DATABASE = 4

class WorkspaceType(Enum):
    DEVELOPMENT = 10
    LOCAL = 20

@dataclass
class Resource:
    name: str
    format: ResourceFormat = field(default=ResourceFormat.AUTO)
    mode: Optional[str] = None
    location: Optional[str] = None
    location_root: LocationRoot = LocationRoot.DATA
    spec: Optional[str] = None

    def get_connection_type(self) -> ResourceConnectionType:
        
        return ResourceConnectionType.LOCAL_FILESYSTEM

    def read_dataframe(self):

        raise Exception("This method must overriden in sub class. Something is wrong here.")
        
        if self.path_pattern is not None:
            path = self.get_path()
            if path.endswith(".json"):
                return pd.read_json(path, orient='records', lines=True)

        if self.prototype is not None:
            if self.prototype.endswith(".csv"):
                return pd.read_csv(self.prototype)

    def open(self):
        if format == 'jsonlines':
            print(self.get_path())

@dataclass
class ExecutionContext:
    notebook_parameters:dict = field(default_factory=dict)
    workspace:dict = field(default_factory=dict)

    def register_parameter(self, name, value):
        self.notebook_parameters[name] = value

@dataclass 
class Workspace:
    id: str = 'local'
    reports_root_location: str = 'local/executions/'
    data_root_location: str = 'local/data/'
    type: WorkspaceType = WorkspaceType.DEVELOPMENT
    variables: dict[str, Any] = field(default_factory=dict)

    def set_execution_timestamp(self, dt: datetime, truncate: str):
        self.variables['execution_timestamp'] = truncate_date(dt, truncate).isoformat().replace(':', '-')

    def get_variables(self):
        return self.variables

@dataclass
class NodeProcessor:
    flow: Flow
    node: Node

@dataclass
class ResourceProcessor:
    workspace: Workspace
    resource: Resource
    execution_context: ExecutionContext = field(default_factory=ExecutionContext)

    def get_location(self):
        return self.get_processor(self.resource).get_location()

    def get_processor(self, resource: Resource) -> ResourceProcessor:

        processors = {
            ResourceConnectionType.LOCAL_FILESYSTEM: LocalFileResourceProcessor
        }

        resource_connection_type: ResourceConnectionType = self.resource.get_connection_type()
        return processors[resource_connection_type](self.workspace, self.resource, self.execution_context)

    def read_jsonlines(self) -> Iterable[str]:
        return self.get_processor(self.resource).read_jsonlines()

    def read_dataframe(self, mode: str = 'full', **kwargs) -> pd.DataFrame:
        resource_connection_type: ResourceConnectionType = self.resource.get_connection_type()
        if (resource_connection_type == ResourceConnectionType.LOCAL_FILESYSTEM):
            prc = LocalFileResourceProcessor(self.workspace, self.resource)
            return prc.read_dataframe(mode, **kwargs)

    def write_jsonlines(self, records: Iterable[dict]):
        return self.get_processor(self.resource).write_jsonlines(records)

    def fetch_resource_spec(self):
        spec_id = self.resource.name
        if (self.resource.spec):
            print(f'INFO: Resource has a defined spec_id "{self.resource.spec}"')
            spec_id = self.resource.spec

        spec_path = os.path.join('resources', spec_id + '.json')

        if os.path.exists(spec_path):
            #TODO: Implement proper logging instead of prints
            print('Validating the dataframe according to spec: ' + spec_path)
            with open(spec_path, 'rt') as f:
                spec = json.load(f)
            #print("done")
            #print(spec)
            return spec
        else:
            print("INFO: Spec for dataframe not found. " + spec_path)
            return None
        

    def validate_dataframe_specs(self, dataframe: DataFrame):
        # TODO: Implement Dataframe validation properly, as a separate class
        spec = self.fetch_resource_spec()
        if (spec):
            columns = spec['definition']['columns']
            allow_unknown_columns = spec['definition'].get('allowUnknownColumns', False)
            mandatory_columns = [k for k, v in columns.items() if v.get('optional', True) == True]
            if allow_unknown_columns:
                if not set(dataframe.columns).issuperset(set(mandatory_columns)):
                    missing_columns = set(mandatory_columns) - set(dataframe.columns)
                    raise AssertionError(f"Dataframe columns are missing: {missing_columns}")
            else:
                if not set(dataframe.columns) == set(mandatory_columns):
                    missing_columns = set(mandatory_columns) - set(dataframe.columns)
                    extra_columns = set(dataframe.columns) - set(mandatory_columns)
                    raise AssertionError(f'Dataframe has missing columns: {missing_columns}.\n Dataframe has extra columns that are not allowed by spec: {extra_columns}')

    def write_dataframe(self, dataframe: DataFrame):
        self.validate_dataframe_specs(dataframe)
        return self.get_processor(self.resource).write_dataframe(dataframe)



        #if path.endswith('json') or path.endswith('json.gz'): 
        #    dataframe.to_json(path, lines=True, orient='records')

@dataclass
class LocalFileResourceProcessor(ResourceProcessor):

    def get_location(self, params=None, must_exist=False, read_glob=False) -> str:
        
        variables = self.workspace.get_variables()

        variables['resource_id'] = self.resource.name

        parameters = self.execution_context.notebook_parameters.copy()

        for k, v in parameters.items():
            if type(v) == str and len(v) > 16:
                parameters[k] = str(hash(v))[:8] 

        def shorten_values(d):
            r = d.copy()
            for k, v in d.items():
                if type(v) != str:
                    del r[k]
                    continue
                if type(v) == str and len(v) > 16:
                    r[k] = str(hash(v))[:8]
                r[k] = r[k].replace(' ', '%20')
            return r

        if (self.execution_context is not None):
            variables['params'] = '__'.join([f"{k}={v}" for (k, v) in sorted(shorten_values(parameters).items())])

        location_roots = {
            LocationRoot.DATA.name: self.workspace.data_root_location,
            LocationRoot.REPORTS.name: self.workspace.reports_root_location,
            LocationRoot.WORKING_DIR.name: None
        }

        if self.resource.location is not None:
            root_dir = location_roots[self.resource.location_root.name]
            if read_glob == True:
                relative_location = self.resource.location.replace('{params}', '*').format(**variables)      
            else:
                relative_location = self.resource.location.format(**variables)

            if root_dir is not None:
                full_path = os.path.join(root_dir, relative_location)
            else:
                full_path = relative_location

            # TODO: Handle globs properly
            if read_glob == False and must_exist and not os.path.exists(full_path):
                raise Exception(f"Resource is not available: {full_path}")

            return full_path
        else:
            raise Exception("Cannot determine path. One of these must be filled: location")

    def read_dataframe(self, mode: str = 'full', **kwargs) -> pd.DataFrame:
        path = self.get_location(must_exist=True, read_glob=True)
        print(f"Reading DataFrame from location: {path}")

        if '*' in path:
            print("Location is a wildcard pattern")

            if (path.endswith(".json") or path.endswith(".jsonl") or path.endswith(".jl")):
                return pd.concat([pd.read_json(l, orient='records', lines=True, **kwargs) for l in glob(path)]).reset_index(drop=True)

            if (path.endswith(".csv") or path.endswith(".csv.gz")):
                return pd.concat([pd.read_csv(l, **kwargs) for l in glob(path)]).reset_index(drop=True)


        if (path.endswith(".json") or path.endswith(".jsonl") or path.endswith(".jl")):

            # TODO: Handle mode = 'chunk' with nrows param
            # TODO: Ability to change row limit
            # TODO: Implement logic for all data types

            nrows = None

            if self.resource.mode == 'chunk' == 'chunk':
                print("Mode: chunk")
                print("Limiting number of rows to 100")
                nrows = 50
            dataframe = pd.read_json(path, orient='records', lines=True, nrows=nrows, **kwargs)
        elif (path.endswith(".csv")):
            dataframe = pd.read_csv(path, **kwargs)
        elif (path.endswith(".parquet")):
            dataframe = pd.read_parquet(path, **kwargs)
        elif (path.endswith(".xlsx")):
            dataframe = pd.read_excel(path, **kwargs)
        else:
            raise Exception("Resource format not supported")
        return dataframe

    def write_jsonlines(self, records: Iterable[dict]):
        path = self.get_location()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        print(f"Writing JSONLines to location: {path}")
        if path.endswith('.json') or path.endswith('.json.gz'): 
            with jsonlines.open(path, 'w') as f:
                f.write_all(records)
    
    def write_dataframe(self, dataframe: DataFrame):
        path = self.get_location()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        print(f"Writing DataFrame to location: {path}")
        if path.endswith('.json') or path.endswith('.json.gz'): 
            dataframe.to_json(path, lines=True, orient='records')
        elif path.endswith('.csv') or path.endswith('.csv.gz'): 
            dataframe.to_csv(path, index=False)
        elif path.endswith('.parquet'): 
            dataframe.to_parquet(path)
        else:
            raise Exception("Resource format not supported")

@dataclass
class FlowProcessor:
    flow: Flow
    workspace: Workspace

    def infer_notebook_location(self, notebook: Node):
        # TODO: Implement method
        if self.workspace.type == WorkspaceType.DEVELOPMENT or self.workspace.type == WorkspaceType.LOCAL:
            location = find_notebook_in_local_dir('.', notebook.get_node_id())
            if location is not None:
                return location
            else:
                raise KeyError(notebook.get_node_id())
        raise Exception("Method is not fully implemented")

    def execute_iterator_node_rows(self, node: IteratorNode, parameters: dict):
        
        iterator_node: IteratorNode = node
        if iterator_node.iteration_node is None:
            raise Exception('Node to use for iteration not found')

        notebook_path = self.infer_notebook_location(node)
        
        execution_context = ExecutionContext(parameters)

        resource_processor = ResourceProcessor(self.workspace, self.flow.get_resource(iterator_node.resource_id), execution_context)
        dataframe = resource_processor.read_dataframe()

        print(f"Loaded DataFrame contains {dataframe.shape[0]} records.")
        print(f"Columns are: {', '.join(dataframe.columns.to_list())}")

        print("Executing for each record ")
        print(f"Running notebook: {notebook_path}")
        

        for index, row in dataframe.iterrows():
            #print(dict(row))
            parameters = dict(row)

            def shorten_values(d):
                r = d.copy()
                for k, v in d.items():
                    if type(v) != str:
                        del r[k]
                        continue
                    if type(v) == str and len(v) > 16:
                        r[k] = str(hash(v))[:8]
                    r[k] = r[k].replace(' ', '%20')
                return r

            workspace_vars = self.workspace.get_variables()
            node_vars = iterator_node.get_variables()

            variables = {}
            variables.update(workspace_vars)
            variables.update(node_vars)

            variables['params'] = '__'.join([f"{k}={v}" for (k, v) in sorted(shorten_values(parameters).items())])

            if iterator_node.iteration_node.report_location is not None:
                output_path_pattern = iterator_node.iteration_node.report_location.format(**variables)
            else:
                output_path_pattern = os.path.join(self.workspace.reports_root_location, notebook_path)

            
            output_path = os.path.join(self.workspace.reports_root_location, output_path_pattern)      
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            print(f"Execution output: {output_path}")

            # TODO: Add proper parameters injection
            #raise Exception("TODO")

            papermill.execute_notebook(notebook_path, output_path, parameters=parameters, request_save_on_cell_execute=False)

    def execute_iterator_node_files(self, node: IteratorNode, parameters: dict):
        
        iterator_node: IteratorNode = node
        if iterator_node.iteration_node is None:
            raise Exception('Node to use for iteration not found')

        notebook_path = self.infer_notebook_location(node)
        
        execution_context = ExecutionContext(parameters)

        resource_processor = ResourceProcessor(self.workspace, self.flow.get_resource(iterator_node.resource_id), execution_context)
        resource_processor
        dataframe = resource_processor.read_dataframe()

        print(f"Loaded DataFrame contains {dataframe.shape[0]} records.")
        print(f"Columns are: {', '.join(dataframe.columns.to_list())}")

        print("Executing for each record ")
        print(f"Running notebook: {notebook_path}")
        

        for index, row in dataframe.iterrows():
            #print(dict(row))
            parameters = dict(row)

            def shorten_values(d):
                r = d.copy()
                for k, v in d.items():
                    if type(v) != str:
                        del r[k]
                        continue
                    if type(v) == str and len(v) > 16:
                        r[k] = str(hash(v))[:8]
                    r[k] = r[k].replace(' ', '%20')
                return r

            workspace_vars = self.workspace.get_variables()
            node_vars = iterator_node.get_variables()

            variables = {}
            variables.update(workspace_vars)
            variables.update(node_vars)

            variables['params'] = '__'.join([f"{k}={v}" for (k, v) in sorted(shorten_values(parameters).items())])

            if iterator_node.iteration_node.report_location is not None:
                output_path_pattern = iterator_node.iteration_node.report_location.format(**variables)
            else:
                output_path_pattern = os.path.join(self.workspace.reports_root_location, notebook_path)

            
            output_path = os.path.join(self.workspace.reports_root_location, output_path_pattern)      
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            print(f"Execution output: {output_path}")

            # TODO: Add proper parameters injection
            #raise Exception("TODO")

            papermill.execute_notebook(notebook_path, output_path, parameters=parameters, request_save_on_cell_execute=False)


    def execute_node(self, node: Node, parameters: dict):
    
        workspace_json = {
            "variables": self.workspace.get_variables(),
            "data_root_location": self.workspace.data_root_location,
            "reports_root_location": self.workspace.reports_root_location,
            "type": self.workspace.type.name
        }

        with jsonlines.open('workspace_temp.json', 'w') as f:
            f.write(workspace_json)

        if isinstance(node, IteratorNode):
            if (node.iteration_mode == 'rows'):
                self.execute_iterator_node_rows(node, parameters)
            elif (node.iteration_mode == 'files'):
                self.execute_iterator_node_files(node, parameters)
            else:
                raise NotImplementedError('Only "rows" and "files" modes are supported.')

        elif isinstance(node, Node):
            notebook_path = self.infer_notebook_location(node)
            output_path = os.path.join(self.workspace.reports_root_location, notebook_path)

            print(f"Running notebook: {notebook_path}")
            print(f"Execution output: {output_path}")
            papermill.execute_notebook(notebook_path, output_path, parameters)

        else:
            raise Exception("Unknown type of Node: " + str(node))
        

@dataclass
class Flow:
    name: str
    resources: dict = field(default_factory=dict)

    def add_node(self, name: str, node: Node):
        pass

    def add_resources(self, resources: Iterable[Resource]):
        for resource in resources:
            self.add_resource(resource)

    def add_resource(self, resource: Resource):
        if resource.name in self.resources:
            raise Exception("Resource already exists")
        self.resources[resource.name] = resource
        pass

    def get_resource(self, name: str) -> Resource:
        return self.resources[name]

    def __rshift__(self, right):
        if type(right) == Node:
            target: Node
            target = right
            self.addNode(target)
        else:
            raise Exception("Can add to other Node or Flow")
        return right


@dataclass()
class Node:
    id: str
    flow: Optional[Flow] = None
    report_location: Optional[str] = None

    def get_node_id(self):
        return self.id

    def get_variables(self):
        return {
            "node_id": self.id
        }

    def for_each_file_in(self, resource_id: str):

        pass

    def for_each_record_in(self, resource_id: str):
        #if self.flow is None:
        #    raise Exception("Flow is not found")
        #self.flow.get_resource(resource_id)
        return IteratorNode(self.id + ' for_each_record_in ' + resource_id, \
                                    resource_id=resource_id, 
                                    flow=self.flow,
                                    iteration_node=self)
        

    def __rshift__(self, right):
        if type(right) == Flow:
            targetFlow: Flow
            targetFlow = right
            targetFlow.addNode(self)
        else:
            raise Exception("Can add to other Node or Flow")
        return right

@dataclass()
class IteratorNode(Node):
    iteration_node: Optional[Node] = None# = field(default_factory=)
    resource_id: str = field(default_factory=str)
    iteration_mode: str = field(default="rows")

    def get_node_id(self):
        return self.iteration_node.id

    def get_variables(self) -> dict:
        assert self.iteration_node
        return self.iteration_node.get_variables()

@dataclass
class FlowParser:
    
    def get_flow(self) -> Flow:

        spec = importlib.util.spec_from_file_location("pipeline", os.path.join(os.getcwd(), 'pipeline.py'))
        pipeline_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(pipeline_module)

        # print(dir(pipeline_module))

        for v in filter(lambda x: not '__' in x, dir(pipeline_module)):
            flow: Flow = getattr(pipeline_module, v)
            #print(f"{flow} : {type(flow)}")
            #print(str(type(flow)))
            if isinstance(flow, Flow) or str(type(flow)) == "<class 'datavertex._datavertex.Flow'>":
                return flow
        raise Exception("Flow not found")

@dataclass
class ResourceManager:
    workspace: Workspace = field(repr=False)
    resources: dict[str, ResourceProcessor] = field(init=False)

    def __post_init__(self):
        self.resources = dict()

    def refresh(self):
        flow = FlowParser().get_flow()
        
        self.resources.clear()
        for resource_id, resource in flow.resources.items():
            self.resources[resource_id] = ResourceProcessor(self.workspace, resource)