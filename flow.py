import prefect
from datavertex import FlowProcessor, Workspace
import pipeline
from prefect import Task, task, Flow, Parameter
from datetime import datetime

def run_node(node):
    workspace: Workspace = Workspace()
    workspace.set_execution_timestamp(datetime.now(), truncate='day')
    executor = FlowProcessor(pipeline.flow, workspace)
    executor.execute_node(node, parameters={})

class NodeTask(Task):

    def __init__(self, node, **kwargs):
        self.node = node
        super().__init__(**kwargs)

    def run(self):
        return run_node(self.node)

flow = Flow('item-price-tracker')
crawl_init = flow.add_task(NodeTask(pipeline.crawl_init))
crawl_all = flow.add_task(NodeTask(pipeline.crawl_all))
unique_webpages = flow.add_task(NodeTask(pipeline.unique_webpages))
scrape_all = flow.add_task(NodeTask(pipeline.scrape_all))
parse = flow.add_task(NodeTask(pipeline.parse))
relevancy_model = flow.add_task(NodeTask(pipeline.relevancy_model))
datamarts = flow.add_task(NodeTask(pipeline.datamarts))
flow.set_dependencies(crawl_all, [crawl_init])
flow.set_dependencies(unique_webpages, [crawl_all])
flow.set_dependencies(scrape_all, [unique_webpages])
flow.set_dependencies(parse, [scrape_all])
flow.set_dependencies(relevancy_model, [parse])
flow.set_dependencies(datamarts, [relevancy_model])

# Register the flow under the "tutorial" project
flow.register(project_name="Item Price Tracking")