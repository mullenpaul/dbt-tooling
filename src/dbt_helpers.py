from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest


def build_dbt_params(target_folder):
    return ["--project-dir", target_folder, "--profiles-dir", target_folder]


def run_parse(runner: dbtRunner, target_folder, args=[]):
    params = ["parse"]
    params.extend(build_dbt_params(target_folder))
    params.extend(args)
    return runner.invoke(params)

def get_manifest(target_folder) -> Manifest:
    dbt = dbtRunner()
    res = run_parse(dbt, target_folder)
    return res.result

def run_deps(target_folder):
    runner = dbtRunner()
    params = ["parse"]
    params.extend(build_dbt_params(target_folder))
    runner.invoke(params)

def run_seed(target_folder):
    runner = dbtRunner()
    params = ["seed"]
    params.extend(build_dbt_params(target_folder))
    runner.invoke(params)

def get_node_type(nodes, t):
    return [i for i in nodes.values() if type(i) is t]
