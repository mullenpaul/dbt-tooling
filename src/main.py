from dbt.cli.main import dbtRunner
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode, GenericTestNode, SingularTestNode, UnitTestNode

import dbt_helpers
import sql_helpers


sql_helpers.create_sqlite("..\\data\\test.db")


if __name__ == '__main__':
    target_folder = "../jaffle_shop_duckdb"
    dbt = dbtRunner()

    #dbt_helpers.run_deps(target_folder)
    #dbt_helpers.run_deps(target_folder)
    
    result: Manifest = dbt_helpers.get_manifest(target_folder)

    models = dbt_helpers.get_node_type(result.nodes, ModelNode)

    sources = result.sources

    print("models")
    for i in models:
        print(i.name)
    n1 = result.nodes["model.jaffle_shop.stg_payments"]
    print(n1.node_info["unique_id"])
    print(n1.depends_on)

    singular_tests = dbt_helpers.get_node_type(result.nodes, SingularTestNode)
    print(singular_tests)
    generic_tests = dbt_helpers.get_node_type(result.nodes, GenericTestNode)
    print("gtests")
    for i in generic_tests:
        print(i.attached_node + ":" + i.name)
    unit_tests = dbt_helpers.get_node_type(result.nodes, UnitTestNode)
    print(unit_tests)

    n = result.nodes['test.jaffle_shop.accepted_values_stg_payments_payment_method__credit_card__coupon__bank_transfer__gift_card.3c3820f278']
    print(n.attached_node) # model which test points to!
