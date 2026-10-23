import unittest

from benchmark.xgboost_udf_bench import (
    create_exasol_pandas_set_script,
    engine_split_threshold,
    tree_exasol_case_sql,
    tree_leaf_node,
)


class TestXGBoostBenchmarkHelpers(unittest.TestCase):
    def test_leaf_routing_uses_default_missing_branch(self):
        tree = {
            "nodeid": 0,
            "split": "f0",
            "split_condition": 1.5,
            "yes": 1,
            "no": 2,
            "missing": 2,
            "children": [
                {"nodeid": 1, "leaf": 0.1},
                {"nodeid": 2, "leaf": -0.2},
            ],
        }

        self.assertEqual(tree_leaf_node(tree, [1.0])["nodeid"], 1)
        self.assertEqual(tree_leaf_node(tree, [2.0])["nodeid"], 2)
        self.assertEqual(tree_leaf_node(tree, [float("nan")])["nodeid"], 2)

    def test_exasol_sql_tree_uses_case_and_missing_branch(self):
        tree = {
            "nodeid": 0,
            "split": "f0",
            "split_condition": 1.5,
            "yes": 1,
            "no": 2,
            "missing": 2,
            "children": [
                {"nodeid": 1, "leaf": 0.1},
                {"nodeid": 2, "leaf": -0.2},
            ],
        }

        sql = tree_exasol_case_sql(tree, ["amount"])

        self.assertIn("CASE WHEN amount IS NULL THEN -0.2", sql)
        self.assertIn(f"WHEN amount < {engine_split_threshold(1.5)!r} THEN 0.1", sql)

    def test_midpoint_threshold_preserves_float32_boundary(self):
        threshold = 49.2400017
        midpoint = engine_split_threshold(threshold)

        self.assertLess(49.24, midpoint)
        self.assertGreaterEqual(float(__import__("numpy").float32(49.24)), midpoint)

    def test_pandas_set_script_has_batching_and_compiles(self):
        artifact = {"trees": []}
        ddl = create_exasol_pandas_set_script(artifact)
        body = ddl.split(" AS\n", 1)[1].rsplit("\n/", 1)[0]

        self.assertIn("BATCH_SIZE = 4096", body)
        compile(body, "<generated Exasol pandas SET script>", "exec")


if __name__ == "__main__":
    unittest.main()
