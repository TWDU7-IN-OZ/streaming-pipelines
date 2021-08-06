import unittest

from airflow.models import DagBag


class AirflowTest(unittest.TestCase):

    def test_dags_load_with_no_errors(self):
        dag_bag = DagBag(dag_folder="../dags", include_examples=False)
        self.assertEqual(len(dag_bag.import_errors), 0)
