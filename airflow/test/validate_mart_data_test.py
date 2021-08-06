import time
import unittest
from unittest.mock import patch, Mock

from requests import Response, HTTPError

from dags.validate_mart_data import file_watcher_task


class TestValidateMartData(unittest.TestCase):

    @staticmethod
    def get_payload(minutes):
        return {
            'FileStatuses': {
                'FileStatus':
                    [
                        {
                            'modificationTime': (time.time() - minutes * 60) * 1000
                        }
                    ]
            }
        }

    @patch("dags.validate_mart_data.Variable")
    @patch("dags.validate_mart_data.requests.get")
    def test_validate_mart_data_should_raise_exception_if_URL_is_not_fount(self, mock_get, _):
        resp = Response()
        resp.status_code = 404
        resp.reason = "Mock Failure"
        mock_get.return_value = resp
        self.assertRaises(HTTPError, file_watcher_task)

    @patch("dags.validate_mart_data.Variable")
    @patch("dags.validate_mart_data.requests.get")
    def test_validate_mart_data_should_raise_if_file_was_not_modified_in_last_ten_minutes(self, mock_get, _):
        resp = Response()
        resp.status_code = 200
        resp.json = Mock(return_value=self.get_payload(15))
        mock_get.return_value = resp
        self.assertRaises(Exception, file_watcher_task, msg="No new file has been processed in last 10 mins!!")

    @patch("dags.validate_mart_data.Variable")
    @patch("dags.validate_mart_data.requests.get")
    def test_validate_mart_data_should_pass_if_file_was_modified_in_last_ten_minutes(self, mock_get, _):
        resp = Response()
        resp.status_code = 200
        resp.json = Mock(return_value=self.get_payload(9))
        mock_get.return_value = resp
        file_watcher_task()
