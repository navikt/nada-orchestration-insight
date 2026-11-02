import base64
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pandas as pd
from google.api_core.exceptions import NotFound, TooManyRequests
from google.cloud.bigquery.job import LoadJob

import main


TABLE_ID = "nada-prod-6977.knorten_north.airflow_dagruns"
STAGING_TABLE_ID = TABLE_ID + main.STAGING_POSTFIX


def rate_limited_job():
    job = Mock()
    job.state = "DONE"
    job.error_result = {"reason": "rateLimitExceeded"}
    job.result.side_effect = TooManyRequests("synthetic rate limit")
    return job


def dataframe():
    return pd.DataFrame(
        [
            {
                "task_id": "task",
                "task_duration": 1.0,
                "task_status": "success",
                "dag_id": "dag",
                "start_date": "2026-01-01",
                "end_date": "2026-01-01",
                "execution_date": "2026-01-01",
                "state": "success",
                "run_id": "run",
                "external_trigger": False,
                "run_type": "scheduled",
                "schedule_interval": "@daily",
                "max_active_tasks": 1,
                "max_active_runs": 1,
            }
        ]
    )


class StagingPreparationTests(unittest.TestCase):
    def test_existing_table_is_truncated_and_waited_for(self):
        client = Mock()
        truncate_job = Mock()
        events = []
        client.get_table.side_effect = lambda _: events.append("get_table")
        client.query.side_effect = lambda query, location: (
            events.append(("query", query, location)) or truncate_job
        )
        truncate_job.result.side_effect = lambda: events.append("truncate_done")

        main.prepare_staging_table(client, STAGING_TABLE_ID)

        self.assertEqual(
            events,
            [
                "get_table",
                ("query", f"TRUNCATE TABLE `{STAGING_TABLE_ID}`", "europe-north1"),
                "truncate_done",
            ],
        )

    def test_missing_table_is_not_truncated(self):
        client = Mock()
        client.get_table.side_effect = NotFound("synthetic missing table")

        main.prepare_staging_table(client, STAGING_TABLE_ID)

        client.query.assert_not_called()

    def test_other_preparation_error_propagates(self):
        client = Mock()
        client.get_table.side_effect = RuntimeError("synthetic preparation failure")

        with self.assertRaisesRegex(RuntimeError, "synthetic preparation failure"):
            main.prepare_staging_table(client, STAGING_TABLE_ID)

        client.query.assert_not_called()


class LoadRetryTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.data = dataframe()

    @patch("main.time.sleep")
    @patch("main.random.uniform", return_value=0.25)
    def test_terminal_rate_limit_resubmits_a_new_job(self, _uniform, _sleep):
        first_job = rate_limited_job()
        second_job = Mock()
        self.client.load_table_from_dataframe.side_effect = [first_job, second_job]

        main.load_namespace_to_staging(self.client, self.data, STAGING_TABLE_ID)

        self.assertEqual(self.client.load_table_from_dataframe.call_count, 2)
        self.assertEqual(first_job.result.call_count, 1)
        second_job.result.assert_called_once_with()

    @patch("main.time.sleep")
    @patch("main.random.uniform", return_value=0.25)
    def test_polling_rate_limit_retries_same_job(self, _uniform, _sleep):
        job = Mock()
        job.state = "PENDING"
        job.result.side_effect = [TooManyRequests("synthetic polling limit"), None]
        self.client.load_table_from_dataframe.return_value = job

        main.load_namespace_to_staging(self.client, self.data, STAGING_TABLE_ID)

        self.client.load_table_from_dataframe.assert_called_once()
        self.assertEqual(job.result.call_count, 2)

    @patch("main.time.sleep")
    def test_non_rate_terminal_429_fails(self, sleep):
        job = Mock()
        job.state = "DONE"
        job.error_result = {"reason": "backendError"}
        job.result.side_effect = TooManyRequests("synthetic terminal limit")
        self.client.load_table_from_dataframe.return_value = job

        with self.assertRaises(TooManyRequests):
            main.load_namespace_to_staging(self.client, self.data, STAGING_TABLE_ID)

        self.client.load_table_from_dataframe.assert_called_once()
        sleep.assert_not_called()

    @patch("main.time.sleep")
    def test_terminal_429_without_error_result_fails_without_retry(self, sleep):
        job = Mock()
        job.state = "DONE"
        job.error_result = None
        job.result.side_effect = TooManyRequests("synthetic terminal limit")
        self.client.load_table_from_dataframe.return_value = job

        with self.assertRaises(TooManyRequests):
            main.load_namespace_to_staging(self.client, self.data, STAGING_TABLE_ID)

        self.client.load_table_from_dataframe.assert_called_once()
        sleep.assert_not_called()

    @patch("main.time.sleep")
    def test_submission_429_propagates_without_retry(self, sleep):
        self.client.load_table_from_dataframe.side_effect = TooManyRequests(
            "synthetic submission limit"
        )

        with self.assertRaises(TooManyRequests):
            main.load_namespace_to_staging(self.client, self.data, STAGING_TABLE_ID)

        self.client.load_table_from_dataframe.assert_called_once()
        sleep.assert_not_called()

    @patch("main.time.sleep")
    def test_other_job_errors_propagate(self, sleep):
        job = Mock()
        job.result.side_effect = ValueError("synthetic job failure")
        self.client.load_table_from_dataframe.return_value = job

        with self.assertRaisesRegex(ValueError, "synthetic job failure"):
            main.load_namespace_to_staging(self.client, self.data, STAGING_TABLE_ID)

        self.client.load_table_from_dataframe.assert_called_once()
        sleep.assert_not_called()

    @patch("main.time.sleep")
    @patch("main.random.uniform", side_effect=lambda _low, high: high)
    def test_retry_budget_is_six_attempts_and_delay_is_capped(self, _uniform, sleep):
        jobs = [rate_limited_job() for _ in range(main.MAX_LOAD_RETRIES + 1)]
        self.client.load_table_from_dataframe.side_effect = jobs

        with self.assertRaises(TooManyRequests):
            main.load_namespace_to_staging(self.client, self.data, STAGING_TABLE_ID)

        self.assertEqual(
            self.client.load_table_from_dataframe.call_count,
            main.MAX_LOAD_RETRIES + 1,
        )
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list], [10, 20, 40, 60, 60]
        )
        self.assertLessEqual(max(call.args[0] for call in sleep.call_args_list), 60)

    @patch("main.time.sleep")
    @patch("main.random.uniform", return_value=0.25)
    def test_polling_429_exhausts_budget_without_resubmitting(self, _uniform, sleep):
        job = Mock()
        job.state = "PENDING"
        job.result.side_effect = [
            TooManyRequests("synthetic polling limit")
            for _ in range(main.MAX_LOAD_RETRIES + 1)
        ]
        self.client.load_table_from_dataframe.return_value = job

        with self.assertRaises(TooManyRequests):
            main.load_namespace_to_staging(self.client, self.data, STAGING_TABLE_ID)

        self.client.load_table_from_dataframe.assert_called_once()
        self.assertEqual(job.result.call_count, main.MAX_LOAD_RETRIES + 1)
        self.assertEqual(sleep.call_count, main.MAX_LOAD_RETRIES)

    @patch("main.time.sleep")
    @patch("main.random.uniform", return_value=0.25)
    def test_sdk_terminal_rate_limit_job_retries_with_a_new_job(self, _uniform, _sleep):
        client = Mock(project="synthetic-project", location="europe-north1")
        sdk_job = LoadJob.from_api_repr(
            {
                "jobReference": {
                    "projectId": "synthetic-project",
                    "jobId": "synthetic-job",
                    "location": "europe-north1",
                },
                "configuration": {
                    "load": {
                        "destinationTable": {
                            "projectId": "synthetic-project",
                            "datasetId": "synthetic_dataset",
                            "tableId": "synthetic_table",
                        }
                    }
                },
                "status": {
                    "state": "DONE",
                    "errorResult": {
                        "reason": "rateLimitExceeded",
                        "message": "synthetic rate limit",
                    },
                    "errors": [
                        {
                            "reason": "rateLimitExceeded",
                            "message": "synthetic rate limit",
                        }
                    ],
                },
            },
            client,
        )
        second_job = Mock()
        client.load_table_from_dataframe.side_effect = [sdk_job, second_job]

        with self.assertRaises(TooManyRequests):
            sdk_job.result()

        main.load_namespace_to_staging(client, self.data, STAGING_TABLE_ID)

        self.assertEqual(client.load_table_from_dataframe.call_count, 2)
        second_job.result.assert_called_once_with()


class WorkflowTests(unittest.TestCase):
    def secret(self, namespace):
        connection = base64.b64encode(b"postgres://user:pass@db:5432/airflow").decode()
        return SimpleNamespace(
            metadata=SimpleNamespace(namespace=namespace),
            data={"connection": connection},
        )

    def run_main(self, secrets, bq_client):
        k8s_client = Mock()
        k8s_client.list_secret_for_all_namespaces.return_value = SimpleNamespace(items=secrets)
        return (
            patch("main.config.load_incluster_config"),
            patch("main.client.CoreV1Api", return_value=k8s_client),
            patch("main.bigquery.Client", return_value=bq_client),
            patch("main.df_from_postgres", return_value=dataframe()),
        )

    def test_loads_complete_before_copy_and_delete(self):
        events = []
        bq_client = Mock()
        bq_client.get_table.side_effect = lambda _: events.append("get_table")
        truncate_job = Mock()
        truncate_job.result.side_effect = lambda: events.append("truncate_done")
        bq_client.query.side_effect = lambda *_args, **_kwargs: (
            events.append("truncate") or truncate_job
        )

        def load(*_args, **_kwargs):
            events.append("load")
            job = Mock()
            job.result.side_effect = lambda: events.append("load_done")
            return job

        bq_client.load_table_from_dataframe.side_effect = load
        copy_job = Mock()
        copy_job.result.side_effect = lambda: events.append("copy_done")
        bq_client.copy_table.side_effect = lambda *_args, **_kwargs: (
            events.append("copy") or copy_job
        )
        bq_client.delete_table.side_effect = lambda *_args, **_kwargs: events.append("delete")
        patches = self.run_main([self.secret("ns-one"), self.secret("ns-two")], bq_client)

        with patches[0], patches[1], patches[2], patches[3]:
            main.main()

        self.assertEqual(
            events,
            [
                "get_table",
                "truncate",
                "truncate_done",
                "load",
                "load_done",
                "load",
                "load_done",
                "copy",
                "copy_done",
                "delete",
            ],
        )

    def test_zero_namespaces_fails_without_copy(self):
        bq_client = Mock()
        patches = self.run_main([], bq_client)

        with patches[0], patches[1], patches[2], patches[3]:
            with self.assertRaisesRegex(RuntimeError, "No Airflow database namespaces"):
                main.main()

        bq_client.copy_table.assert_not_called()
        bq_client.delete_table.assert_not_called()

    def test_failed_load_prevents_copy_and_delete(self):
        bq_client = Mock()
        bq_client.get_table.side_effect = NotFound("synthetic missing table")
        failed_job = Mock()
        failed_job.result.side_effect = ValueError("synthetic load failure")
        bq_client.load_table_from_dataframe.return_value = failed_job
        patches = self.run_main([self.secret("ns-one")], bq_client)

        with patches[0], patches[1], patches[2], patches[3]:
            with self.assertRaisesRegex(ValueError, "synthetic load failure"):
                main.main()

        bq_client.copy_table.assert_not_called()
        bq_client.delete_table.assert_not_called()

    def test_staging_truncate_failure_stops_before_load_copy_and_delete(self):
        bq_client = Mock()
        truncate_job = Mock()
        truncate_job.result.side_effect = ValueError("synthetic truncate failure")
        bq_client.query.return_value = truncate_job
        patches = self.run_main([self.secret("ns-one")], bq_client)

        with patches[0], patches[1], patches[2], patches[3]:
            with self.assertRaisesRegex(ValueError, "synthetic truncate failure"):
                main.main()

        truncate_job.result.assert_called_once_with()
        bq_client.load_table_from_dataframe.assert_not_called()
        bq_client.copy_table.assert_not_called()
        bq_client.delete_table.assert_not_called()

    def test_second_namespace_load_failure_prevents_copy_and_delete(self):
        bq_client = Mock()
        bq_client.get_table.side_effect = NotFound("synthetic missing table")
        first_job = Mock()
        second_job = Mock()
        second_job.result.side_effect = ValueError("synthetic second load failure")
        bq_client.load_table_from_dataframe.side_effect = [first_job, second_job]
        patches = self.run_main(
            [self.secret("ns-one"), self.secret("ns-two")], bq_client
        )

        with patches[0], patches[1], patches[2], patches[3]:
            with self.assertRaisesRegex(ValueError, "synthetic second load failure"):
                main.main()

        self.assertEqual(bq_client.load_table_from_dataframe.call_count, 2)
        first_job.result.assert_called_once_with()
        second_job.result.assert_called_once_with()
        bq_client.copy_table.assert_not_called()
        bq_client.delete_table.assert_not_called()


if __name__ == "__main__":
    unittest.main()
