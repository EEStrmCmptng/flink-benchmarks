import requests
import logging
import json


class FlinkException(Exception):
    pass


class Flink:
    '''
    Flink REST API connector.

    TODO: Parse the response and return a dict.
    '''

    def __init__(self, endpoint="http://localhost:8081"):
        self._endpoint = endpoint

    def get_endpoint(self):
        return self._endpoint
    
    def set_endpoint(self, endpoint):
        self._endpoint = endpoint

    def get_cluster(self):
        '''
        Show cluster information.

        `/cluster`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#cluster

        '''        
        url = "{}/taskmanagers/10.0.0.235:33307-153094/logs".format(self._endpoint)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def list_jobs(self):
        '''
        List all jobs.

        `/jobs`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#jobs

        '''
        url = "{}/jobs".format(self._endpoint)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_job_agg_metrics(self):
        '''
        Get job aggregated metrics.

        `/jobs/metrics`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#job-metrics

        '''
        url = "{}/jobs/metrics".format(self._endpoint)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_job_details(self, jobid):
        '''
        Get job details by ID

        `/jobs/{jobid}`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#job-details

        '''
        url = "{}/jobs/{}".format(self._endpoint, jobid)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_job_metrics(self, jobid):
        '''
        Get job metrics by ID

        `/jobs/{jobid}/metrics`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#jobs-jobid-metrics

        '''
        url = "{}/jobs/{}/metrics".format(self._endpoint, jobid)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_job_plan(self, jobid):
        '''
        Get job plan by ID

        `/jobs/{jobid}/plan`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#jobs-jobid-plan

        '''
        url = "{}/jobs/{}/plan".format(self._endpoint, jobid)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_task_status(self, jobid, taskid):
        '''
        Get task status by ID

        `/jobs/:jobid/vertices/:vertexid`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#jobs-jobid-tasks-taskid

        '''
        url = "{}/jobs/{}/vertices/{}".format(self._endpoint, jobid, taskid)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_task_backpressure(self, jobid, taskid):
        '''
        Get task backpressure by ID

        `/jobs/:jobid/vertices/:vertexid/backpressure`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#jobs-jobid-tasks-taskid-backpressure

        '''
        url = "{}/jobs/{}/vertices/{}/backpressure".format(self._endpoint, jobid, taskid)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_task_metrics(self, jobid, taskid):
        '''
        Get task metrics by ID

        `/jobs/:jobid/vertices/:vertexid/metrics`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#jobs-jobid-tasks-taskid-metrics

        '''
        url = "{}/jobs/{}/vertices/{}/metrics".format(self._endpoint, jobid, taskid)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_task_metrics_details(self, jobid, taskid, fieldid):
        '''
        Get task metrics by ID

        `/jobs/:jobid/vertices/:vertexid/metrics`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#jobs-jobid-tasks-taskid-metrics

        '''
        url = "{}/jobs/{}/vertices/{}/metrics?get={}".format(self._endpoint, jobid, taskid, fieldid)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_subtask_metrics(self, jobid, taskid):
        '''
        Get subtask metrics by ID

        `/jobs/:jobid/vertices/:vertexid/subtasks/metrics`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#jobs-jobid-tasks-taskid-metrics-subtaskid

        '''
        url = "{}/jobs/{}/vertices/{}/subtasks/metrics".format(
            self._endpoint, jobid, taskid)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_subtask_metrics_details(self, jobid, taskid, subtaskid, fieldid):
        '''
        Get subtask metrics by ID

        `/jobs/:jobid/vertices/:vertexid/subtasks/metrics`

        https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/rest_api/#jobs-jobid-tasks-taskid-metrics-subtaskid

        '''
        url = "{}/jobs/{}/vertices/{}/subtasks/metrics?get={}".format(
            self._endpoint, jobid, taskid, subtaskid, fieldid)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()


def _test():
    from pprint import pprint
    flink = Flink(endpoint="http://128.31.24.216:8081/")
    print("Endpoint is:", flink.get_endpoint())
    jobs = flink.list_jobs()
    print("Jobs:", jobs)
    print("Job aggregated metrics:", flink.get_job_agg_metrics())
    for job in jobs['jobs']:
        job_id = job['id']
        print("Job ID:", job_id)
        print("Job details:")
        job_details = flink.get_job_details(job_id)
        pprint(job_details)
        print("Job metrics:")
        pprint(flink.get_job_metrics(job_id))
        print("Job plan:")
        pprint(flink.get_job_plan(job_id))
        for task in job_details['vertices']:
            task_id = task['id']
            print("Task ID:", task_id)
            print("Task status:")
            pprint(flink.get_task_status(job_id, task_id))
            print("Task backpressure:")
            pprint(flink.get_task_backpressure(job_id, task_id))
            print("Task metrics:")
            pprint(flink.get_task_metrics(job_id, task_id))
            print("Task Metrics Details:")
            pprint(flink.get_task_metrics_details(
                job_id, task_id, '0.numRecordsInPerSecond'))
            print("Subtask metrics:")
            pprint(flink.get_subtask_metrics(job_id, task_id))

if __name__ == "__main__":
    _test()
