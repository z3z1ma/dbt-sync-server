"""
Provides the main class for the dbt client.
"""

import base64
import subprocess
import os, signal

from re import L
from time import sleep, time
from typing import Callable, Dict, List, Union
from uuid import uuid1


import requests

DEFAULT_SYNC_SLEEP = 0.5


class RPCError(Exception):
    def __init__(self, response, *args, **kwargs):
        self.response: Dict = response
        super().__init__(*args, **kwargs)


class DbtClient:
    """
    DbtClient constructor.

    Args:
        - host (str): The host of the dbt server.
        - port (int): The port of the dbt server.
        - jsonrpc_version (str, optional): The jsonrpc version of the dbt server.
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 8580,
        jsonrpc_version: str = "2.0",
    ):
        """
        Initializes the dbt client.
        """
        self._host = host
        self._port = port
        self._jsonrpc_version = jsonrpc_version
        self.pid = None
        self.session = requests.Session()
        self._url = f"http://{self._host}:{self._port}/jsonrpc"

    def _request(self, method: str, *, params: dict = None) -> dict:
        """
        Makes a request to the dbt server.
        """
        if params is None:
            params = {}
        data = {
            "jsonrpc": self._jsonrpc_version,
            "method": method,
            "params": params,
            "id": str(uuid1()),
        }
        pid = self._get_pid()

        if method == "run_sql":
            print(f"Method: {method}, send HUP to {self.pid}")
            os.kill(self.pid, signal.SIGHUP)
            sleep(DEFAULT_SYNC_SLEEP)

        response = self.session.post(self._url, json=data)
        return response.json()

    def _get_pid(self) -> int:

        if self.pid:
            return self.pid

        proc1 = subprocess.Popen(["ps", "aux"], stdout=subprocess.PIPE)
        proc2 = subprocess.Popen(
            ["grep", "dbt-rpc"],
            stdin=proc1.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        proc3 = subprocess.Popen(
            ["grep", "-v", "grep"],
            stdin=proc2.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        proc4 = subprocess.Popen(
            ["awk", "{print $2}"],
            stdin=proc3.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        proc1.stdout.close()
        proc2.stdout.close()
        proc3.stdout.close()
        out, err = proc4.communicate()
        self.pid = int(out.strip().decode())

        return self.pid

    def status(self) -> dict:
        """
        Gets the status of the dbt server.

        Docs: https://docs.getdbt.com/reference/commands/rpc#status
        """
        return self._request("status")

    def poll(
        self,
        *,
        request_token: str,
        logs: bool = False,
        logs_start: int = 0,
    ) -> dict:
        """
        Polls the dbt server for the status of a request.

        Docs: https://docs.getdbt.com/reference/commands/rpc#poll
        """
        return self._request(
            "poll",
            params={
                "request_token": request_token,
                "logs": logs,
                "logs_start": logs_start,
            },
        )

    def ps(  # pylint: disable=invalid-name
        self,
        *,
        completed: bool = False,
    ):
        """
        Gets the status of the dbt server.

        Docs: https://docs.getdbt.com/reference/commands/rpc#ps
        """
        return self._request(
            "ps",
            params={
                "completed": completed,
            },
        )

    def kill(
        self,
        *,
        task_id: str,
    ):
        """
        Kills a running request.

        Docs: https://docs.getdbt.com/reference/commands/rpc#kill
        """
        return self._request(
            "kill",
            params={
                "task_id": task_id,
            },
        )

    def _run_sync(
        self,
        method: str,
        *,
        params: dict = None,
        logs: bool = False,
        logs_start: int = 0,
        timeout: int = None,
        timeout_action: Union[str, Callable] = "raise",
        task_tags: dict = None,
    ) -> dict:
        """
        Wrapper for running commands synchronously.
        """

        if params is None:
            params = {}

        if task_tags is None:
            task_tags = {}
        params["task_tags"] = task_tags

        if timeout is not None:
            max_time = time() + timeout
            params["timeout"] = timeout

        if (timeout_action not in ["raise", "return"]) and (
            not callable(timeout_action)
        ):
            raise ValueError("timeout_action must be 'raise', 'return' or a callable")

        response_data = self._request(method, params=params)
        if "result" in response_data:
            if "request_token" in response_data["result"]:
                request_token = response_data["result"]["request_token"]
            else:
                raise RPCError(
                    response_data,
                    f"Unknown response format for sync execution: {response_data}",
                )
        else:
            raise RPCError(
                response_data,
                f"Unknown response format for sync execution: {response_data}",
            )

        state = None
        while True:
            if (timeout is not None) and (time() >= max_time):
                if callable(timeout_action):
                    timeout_action()
                elif timeout_action == "raise":
                    raise Exception("Timed out waiting for response")
                elif timeout_action == "return":
                    return response_data
            response_data = self.poll(
                request_token=request_token,
                logs=logs,
                logs_start=logs_start,
            )
            if "result" in response_data:
                if "state" in response_data["result"]:
                    state = response_data["result"]["state"]
                else:
                    raise RPCError(
                        response_data,
                        f"Unknown response format for sync execution: {response_data}",
                    )
            else:
                raise RPCError(
                    response_data,
                    f"Unknown response format for sync execution: {response_data}",
                )
            if state == "success":
                return response_data
            sleep(DEFAULT_SYNC_SLEEP)

    def cli(
        self,
        command: str,
        *,
        logs: bool = False,
        logs_start: int = 0,
        sync: bool = False,
        task_tags: dict = None,
        timeout: int = None,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        Runs a dbt command using CLI syntax

        Docs: https://docs.getdbt.com/reference/commands/rpc#running-a-task-with-cli-syntax

        Args:
            - command (str): The command to run. No need to add `dbt` at the beginning.
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out. Only
                applies to sync mode.
        """
        command = command if not command.startswith("dbt") else command[3:].strip()
        params = {"cli": command}
        if timeout:
            params["timeout"] = timeout
        if task_tags:
            params["task_tags"] = task_tags

        if sync:
            return self._run_sync(
                "cli_args",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("cli_args", params=params)

    def compile(
        self,
        *,
        exclude: List[str] = None,
        logs: bool = False,
        logs_start: int = 0,
        select: List[str] = None,
        selector: str = None,
        state: str = None,
        sync: bool = False,
        task_tags: dict = None,
        threads: int = None,
        timeout: int = None,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        Compiles a project.

        Docs: https://docs.getdbt.com/reference/commands/rpc#compile-a-project-docs

        Args:
            - exclude (list, optional): A list of resources to exclude from compiling, running,
                testing, seeding, or snapshotting .
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - select (list, optional): A list of resources to execute.
            - selector (str, optional): The name of a predefined YAML selector that defines the set
                of resources to execute.
            - state (str, optional): The filepath of artifacts to use when establishing
                [state](https://docs.getdbt.com/docs/guides/understanding-state).
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods.
            - threads (int, optional): The number of threads to use.
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out. Only
                applies to sync mode.
        """

        exclude_str = " ".join(exclude) if exclude else None
        select_str = " ".join(select) if select else None
        task_tags = task_tags or {}

        params = {}
        if exclude_str:
            params["exclude"] = exclude_str
        if select_str:
            params["select"] = select_str
        if selector:
            params["selector"] = selector
        if state:
            params["state"] = state
        if task_tags:
            params["task_tags"] = task_tags
        if threads:
            params["threads"] = threads
        if timeout:
            params["timeout"] = timeout

        if sync:
            return self._run_sync(
                "compile",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("compile", params=params)

    def run(
        self,
        *,
        defer: bool = None,
        exclude: List[str] = None,
        logs: bool = False,
        logs_start: int = 0,
        select: List[str] = None,
        selector: str = None,
        state: str = None,
        sync: bool = False,
        task_tags: dict = None,
        threads: int = None,
        timeout: int = None,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        Runs models.

        Docs: https://docs.getdbt.com/reference/commands/rpc#run-models-docs

        Args:
            - defer (bool, optional): Whether to defer references to upstream, unselected resources.
                Requires `state`.
            - exclude (list, optional): A list of resources to exclude from compiling, running,
                testing, seeding, or snapshotting .
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - select (list, optional): A list of resources to execute.
            - selector (str, optional): The name of a predefined YAML selector that defines the set
                of resources to execute.
            - state (str, optional): The filepath of artifacts to use when establishing
                [state](https://docs.getdbt.com/docs/guides/understanding-state).
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods.
            - threads (int, optional): The number of threads to use.
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out. Only
                applies to sync mode.
        """

        if defer is not None and state is None:
            raise Exception("`defer` requires `state` to be set")

        exclude_str = " ".join(exclude) if exclude else None
        select_str = " ".join(select) if select else None
        task_tags = task_tags or {}

        params = {}
        if defer is not None:
            params["defer"] = defer
        if exclude_str:
            params["exclude"] = exclude_str
        if select_str:
            params["select"] = select_str
        if selector:
            params["selector"] = selector
        if state:
            params["state"] = state
        if task_tags:
            params["task_tags"] = task_tags
        if threads:
            params["threads"] = threads
        if timeout:
            params["timeout"] = timeout

        if sync:
            return self._run_sync(
                "run",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("run", params=params)

    # pylint: disable=too-many-arguments
    def test(
        self,
        *,
        data: bool = True,
        exclude: List[str] = None,
        logs: bool = False,
        logs_start: int = 0,
        schema: bool = True,
        select: List[str] = None,
        selector: str = None,
        state: str = None,
        sync: bool = False,
        task_tags: dict = None,
        threads: int = None,
        timeout: int = None,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        Run tests.

        Docs: https://docs.getdbt.com/reference/commands/rpc#run-tests-docs

        Args:
            - data (bool, optional): If True, run data tests.
            - exclude (list, optional): A list of resources to exclude from compiling, running,
                testing, seeding, or snapshotting .
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - schema (bool, optional): If True, run schema tests.
            - select (list, optional): A list of resources to execute.
            - selector (str, optional): The name of a predefined YAML selector that defines the set
                of resources to execute.
            - state (str, optional): The filepath of artifacts to use when establishing
                [state](https://docs.getdbt.com/docs/guides/understanding-state).
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods.
            - threads (int, optional): The number of threads to use.
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out. Only
                applies to sync mode.
        """

        exclude_str = " ".join(exclude) if exclude else None
        select_str = " ".join(select) if select else None
        task_tags = task_tags or {}

        params = {}
        if data is not None:
            params["data"] = data
        if exclude_str:
            params["exclude"] = exclude_str
        if schema is not None:
            params["schema"] = schema
        if select_str:
            params["select"] = select_str
        if selector:
            params["selector"] = selector
        if state:
            params["state"] = state
        if task_tags:
            params["task_tags"] = task_tags
        if threads:
            params["threads"] = threads
        if timeout:
            params["timeout"] = timeout

        if sync:
            return self._run_sync(
                "test",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("test", params=params)

    # pylint: disable=too-many-arguments
    def seed(
        self,
        *,
        exclude: List[str] = None,
        logs: bool = False,
        logs_start: int = 0,
        select: List[str] = None,
        selector: str = None,
        show: bool = None,
        state: str = None,
        sync: bool = False,
        task_tags: dict = None,
        threads: int = None,
        timeout: int = None,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        Run seeds.

        Docs: https://docs.getdbt.com/reference/commands/rpc#run-seeds-docs

        Args:
            - exclude (list, optional): A list of resources to exclude from compiling, running,
                testing, seeding, or snapshotting .
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - select (list, optional): A list of resources to execute.
            - selector (str, optional): The name of a predefined YAML selector that defines the set
                of resources to execute.
            - show (bool, optional): If True, show the seed commands.
            - state (str, optional): The filepath of artifacts to use when establishing
                [state](https://docs.getdbt.com/docs/guides/understanding-state).
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods.
            - threads (int, optional): The number of threads to use.
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out. Only
                applies to sync mode.
        """

        exclude_str = " ".join(exclude) if exclude else None
        select_str = " ".join(select) if select else None
        task_tags = task_tags or {}

        params = {}
        if exclude_str:
            params["exclude"] = exclude_str
        if select_str:
            params["select"] = select_str
        if selector:
            params["selector"] = selector
        if show is not None:
            params["show"] = show
        if state:
            params["state"] = state
        if task_tags:
            params["task_tags"] = task_tags
        if threads:
            params["threads"] = threads
        if timeout:
            params["timeout"] = timeout

        if sync:
            return self._run_sync(
                "seed",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("seed", params=params)

    def snapshot(
        self,
        *,
        exclude: List[str] = None,
        logs: bool = False,
        logs_start: int = 0,
        select: List[str] = None,
        selector: str = None,
        state: str = None,
        sync: bool = False,
        task_tags: dict = None,
        threads: int = None,
        timeout: int = None,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        Run snapshots.

        Docs: https://docs.getdbt.com/reference/commands/rpc#run-snapshots-docs

        Args:
            - exclude (list, optional): A list of resources to exclude from compiling, running,
                testing, seeding, or snapshotting .
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - select (list, optional): A list of resources to execute.
            - selector (str, optional): The name of a predefined YAML selector that defines the set
                of resources to execute.
            - state (str, optional): The filepath of artifacts to use when establishing
                [state](https://docs.getdbt.com/docs/guides/understanding-state).
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods.
            - threads (int, optional): The number of threads to use.
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out. Only
                applies to sync mode.
        """

        exclude_str = " ".join(exclude) if exclude else None
        select_str = " ".join(select) if select else None
        task_tags = task_tags or {}

        params = {}
        if exclude_str:
            params["exclude"] = exclude_str
        if select_str:
            params["select"] = select_str
        if selector:
            params["selector"] = selector
        if state:
            params["state"] = state
        if task_tags:
            params["task_tags"] = task_tags
        if threads:
            params["threads"] = threads
        if timeout:
            params["timeout"] = timeout

        if sync:
            return self._run_sync(
                "snapshot",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("snapshot", params=params)

    # pylint: disable=too-many-arguments
    def build(
        self,
        *,
        defer: str = None,
        exclude: List[str] = None,
        logs: bool = False,
        logs_start: int = 0,
        select: List[str] = None,
        selector: str = None,
        state: str = None,
        sync: bool = False,
        task_tags: dict = None,
        threads: int = None,
        timeout: int = None,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        Build.

        Docs: https://docs.getdbt.com/reference/commands/rpc#build-docs

        Args:
            - defer (str, optional): ???
            - exclude (list, optional): A list of resources to exclude from compiling, running,
                testing, seeding, or snapshotting .
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - select (list, optional): A list of resources to execute.
            - selector (str, optional): The name of a predefined YAML selector that defines the set
                of resources to execute.
            - state (str, optional): The filepath of artifacts to use when establishing
                [state](https://docs.getdbt.com/docs/guides/understanding-state).
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods.
            - threads (int, optional): The number of threads to use.
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out. Only
                applies to sync mode.
        """

        exclude_str = " ".join(exclude) if exclude else None
        select_str = " ".join(select) if select else None
        task_tags = task_tags or {}

        params = {}
        if defer:
            params["defer"] = defer
        if exclude_str:
            params["exclude"] = exclude_str
        if select_str:
            params["select"] = select_str
        if selector:
            params["selector"] = selector
        if state:
            params["state"] = state
        if task_tags:
            params["task_tags"] = task_tags
        if threads:
            params["threads"] = threads
        if timeout:
            params["timeout"] = timeout

        if sync:
            return self._run_sync(
                "build",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("build", params=params)

    # pylint: disable=too-many-arguments
    def list_resources(
        self,
        *,
        exclude: List[str] = None,
        logs: bool = False,
        logs_start: int = 0,
        output_keys: List[str] = None,
        resource_types: List[str] = None,
        select: List[str] = None,
        selector: str = None,
        sync: bool = False,
        task_tags: dict = None,
        threads: int = None,
        timeout: int = None,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        List project resources.

        Docs: https://docs.getdbt.com/reference/commands/rpc#list-project-resources-docs

        Args:
            - exclude (list, optional): A list of resources to exclude from compiling, running,
                testing, seeding, or snapshotting .
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - output_keys (list, optional): Specify which node properties to include in output.
            - resource_types (list, optional): Filter selected resources by type.
            - select (list, optional): A list of resources to execute.
            - selector (str, optional): The name of a predefined YAML selector that defines the set
                of resources to execute.
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods.
            - threads (int, optional): The number of threads to use.
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out. Only
                applies to sync mode.
        """

        exclude_str = " ".join(exclude) if exclude else None
        select_str = " ".join(select) if select else None
        task_tags = task_tags or {}

        params = {}
        if exclude_str:
            params["exclude"] = exclude_str
        if output_keys:
            params["output_keys"] = output_keys
        if resource_types:
            params["resource_types"] = resource_types
        if select_str:
            params["select"] = select_str
        if selector:
            params["selector"] = selector
        if task_tags:
            params["task_tags"] = task_tags
        if threads:
            params["threads"] = threads
        if timeout:
            params["timeout"] = timeout

        if sync:
            return self._run_sync(
                "build",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("build", params=params)

    # pylint: disable=too-many-arguments
    def generate_docs(
        self,
        *,
        compile_project: bool = None,
        logs: bool = False,
        logs_start: int = 0,
        state: str = None,
        sync: bool = False,
        task_tags: dict = None,
        timeout: int = None,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        Generate docs.

        Docs: https://docs.getdbt.com/reference/commands/rpc#generate-docs-docs

        Args:
            - compile_project (bool, optional): If True, compile the project before generating a
                catalog (optional, default=false).
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - state (str, optional): The filepath of artifacts to use when establishing
                [state](https://docs.getdbt.com/docs/guides/understanding-state).
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods.
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out. Only
                applies to sync mode.
        """

        task_tags = task_tags or {}

        params = {}
        if compile_project:
            params["compile"] = compile_project
        if state:
            params["state"] = state
        if task_tags:
            params["task_tags"] = task_tags

        if sync:
            return self._run_sync(
                "docs.generate",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("docs.generate", params=params)

    def compile_sql(
        self,
        name: str,
        sql: str,
        *,
        logs: bool = False,
        logs_start: int = 0,
        sync: bool = False,
        task_tags: dict = None,
        timeout: int = 60,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        Compiles a SQL statement.

        Docs: https://docs.getdbt.com/reference/commands/rpc#compiling-a-query

        Args:
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - name (str): The name of the query.
            - sql (str): The SQL statement to compile. No need to encode it in base64 before.
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods.
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out.
                Only applies to sync mode.
        """

        task_tags = task_tags or {}

        params = {}
        if task_tags:
            params["task_tags"] = task_tags
        if timeout:
            params["timeout"] = timeout
        params["name"] = name
        params["sql"] = base64.b64encode(sql.encode("utf-8")).decode("utf-8")

        if sync:
            return self._run_sync(
                "compile_sql",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("compile_sql", params=params)

    def run_sql(
        self,
        name: str,
        sql: str,
        *,
        logs: bool = False,
        logs_start: int = 0,
        sync: bool = False,
        task_tags: dict = None,
        timeout: int = 60,
        timeout_action: Union[str, Callable] = "raise",
    ):
        """
        Runs a SQL statement.

        Docs: https://docs.getdbt.com/reference/commands/rpc#executing-a-query

        Args:
            - logs (bool, optional): Whether to return logs. Only applies to sync mode.
            - logs_start (int, optional): The number of logs to skip. Only applies to sync mode.
            - name (str): The name of the query.
            - sql (str): The SQL statement to run. No need to encode it in base64 before.
            - sync (bool, optional): Whether to run the command synchronously.
            - task_tags (dict, optional): Arbitrary key/value pairs to attach to this task. These
                tags will be returned in the output of the poll and ps methods.
            - timeout (int, optional): The timeout in seconds.
            - timeout_action (str, optional): The action to take if the command times out. Only
                applies to sync mode.
        """

        task_tags = task_tags or {}

        params = {}
        if task_tags:
            params["task_tags"] = task_tags
        if timeout:
            params["timeout"] = timeout
        params["name"] = name
        params["sql"] = base64.b64encode(sql.encode("utf-8")).decode("utf-8")

        if sync:
            return self._run_sync(
                "run_sql",
                params=params,
                logs=logs,
                logs_start=logs_start,
                timeout=timeout,
                timeout_action=timeout_action,
                task_tags=task_tags,
            )
        return self._request("run_sql", params=params)
