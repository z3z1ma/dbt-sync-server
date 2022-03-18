"""
A super lightweight abstraction of the dbt rpc which serves out synchronous requests
and affords more customization. Should also afford us the ability to swap the RPC
with another solution like `dbt.lib` which dbt server may implicitly use.
"""
from typing import Dict
import multiprocessing
import subprocess
import time

from flask import Flask, request
from dbt_client import DbtClient
import click


app = Flask(__name__)


STATE: Dict[str, DbtClient] = {}

LOG_MSG = """
{action} QUERY
===============
{query}
"""


@app.route("/run", methods=["POST"])
def run_sql():
    # Server Logging
    print(LOG_MSG.format(action="RUNNING", query=request.data))
    try:
        # Lets consider memoization
        result = STATE["server"].run_sql("dbt-osmosis", f'SELECT * FROM ({request.data.decode("UTF-8")}) AS __rpc_query LIMIT 2000', sync=True)
    except Exception as err:
        return {"error": str(err)}
    else:
        return result["result"]["results"][0]["table"]


@app.route("/compile", methods=["POST"])
def compile_sql():
    # Server Logging
    print(LOG_MSG.format(action="COMPILING", query=request.data))
    try:
        # Lets consider memoization
        result = STATE["server"].compile_sql("dbt-osmosis", request.data.decode("UTF-8"), sync=True)
    except Exception as err:
        return {"error": str(err)}
    else:
        return {"result": result["result"]["results"][0]["compiled_sql"]}


def run_rpc(rpc_port: int = 8580, project_dir: str = "./"):
    print(f"Starting RPC on port {rpc_port}")
    try:
        subprocess.run(["dbt-rpc", "serve", "--port", str(rpc_port), "--project-dir", str(project_dir)])
    except Exception as err:
        print("RPC Terminated? Error: {}".format(str(err)))


@click.group()
def cli():
    pass


@cli.command()
@click.option("--port", type=click.INT, default=8581)
@click.option("--rpc-port", type=click.INT, default=8580)
@click.option("--project-dir", type=click.Path(exists=True, file_okay=False, dir_okay=True), default="./")
def serve(port: int = 8581, rpc_port: int = 8580, project_dir: str = "./"):
    STATE["server"] = DbtClient(port=rpc_port)
    rpc_server = multiprocessing.Process(target=run_rpc, args=(rpc_port, project_dir), daemon=True)
    rpc_server.start()
    time.sleep(2.5)
    if not rpc_server.is_alive():
        exit_code = rpc_server.exitcode
        rpc_server.close()
        if exit_code == 0:
            print("RPC failed to initialize, exit code {} most likely indicates a process is already running on port {} or the project directory provided [{}] is not a valid dbt project.".format(exit_code, rpc_port, project_dir))
        elif exit_code == 1:
            print("RPC failed to initialize, exit code {} most likely indicates the dbt project is invalid or has an error.".format(exit_code))
        else:
            print("RPC failed to initialize, exit code {} with unknown root cause.".format(exit_code))
        exit(1)
    try:
        app.run("localhost", port)
    finally:
        print("\nSHUTDOWN")
        if rpc_server.is_alive():
            print("CLEANING UP RPC")
            rpc_server.terminate()
            rpc_server.join()
            rpc_server.close()



if __name__ == "__main__":
    cli()
