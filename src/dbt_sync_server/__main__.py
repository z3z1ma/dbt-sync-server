import sys

import dbt_sync_server

if __name__ == "__main__":
    dbt_sync_server.serve(sys.argv[1:])
