import os
import time

import psycopg


def wait(dsn):
    for _ in range(30):
        try:
            with psycopg.connect(dsn):
                return

        except Exception:
            time.sleep(1)

    raise RuntimeError(
        "database unavailable"
    )


def main():

    core_dsn = os.environ["CORE_DSN"]
    mart_dsn = os.environ["MART_DSN"]

    wait(core_dsn)
    wait(mart_dsn)

    ###########################################################################
    # JSON source -> generic SCD2 core
    ###########################################################################

    with psycopg.connect(core_dsn) as conn:

        conn.execute(
            "CALL core.sync_from_staging()"
        )

        #######################################################################
        # Operational users -> SCD2 core
        #######################################################################

        conn.execute(
            "CALL core.sync_users()"
        )

    ###########################################################################
    # Core current state -> mart
    ###########################################################################

    with psycopg.connect(mart_dsn) as conn:

        conn.execute(
            "CALL mart.refresh()"
        )

    print(
        "PIPELINE OK",
        flush=True,
    )


if __name__ == "__main__":
    main()
