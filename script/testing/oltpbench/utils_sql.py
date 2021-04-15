from ..util.db_server import NoisePageServer


def tpcc_drop_indexes_secondary(server: NoisePageServer):
    """
    Drop the secondary indexes for TPCC.

    Parameters
    ----------
    server : The server to drop the indexes for.
    """
    # Keep this in sync with OLTPBench.
    server.execute('DROP INDEX IDX_CUSTOMER_NAME', expect_result=False, quiet=False)
    server.execute('DROP INDEX IDX_ORDER', expect_result=False, quiet=False)
