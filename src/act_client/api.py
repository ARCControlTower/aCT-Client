"""Public functions for using act-client from Python without going through the CLI."""

from act_client.config import checkConf, expandPaths, loadConf
from act_client.operations import ACTRest


def getRestClient(conf_path=None, server=None, token=None):
    """Build an ACTRest client from the user's config, with optional overrides."""
    conf = loadConf(path=conf_path or '')
    if server:
        conf['server'] = server
    checkConf(conf, ['server', 'token'])
    expandPaths(conf)

    if token is None:
        with open(conf['token'], 'r') as f:
            token = f.read()

    return ACTRest(conf['server'], token=token)


def getJobStats(jobids=None, name='', state='', clienttab=None, arctab=None,
                 conf_path=None, server=None, token=None):
    """
    Return job status data as a list of dicts, the same data 'act stat' prints,
    without needing to parse its terminal output.

    jobids, name, state, clienttab, arctab - passed through to the aCT REST API,
        same meaning as the matching 'act stat' options; clienttab and arctab
        default to the same columns 'act stat' requests ('id,jobname' and
        'JobID,State,arcstate') since the server requires at least one column
        from each table
    conf_path, server, token - override the config file location, server URL or
        auth token used to connect; default to the user's normal config
    """
    actrest = getRestClient(conf_path=conf_path, server=server, token=token)
    try:
        return actrest.getJobStats(
            jobids=jobids or [],
            name=name,
            state=state,
            clienttab=clienttab or ['id', 'jobname'],
            arctab=arctab or ['JobID', 'State', 'arcstate'],
        )
    finally:
        actrest.close()
