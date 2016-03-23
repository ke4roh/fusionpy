__author__ = 'jscarbor'

from fusionpy.fusion import Fusion
import sys
import json

import errno
import os


def configure(args):
    """
    Configure collection(s) if not already there, fail if the collection(s) exist but differ from the given
    configuration.

    :param args: the name of a file with configuration information
    """
    with open(args[0]) as f:
        cfg = __ascii_keys(json.load(f))

    fusion = Fusion().ensure_config(write=False, **cfg)
    if fusion is None:
        # It's not there, so safe to write
        Fusion().ensure_config(**cfg)
    elif not fusion:
        # Cowardly not overwriting a differing configuration
        print "Fusion configuration differs from files.  Maybe clean to start over."
        sys.exit(5)

    print "Fusion collection matches file configuration."


def delete(args):
    """
    Delete a collection if it exists.  If the named collection does not exist, do nothing.

    :param args:  Optional, the name of a single collection to delete
    """
    if len(args) > 1:
        print "Too many arguments.  Name at most one collection."
        sys.exit(2)

    collection = None
    if len(args) == 1:
        collection = args[0]

    collection = Fusion().get_collection(collection)
    if collection.exists():
        collection.delete_collection(purge=True, solr=True)


def export(args):
    """
    Save out the current configuration from Fusion to file and folder(s) to permit re-import
    :param args: either a file name, preceeded by @, or a json string of elements to
    """
    with open(args[0]) as fh:
        things_to_save = json.load(fh)

    Fusion().export_config(things_to_save)


def dir(args):
    """
    Write to stdout a json file listing the collections and pipelines available for export.
    This file can then be pruned to only the things that should be collected.
    """
    f = Fusion()
    print json.dumps({
        "collections": f.get_collections(),
        "indexPipelines": [p["id"] for p in f.index_pipelines.get_pipelines()],
        "queryPipelines": [p["id"] for p in f.query_pipelines.get_pipelines()]
    }, indent=True, separators=(',', ':'), sort_keys=True)


def print_help(args):
    print "Usage"
    print "  python -m fusionpy.tool <verb> [argument] [...]"


def __ascii_keys(athing):
    # json will not always load these as regular strings.  Python2 (at least) requires strings, not unicode,
    # for the keys going in to the ** parameters
    if type(athing) is dict:
        cc = {}
        for k, v in athing.items():
            cc[k.encode('ascii', 'replace')] = __ascii_keys(v)
    elif type(athing) is list:
        cc = []
        for x in athing:
            cc.append(__ascii_keys(x))
    else:
        return athing
    return cc


if __name__ == "__main__":
    try:
        if sys.argv[1] == "help" or sys.argv[1] == "?":
            sys.argv[1] = "print_help"
        globals()[sys.argv[1]](sys.argv[2:])
    except IndexError as ie:
        print_help([])
    except Exception as e:
        print_help([])
        raise
