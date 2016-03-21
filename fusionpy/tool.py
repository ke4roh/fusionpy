__author__ = 'jscarbor'

from fusionpy.fusion import Fusion
import sys
import json

import errno
import os


def __mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def configure(args):
    """
    Configure collection(s) if not already there, fail if the collection(s) exist but differ from the given
    configuration.

    :param args: the name of a file with configuration information
    """
    with open(args[0]) as f:
        cfg = json.load(f)

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

    f = Fusion()

    system_config = {}
    if 'collections' in things_to_save:
        system_config["collections"] = {}
        for c in things_to_save['collections']:
            fc = f.get_collection(c)
            system_config["collections"][c] = fc.get_config()
            path = "fusion-config/" + c
            __mkdir_p(path)
            system_config["collections"]["files"] = path
            for cf in [x["name"] for x in fc.config_files.dir() if
                       x["name"] != "managed-schema" and
                               not x['isDir'] and
                               (x['version'] > 0
                                or x["name"] not in ["currency.xml",
                                                     "elevate.xml",
                                                     "params.json",
                                                     "protwords.txt",
                                                     "solrconfig.xml",
                                                     "stopwords.txt",
                                                     "synonyms.txt"])]:
                with open(path + "/" + cf, "w") as fh:
                    fh.write(fc.config_files.get_config_file(cf))
            schema = fc.schema()
            system_config["collections"]["fields"] = schema["fields"]
            system_config["collections"]["fieldTypes"] = schema["fieldTypes"]

    if 'indexPipelines' in things_to_save:
        system_config["indexPipelines"] = [p for p in f.index_pipelines.get_pipelines() if
                                           p["id"] in things_to_save['indexPipelines']]
    if 'queryPipelines' in things_to_save:
        system_config["queryPipelines"] = [p for p in f.query_pipelines.get_pipelines() if
                                           p["id"] in things_to_save['queryPipelines']]

    print json.dumps(system_config, indent=True, separators=(',', ':'), sort_keys=True)


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
