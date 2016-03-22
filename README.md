This module provides access to the Lucidworks Fusion REST API
through Python, allowing convenient queries and idempotent
initializatoin of a configuratoin.

# TESTS
  make test

# USAGE
For every use case, set an environment variable to specify how to connect to your Fusion:

```bash
FUSION_API_COLLECTION_URL=http://admin:topsecret1@localhost:8764/api/apollo/collections/mythings
```

## To index
```python

from fusionpy.fusion import Fusion

collection = Fusion().get_collection()
docs=[{"foo":"bar"},{"foo":"baz"}]
collection.index(docs, pipeline='default')
collection.commit()
```

## To query
```python
from fusionpy.fusion import Fusion

collection = Fusion().get_collection()
resp = collection.query(pipeline='default', qparams={"q": "foo:bar"})
print resp.body
```

## Makefile for development cycle
```Makefile
.PHONY: all stats queries check clean print-fusion-config
all: stats

# This URL should work when things are set up correctly, and it must point to the collection.
export FUSION_API_COLLECTION_URL=http://admin:topsecret1@localhost:8764/api/apollo/collections/mythings

RUNRECORD_HEADER="date,recall,precision,f1,remarks"

clean:
        # Clear out most files, so that a fresh run will re-index & re-query
        python -m fusionpy.tool delete
        rm -f indexed configured ...  

configured: solutiondupes.json solr-config/*
        # Configure if it's empty, succeed if it's matching, fail if it's mismatching
        python -m fusionpy.tool configure solutiondupes.json
        touch $@

indexed: index_my_stuff stuff.json configured
        # Index all the duplicates
        ./index_my_stuff
        touch $@

queries runrecord.csv querystats.json: test_queries indexed
        # Query, measure recall & precision, save out some stats to querystats.json
        $(if $(wildcard runrecord.csv),,echo $(RUNRECORD_HEADER) >runrecord.csv)
        echo -n `date --utc +%Y-%m-%d\ %T,` >>runrecord.csv
        ./test_queries >>runrecord.csv

stats: calcstats querystats.json
        # Deep look at data
        ./calcstats

# These targets are for saving configuration 
print-fusion-config:
        # Write this file into fusion-config-to-save.json and it will feed into the export-fusion-config target
        python -m fusionpy.tool dir

mythings-config.json: fusion-config-to-save.json
        python -m fusionpy.tool export fusion-config-to-save.json >mythings-config.json

```

Flow for this Makefile:

1. Set up Fusion
2. The first time, `make print-fusion-config >fusion-config-to-save.json` and edit the output file to be just the things you want to save from Fusion.  
3. Save the Fusion configuration with `make mythings-config.json`
4. Run tests against your data set with `make stats`
5. Adjust Fusion config and repeat from step 3.
