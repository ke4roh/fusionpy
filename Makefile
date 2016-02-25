.PHONY: test clean develop
test: tests/*
	./setup.py nosetests
clean:
	rm -rf dist build fusionpy.egg-info .eggs
develop: test
	sudo ./setup.py develop
