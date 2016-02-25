.PHONY: test clean
test: tests/*
	./setup.py nosetests
clean:
	rm -rf dist build fusionpy.egg-info .eggs

