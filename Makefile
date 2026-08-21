PY := venv311/bin/python

.PHONY: run preseason backfill publish predict model test

run:
	$(PY) pipeline.py run

preseason:
	$(PY) pipeline.py preseason

backfill:
	$(PY) pipeline.py backfill

publish:
	$(PY) pipeline.py publish

# Manual model usage: make predict GW=5
predict:
	$(PY) model.py $(GW)
