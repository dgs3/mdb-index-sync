DIST_DIR := dist
VENV_DIR := venv
VENV := . venv/bin/activate
SOURCE_DIRS := mdb_index_sync

.PHONY: venv clean lint

clean:
	rm -rf $(VENV_DIR)
	rm -rf $(DIST_DIR)

venv:
	python3.12 -m venv $(VENV_DIR)
	$(VENV); pip3 install --upgrade pip
	$(VENV); pip3 install -r requirements.txt

lint: venv
	$(VENV); mypy --strict  $(SOURCE_DIRS)
	$(VENV); pylint $(SOURCE_DIRS)
	$(VENV); black --check $(SOURCE_DIRS)

format: venv
	$(VENV); black $(SOURCE_DIRS)

build: venv
	$(VENV); python3 -m pip install --upgrade build
	$(VENV); python3 -m build

test-dist-upload: build
	$(VENV); python3 -m pip install --upgrade twine
	$(VENV); python3 -m twine upload --repository testpypi $(DIST_DIR)/MDB-Index-Sync*

prod-dist-upload: build
	$(VENV); python3 -m pip install --upgrade twine
	$(VENV); python3 -m twine upload $(DIST_DIR)/MDB-Index-Sync*
