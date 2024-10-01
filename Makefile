run_fake_api_server:
	python ref_server/ref_server.py

rebuild_packages:
	pip-compile requirements_dev.in
	pip install -r requirements_dev.txt