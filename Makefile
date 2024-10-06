run_fake_api_server:
	python ref_server/ref_server.py

run_app:
	python bus_server/app.py

rebuild_packages:
	pip-compile requirements_dev.in
	pip-compile requirements.in
	pip install -r requirements_dev.txt
	pip install -r requirements.txt

up:
	docker-compose up

down:
	docker-compose down

build:
	docker-compose build

test:
	pytest -vv tests