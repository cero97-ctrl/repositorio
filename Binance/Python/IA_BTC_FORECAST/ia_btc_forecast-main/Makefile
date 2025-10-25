.PHONY: all data train run clean logs

PY=python

all: run

data:
	$(PY) get_data_btc.py

train:
	$(PY) model_forecast.py

run: data train

clean:
	rm -f run_all.log || true
	rm -f model/*.keras || true

logs:
	tail -n 200 run_all.log || true
