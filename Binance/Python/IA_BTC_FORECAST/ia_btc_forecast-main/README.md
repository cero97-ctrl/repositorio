# IA_BTC_FORECAST

Repositorio con scripts para obtener datos y entrenar un modelo LSTM simple para predecir el precio de Bitcoin.

## Contenido relevante
- `get_data_btc.py` — obtiene datos de precios y los guarda en `data/prices.csv`.
- `model_forecast.py` — prepara datos, entrena un modelo LSTM (ejemplo) y grafica resultados.
- `requirements.txt` — dependencias pip mínimas.
- `environment.yml` — export del entorno conda `IA` (recomendado para reproducibilidad).

## Requisitos
- Conda (recomendado) o Python 3.11+.
- Entorno conda recomendado: `IA` (se proporciona `environment.yml`).

## Pasos rápidos (con Conda)

1. Activar tu entorno conda (o créalo desde `environment.yml` si no existe):

```bash
conda env create -f environment.yml   # crear (si aún no existe)
conda activate IA
```

2. Alternativa pip (si no usas conda):

```bash
python -m pip install -r requirements.txt
```

3. Ejecutar la recolección de datos (opcional):

```bash
python get_data_btc.py
```

4. Ejecutar el script de modelado (entrena y grafica):

```bash
python model_forecast.py
```

## Notas y troubleshooting
- Si obtienes un error tipo "Illegal instruction" al ejecutar el script (exit code 132), probablemente tu instalación de TensorFlow/NumPy contiene binarios optimizados para una CPU diferente. Solución rápida probada en este repo:

```bash
# respaldar estado
pip freeze > requirements_freeze_before.txt

# intentar reinstalar usando conda-forge (más compatible en CPUs variadas)
conda activate IA
conda install -y -c conda-forge tensorflow numpy
```

- Mensaje informativo de TensorFlow sobre optimizaciones de CPU es normal (no crítico). Si ves warnings de Keras sobre `input_shape`, actualiza la primera capa a `keras.Input(shape=...)` (ya aplicado en `model_forecast.py`).

## Guardado de modelo
- El repo puede contener un modelo en `model/`. Para guardar manualmente desde `model_forecast.py` puedes añadir al final del script:

```python
# ejemplo: model.save('model/btc_forecast_model.keras')
```

## Contribuciones
- Haz fork y pull request. Si quieres que añada scripts de evaluación/inferencia o un runner CLI, dímelo y lo preparo.

---

Generado el: 2025-10-25
