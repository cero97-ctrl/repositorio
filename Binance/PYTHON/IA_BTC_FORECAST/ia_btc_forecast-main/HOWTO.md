HOWTO — Retomar rápidamente este proyecto
=========================================

Resumen ultra-rápido para volver a trabajar desde cero en esta máquina:

1) Abrir el directorio del proyecto

```bash
cd /home/cero/MEGA/VSCODE/Binance/Python/IA_BTC_FORECAST/ia_btc_forecast-main
```

2) Reactivar el entorno conda `IA` (o crearlo desde `environment.yml` si no existe)

```bash
conda activate IA || conda env create -f environment.yml && conda activate IA
```

3) Comprobar que los principales archivos están presentes

```bash
ls -la model_forecast.py infer.py run_all.sh Makefile README.md
```

4) Ejecutar pipeline completo (recolección + entrenamiento)

```bash
./run_all.sh          # crea run_all.log
# o
make run
```

5) Ejecutar inferencia con el modelo guardado

```bash
python infer.py
```

6) Ver logs rápidamente

```bash
tail -n 200 run_all.log
```

7) Para compartir o mover el entorno a otra máquina

```bash
conda env export --name IA --no-builds > environment.yml
pip freeze > requirements_freeze.txt   # opcional
```

8) Git: ver el estado y subir cambios

```bash
git status
git add <files>
git commit -m "mensaje"
git push origin main
```

Notas
- `model/` contiene el modelo guardado (`btc_forecast_model.keras`), `scaler.pkl` y `config.json`.
- Si al ejecutar ves "Illegal instruction" (exit code 132), reinstala TensorFlow/NumPy con conda-forge (ver README).

Fin — deja este HOWTO en el repo para retomarlo rápido.
