# dashboard.py
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os
import common_utils as utils

# --- Configuración de la página ---
st.set_page_config(layout="wide", page_title="Dashboard de Trading")

st.title('📈 Dashboard de Monitorización de Trading')
st.caption('Visualiza datos de mercado, indicadores y señales generadas por el bot.')

# --- Barra Lateral de Controles ---
st.sidebar.header('Configuración')

# Usamos los argumentos por defecto de tu script, pero permitimos que el usuario los cambie
args, _, _ = utils.load_config()

symbol = st.sidebar.text_input('Símbolo (Symbol)', args.symbol)
interval = st.sidebar.selectbox('Intervalo (Interval)', ['1m', '5m', '15m', '30m', '1h', '4h', '1d'], index=5) # 1h por defecto
limit = st.sidebar.slider('Límite de Velas (Limit)', 100, 1000, args.limit)
poc = st.sidebar.number_input('Punto de Control (POC)', value=args.poc, format="%.2f")

# Botón para actualizar los datos
if st.sidebar.button('Actualizar Datos'):
    st.experimental_rerun()

# --- Carga y Procesamiento de Datos ---
@st.cache_data(ttl=60) # Cachear los datos por 60 segundos
def load_data(symbol, interval, limit):
    df = utils.get_klines(symbol, interval, limit)
    if not df.empty:
        # Reutilizamos tus funciones de cálculo de indicadores
        df = utils.calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

df = load_data(symbol, interval, limit)

if df.empty:
    st.error(f"No se pudieron obtener datos para {symbol}. Revisa el símbolo o la conexión.")
else:
    # --- Visualización del Gráfico Principal ---
    st.subheader(f'Gráfico de Velas para {symbol} ({interval})')

    fig = go.Figure(data=[go.Candlestick(x=df['timestamp'],
                    open=df['open'], high=df['high'],
                    low=df['low'], close=df['close'], name='Velas')])

    # Añadir indicadores al gráfico
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['EMA_50'], mode='lines', name='EMA 50', line=dict(color='orange', width=1)))
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['EMA_200'], mode='lines', name='EMA 200', line=dict(color='purple', width=1)))
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['Boll_Upper'], mode='lines', name='Bollinger Superior', line=dict(color='lightblue', width=1, dash='dash')))
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['Boll_Lower'], mode='lines', name='Bollinger Inferior', line=dict(color='lightblue', width=1, dash='dash')))

    # Añadir línea de POC si está configurado
    if poc > 0:
        fig.add_hline(y=poc, line_width=2, line_dash="dot", line_color="red",
                      annotation_text=f"POC: {poc}", annotation_position="bottom right")

    fig.update_layout(xaxis_rangeslider_visible=False, height=500, title=f'Análisis Técnico de {symbol}')
    st.plotly_chart(fig, use_container_width=True)

    # --- Mostrar últimos datos e indicadores ---
    st.subheader('Últimos Datos e Indicadores')
    st.dataframe(df.tail(10).iloc[::-1]) # Mostrar los 10 más recientes, invertidos para ver el último arriba

    # --- Historial de Trades ---
    st.subheader('Historial de Trades')
    trades_log_file = args.trades_log_file
    if os.path.exists(trades_log_file):
        trades_df = pd.read_csv(trades_log_file)
        st.dataframe(trades_df.tail(20).iloc[::-1]) # Mostrar los últimos 20 trades
    else:
        st.info(f"El archivo de log de trades '{trades_log_file}' no existe todavía.")