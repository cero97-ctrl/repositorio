# dashboard.py
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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
interval = st.sidebar.selectbox('Intervalo (Interval)', ['1m', '5m', '15m', '30m', '1h', '4h', '1d'], index=4) # 1h por defecto
limit = st.sidebar.slider('Límite de Velas (Limit)', 100, 1000, args.limit)
poc = st.sidebar.number_input('Punto de Control (POC)', value=args.poc, format="%.2f")

st.sidebar.header('Actualización Automática')
auto_refresh = st.sidebar.checkbox('Activar auto-actualización', value=True)
refresh_interval = st.sidebar.number_input('Intervalo (segundos)', value=60, min_value=10, max_value=3600)

if auto_refresh:
    st.html(f'<meta http-equiv="refresh" content="{refresh_interval}">')
elif st.sidebar.button('Actualizar Datos Manualmente'):
    st.rerun()

# --- Carga y Procesamiento de Datos ---
@st.cache_data # Ya no se usa un TTL fijo, la actualización la controla el refresco de la página
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

    # Crear una figura con 2 subplots: uno para las velas y otro para el RSI
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.03, row_heights=[0.6, 0.2, 0.2])
    
    # Determinar el color de las barras de volumen
    volume_colors = ['green' if row['close'] >= row['open'] else 'red' for index, row in df.iloc[:-1].iterrows()] # Excluimos la última vela (en progreso)

    # --- Gráfico de Velas (subplot 1) ---
    fig.add_trace(go.Candlestick(x=df['timestamp'],
                    open=df['open'], high=df['high'],
                    low=df['low'], close=df['close'], name='Velas',
                    increasing_line_color='green', decreasing_line_color='red'), row=1, col=1)

    # Añadir indicadores al gráfico
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['EMA_50'], mode='lines', name='EMA 50', line=dict(color='orange', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['EMA_200'], mode='lines', name='EMA 200', line=dict(color='yellow', width=1.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['Boll_Upper'], mode='lines', name='Bollinger Sup.', line=dict(color='lightblue', width=1, dash='dash')), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['Boll_Lower'], mode='lines', name='Bollinger Inf.', line=dict(color='lightblue', width=1, dash='dash')), row=1, col=1)

    # Añadir línea de POC si está configurado
    if poc > 0:
        fig.add_hline(y=poc, line_width=2, line_dash="dot", line_color="red",
                      annotation_text=f"POC: {poc}", annotation_position="bottom right", row=1, col=1)

    # --- Gráfico de Volumen (subplot 2) ---
    fig.add_trace(go.Bar(x=df['timestamp'], y=df['volume'], name='Volumen', marker_color=volume_colors), row=2, col=1)
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['volume_sma'], mode='lines', name='Volumen SMA', line=dict(color='purple', width=1)), row=2, col=1)

    # --- Gráfico de RSI (subplot 3) ---
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['RSI'], mode='lines', name='RSI', line=dict(color='cyan')), row=3, col=1)
    fig.add_hline(y=70, line_dash="dot", line_color="red", line_width=1, row=3, col=1)
    fig.add_hline(y=30, line_dash="dot", line_color="green", line_width=1, row=3, col=1)

    # Actualizar layout general
    fig.update_layout(xaxis_rangeslider_visible=False, height=700, title=f'Análisis Técnico de {symbol}',
                      showlegend=True)
    fig.update_yaxes(title_text="Precio", row=1, col=1)
    fig.update_yaxes(title_text="Volumen", row=2, col=1)
    fig.update_yaxes(title_text="RSI", row=3, col=1)
    st.plotly_chart(fig, use_container_width=True)

    # --- Mostrar últimos datos e indicadores ---
    st.subheader('Últimos Datos e Indicadores')
    st.dataframe(df.tail(10).iloc[::-1], use_container_width=True) # Mostrar los 10 más recientes, invertidos para ver el último arriba

    # --- Historial de Trades ---
    st.subheader('Historial de Trades')
    trades_log_file = args.trades_log_file
    if os.path.exists(trades_log_file):
        trades_df = pd.read_csv(trades_log_file)
        st.dataframe(trades_df.tail(20).iloc[::-1], use_container_width=True) # Mostrar los últimos 20 trades
    else:
        st.info(f"El archivo de log de trades '{trades_log_file}' no existe todavía.")