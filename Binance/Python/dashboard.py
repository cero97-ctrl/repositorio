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
    # --- LÓGICA MEJORADA PARA LÍNEAS DE INDICADORES COMPLETAS ---
    # 1. Determinar el periodo de "calentamiento" necesario. El indicador más largo es la EMA de 200.
    warmup_period = 200

    # 2. Pedir datos adicionales para que los indicadores se calculen correctamente desde el inicio.
    #    Pedimos las velas que el usuario quiere ver (limit) + las velas para el calentamiento.
    df = utils.get_klines(symbol, interval, limit + warmup_period)

    if not df.empty:
        # 3. Calcular los indicadores sobre el conjunto de datos completo (ej: 250 + 200 = 450 velas).
        df = utils.calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # 4. Recortar el DataFrame para mostrar solo las últimas 'limit' velas que el usuario solicitó.
        #    Ahora, estas velas ya tienen los valores de los indicadores calculados.
        df = df.tail(limit).reset_index(drop=True)
    return df

df = load_data(symbol, interval, limit)

# --- Pestañas Principales ---
tab1, tab2, tab3 = st.tabs(["📈 Gráfico en Vivo", "📊 Análisis de Backtest", "📜 Historial de Trades"])

with tab1:
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
    
        # --- AÑADIR MARCADORES DE TRADES DESDE EL LOG ---
        # Priorizamos el archivo con resultados si existe
        results_log_file = 'trades_log_results.csv'
        log_file_to_use = results_log_file if os.path.exists(results_log_file) else args.trades_log_file

        if os.path.exists(log_file_to_use):
            trades_df = pd.read_csv(log_file_to_use)
            trades_df['timestamp'] = pd.to_datetime(trades_df['timestamp'])
            # Filtrar trades para el símbolo actual
            symbol_trades = trades_df[trades_df['symbol'] == symbol].copy()
    
            # Separar trades de compra (long) y venta (short)
            buy_trades = symbol_trades[symbol_trades['type'].str.contains('LONG', case=False)].copy()
            sell_trades = symbol_trades[symbol_trades['type'].str.contains('SHORT', case=False)].copy()
    
            if not buy_trades.empty:
                # Crear texto personalizado para el hover
                buy_trades['hover_text'] = buy_trades.apply(
                    lambda row: f"<b>Compra ({row['type']})</b><br>Entrada: {row['entry']:.2f}<br>R:R: {row['rr']}",
                    axis=1
                )
                fig.add_trace(go.Scatter(
                    x=buy_trades['timestamp'], y=buy_trades['entry'],
                    mode='markers', name='Compras (Long)',
                    marker=dict(symbol='triangle-up', color='lime', size=10, line=dict(width=1, color='black')),
                    hoverinfo='text', text=buy_trades['hover_text']
                ), row=1, col=1)
    
            if not sell_trades.empty:
                sell_trades['hover_text'] = sell_trades.apply(
                    lambda row: f"<b>Venta ({row['type']})</b><br>Entrada: {row['entry']:.2f}<br>R:R: {row['rr']}",
                    axis=1
                )
                fig.add_trace(go.Scatter(
                    x=sell_trades['timestamp'], y=sell_trades['entry'],
                    mode='markers', name='Ventas (Short)',
                    marker=dict(symbol='triangle-down', color='red', size=10, line=dict(width=1, color='black')),
                    hoverinfo='text', text=sell_trades['hover_text']
                ), row=1, col=1)
    
    
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

with tab2:
    st.header("Análisis de Rendimiento del Backtest")

    # --- MEJORA: Selector de archivos de resultados ---
    # Buscar todos los archivos que terminen en '_results.csv' en el directorio actual
    result_files = [f for f in os.listdir('.') if f.endswith('_results.csv')]

    if not result_files:
        st.warning("No se encontraron archivos de resultados de backtest (ej: 'trades_log_results.csv').")
        st.info("Asegúrate de haber ejecutado el flujo de backtesting completo, incluyendo el script 'simulate_trades.py'.")
    else:
        selected_file = st.selectbox("Selecciona un archivo de resultados para analizar:", result_files)
        if selected_file:
            results_df = pd.read_csv(selected_file)
            
            # --- MEJORA: Filtro por tipo de señal ---
            # Extraer los tipos de señal únicos del log
            signal_types = results_df['type'].unique()
            selected_types = st.multiselect("Filtrar por tipo de señal:", options=signal_types, default=signal_types)

            # Filtrar el DataFrame basado en la selección
            filtered_df = results_df[results_df['type'].isin(selected_types)]
            filtered_df = filtered_df[filtered_df['outcome'] != 'IN_PROGRESS'].copy() # Ignorar trades no cerrados

            st.dataframe(filtered_df, use_container_width=True)

            if not filtered_df.empty:
                # Recalcular métricas para los datos filtrados
                total_trades = len(filtered_df)
                wins = filtered_df[filtered_df['outcome'] == 'WIN']                
                win_rate = (len(wins) / total_trades) * 100 if total_trades > 0 else 0
                
                # Profit Factor
                total_gain = filtered_df[filtered_df['pnl_usd'] > 0]['pnl_usd'].sum()
                total_loss = abs(filtered_df[filtered_df['pnl_usd'] < 0]['pnl_usd'].sum())
                profit_factor = total_gain / total_loss if total_loss > 0 else float('inf')

                col1, col2, col3 = st.columns(3)
                col1.metric("Total de Operaciones", f"{total_trades}")
                col2.metric("Tasa de Acierto (Win Rate)", f"{win_rate:.2f}%")
                col3.metric("Profit Factor", f"{profit_factor:.2f}")
                
                # --- MÉTRICAS DE CUENTA REAL ---
                if 'balance_after_trade' in filtered_df.columns:
                    initial_balance = filtered_df.iloc[0]['balance_after_trade'] - filtered_df.iloc[0]['pnl_usd']
                    final_balance = filtered_df.iloc[-1]['balance_after_trade']
                    net_pnl_usd = final_balance - initial_balance
                    
                    st.divider()
                    col1_b, col2_b, col3_b = st.columns(3)
                    col1_b.metric("Balance Inicial", f"${initial_balance:,.2f}")
                    col2_b.metric("Balance Final", f"${final_balance:,.2f}")
                    col3_b.metric("Ganancia/Pérdida Neta", f"${net_pnl_usd:,.2f}", delta=f"{(net_pnl_usd/initial_balance)*100:.2f}%")

                    # Gráfico de Curva de Capital en USD
                    st.subheader("Curva de Capital (Balance de Cuenta)")
                    st.line_chart(filtered_df.set_index('timestamp')['balance_after_trade'])
                else:
                    # Mantener la lógica anterior si el archivo de resultados es antiguo
                    total_pnl = filtered_df['pnl_percentage'].sum()
                    st.metric("Ganancia/Pérdida Neta (Porcentual)", f"{total_pnl:.2f}%")

                # Gráfico de P&L Acumulado
                filtered_df['cumulative_pnl'] = filtered_df['pnl_percentage'].cumsum()
                st.subheader("Curva de Capital (P&L Acumulado)")
                st.line_chart(filtered_df.set_index('timestamp')['cumulative_pnl'])
            else:
                st.info("No hay trades que coincidan con los filtros seleccionados.")


with tab3:
    st.header("Historial Completo de Trades")
    trades_log_file = args.trades_log_file
    if os.path.exists(trades_log_file):
        trades_df = pd.read_csv(trades_log_file)
        st.dataframe(trades_df.tail(20).iloc[::-1], use_container_width=True) # Mostrar los últimos 20 trades
    else:
        st.info(f"El archivo de log de trades '{trades_log_file}' no existe todavía.")