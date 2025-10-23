# dashboard.py
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime, timedelta
import json
import common_utils as utils

# --- Funciones para Presets de Backtest ---
PRESETS_FILE = 'backtest_presets.json'

def load_presets():
    if not os.path.exists(PRESETS_FILE):
        return {}
    with open(PRESETS_FILE, 'r') as f:
        return json.load(f)

def save_presets(presets):
    with open(PRESETS_FILE, 'w') as f:
        json.dump(presets, f, indent=4)

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
poc = st.sidebar.number_input('Punto de Control (POC)', value=args.poc, format="%.2f")

# --- MEJORA: Selección de rango de fechas en lugar de límite de velas ---
st.sidebar.subheader('Rango de Fechas')
end_date = st.sidebar.date_input('Fecha Fin', value=datetime.now().date())
start_date = st.sidebar.date_input('Fecha Inicio', value=end_date - timedelta(days=7)) # Por defecto, 7 días antes

st.sidebar.header('Actualización Automática')
auto_refresh = st.sidebar.checkbox('Activar auto-actualización', value=True)
refresh_interval = st.sidebar.number_input('Intervalo (segundos)', value=60, min_value=10, max_value=3600)

if auto_refresh:
    st.html(f'<meta http-equiv="refresh" content="{refresh_interval}">')
elif st.sidebar.button('Actualizar Datos Manualmente'):
    st.rerun()

# --- Carga y Procesamiento de Datos ---
@st.cache_data(ttl=refresh_interval) # Cache con TTL para auto-actualización
def load_data(symbol, interval, start_date_obj, end_date_obj, _args):
    # --- LÓGICA MEJORADA PARA LÍNEAS DE INDICADORES COMPLETAS ---
    # 1. Determinar el periodo de "calentamiento" necesario. El indicador más largo es la EMA de 200.
    warmup_period = 200

    # Convertir fechas a timestamps en milisegundos
    # Para la fecha de inicio, tomamos el inicio del día.
    # Para la fecha de fin, tomamos el final del día (23:59:59.999).
    start_time_ms = int(datetime.combine(start_date_obj, datetime.min.time()).timestamp() * 1000)
    end_time_ms = int(datetime.combine(end_date_obj, datetime.max.time()).timestamp() * 1000)

    # Calcular el tiempo adicional necesario para el calentamiento de indicadores
    interval_ms = utils.interval_to_ms(interval)
    fetch_start_time_ms = start_time_ms - (warmup_period * interval_ms)

    # 2. Pedir datos desde Binance usando el rango de fechas extendido
    #    get_klines ahora puede manejar start_time y end_time
    df_full = utils.get_klines(symbol, interval, start_time_ms=fetch_start_time_ms, end_time_ms=end_time_ms)

    if not df_full.empty:
        # 3. Calcular los indicadores sobre el conjunto de datos completo (ej: 250 + 200 = 450 velas).
        df_full = utils.calculate_indicators(df_full, _args.volume_sma_period, _args.atr_window, _args.bollinger_window)
        df_full['timestamp'] = pd.to_datetime(df_full['timestamp'], unit='ms')

        # --- Añadir la vela en progreso (solo para el gráfico en vivo y si la fecha fin es hoy) ---
        if end_date_obj == datetime.now().date():
            current_price = utils.get_current_price_ticker(symbol)
            if current_price and not df_full.empty:
                new_candle_series = df_full.iloc[-1].copy()
                # --- CORRECCIÓN: Asegurar que el nuevo timestamp sea un objeto Datetime ---
                new_candle_series['timestamp'] = pd.to_datetime(new_candle_series['close_time'] + 1, unit='ms')
                new_candle_series['open'] = new_candle_series['close']
                new_candle_series['high'] = max(new_candle_series['high'], current_price)
                new_candle_series['low'] = min(new_candle_series['low'], current_price)
                new_candle_series['close'] = current_price
                new_candle_series['volume'] = 0
                new_candle = pd.DataFrame([new_candle_series])
                df_full = pd.concat([df_full, new_candle], ignore_index=True)
                # Recalcular indicadores para la última vela si es necesario (o simplemente dejarla sin algunos)
                # Para simplificar, asumimos que los indicadores de la última vela no son críticos para el display

        # 4. Recortar el DataFrame para mostrar solo el rango de fechas solicitado por el usuario.
        df_display = df_full[(df_full['timestamp'] >= pd.to_datetime(start_time_ms, unit='ms')) &
                              (df_full['timestamp'] <= pd.to_datetime(end_time_ms, unit='ms'))].reset_index(drop=True)
        return df_display
    return pd.DataFrame() # Devolver un DataFrame vacío si no hay datos

df = load_data(symbol, interval, start_date, end_date, args)

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
        # --- CORRECCIÓN: Generar colores para todas las velas ---
        volume_colors = ['green' if row['close'] >= row['open'] else 'red' for index, row in df.iterrows()]
    
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
        # --- CORRECCIÓN: Usar siempre el log principal para el gráfico en vivo ---
        # El gráfico en vivo solo debe mostrar las señales generadas, no los resultados de una simulación.
        log_file_to_use = args.trades_log_file

        if os.path.exists(log_file_to_use):
            trades_df = pd.read_csv(log_file_to_use)
            # --- CORRECCIÓN DE TIMESTAMP ---
            # Convertir el timestamp de los trades a datetime, asegurando compatibilidad
            # con el formato de las velas (que es UTC por defecto al convertir desde ms).
            # Esto alinea correctamente los marcadores en el gráfico.
            trades_df['timestamp'] = pd.to_datetime(trades_df['timestamp'], utc=True)

            # --- CORRECCIÓN CRÍTICA: Filtrar trades por el rango de fechas visible ---
            # Esto evita que trades antiguos compriman el gráfico.
            visible_start_date = pd.to_datetime(start_date).tz_localize('UTC')
            visible_end_date = pd.to_datetime(end_date + timedelta(days=1)).tz_localize('UTC')

            symbol_trades = trades_df[(trades_df['symbol'] == symbol) & 
                                      (trades_df['timestamp'] >= visible_start_date) & 
                                      (trades_df['timestamp'] < visible_end_date)].copy()
    
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
        fig.update_layout(
            xaxis_rangeslider_visible=False, 
            height=700, 
            title=f'Análisis Técnico de {symbol}',
            showlegend=True
        )
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
        # --- MEJORA: Cargar y aplicar presets ---
        presets = load_presets()
        preset_options = ["- Personalizado -"] + list(presets.keys())
        
        # Usar session_state para manejar la selección de preset
        if 'selected_preset' not in st.session_state:
            st.session_state.selected_preset = preset_options[0]

        def apply_preset():
            preset_name = st.session_state.preset_selector
            st.session_state.selected_preset = preset_name

        selected_preset_name = st.selectbox(
            "Cargar Preset de Filtros:", 
            options=preset_options, 
            key='preset_selector',
            on_change=apply_preset
        )

        # Determinar los valores por defecto para los filtros
        default_file_index = 0
        default_signal_types = []
        if st.session_state.selected_preset != preset_options[0]:
            preset_data = presets.get(st.session_state.selected_preset, {})
            if preset_data.get('file') in result_files:
                default_file_index = result_files.index(preset_data['file'])
            default_signal_types = preset_data.get('signal_types', [])

        selected_file = st.selectbox("Selecciona un archivo de resultados para analizar:", result_files, index=default_file_index)
        
        # --- MEJORA: Análisis de datos sin gráfico, enfocado en métricas ---
        if selected_file:
            try:
                results_df = pd.read_csv(selected_file)
                results_df['timestamp'] = pd.to_datetime(results_df['timestamp'])
            except Exception as e:
                st.error(f"No se pudo leer el archivo '{selected_file}'. Error: {e}")
                st.stop()
            
            signal_types = results_df['type'].unique()
            # Si hay tipos por defecto del preset, úsalos. Si no, selecciona todos.
            default_selection = default_signal_types if default_signal_types else list(signal_types)
            selected_types = st.multiselect("Filtrar por tipo de señal:", options=signal_types, default=default_selection)

            filtered_df = results_df[results_df['type'].isin(selected_types)]
            filtered_df = filtered_df[filtered_df['outcome'] != 'IN_PROGRESS'].copy() # Ignorar trades no cerrados

            if not filtered_df.empty:
                # --- 1. MÉTRICAS DE RENDIMIENTO GLOBAL ---
                st.subheader("Métricas de Rendimiento Global")
                total_trades = len(filtered_df)
                wins_df = filtered_df[filtered_df['outcome'] == 'WIN']
                losses_df = filtered_df[filtered_df['outcome'] == 'LOSS']
                win_rate = (len(wins_df) / total_trades) * 100 if total_trades > 0 else 0
                
                total_gain = wins_df['pnl_usd'].sum()
                total_loss = abs(losses_df['pnl_usd'].sum())
                profit_factor = total_gain / total_loss if total_loss > 0 else float('inf')

                avg_win = wins_df['pnl_usd'].mean() if not wins_df.empty else 0
                avg_loss = abs(losses_df['pnl_usd'].mean()) if not losses_df.empty else 0
                expectancy = (win_rate/100 * avg_win) - ((1 - win_rate/100) * avg_loss)

                # --- CORRECCIÓN CRÍTICA EN EL CÁLCULO DE P&L ---
                # Sumar el P&L de las operaciones filtradas es la forma correcta y robusta
                # de calcular la ganancia/pérdida neta para el subconjunto de datos.
                net_pnl_usd = filtered_df['pnl_usd'].sum()
                # El balance inicial para este cálculo porcentual debe ser el global.
                # --- MEJORA: Leer el balance inicial directamente del archivo si existe ---
                if 'initial_balance' in results_df.columns:
                    initial_balance_global = results_df['initial_balance'].iloc[0]
                else:
                    # Fallback a la lógica anterior si la columna no existe (para compatibilidad con resultados antiguos)
                    initial_balance_global = results_df.iloc[0]['balance_after_trade'] - results_df.iloc[0]['pnl_usd']
                
                net_pnl_perc = (net_pnl_usd / initial_balance_global) * 100 if initial_balance_global != 0 else 0

                # Cálculo de Max Drawdown
                filtered_df['cummax_balance'] = (initial_balance_global + filtered_df['pnl_usd'].cumsum()).cummax()
                filtered_df['drawdown'] = (filtered_df['cummax_balance'] - filtered_df['balance_after_trade']) / filtered_df['cummax_balance']
                max_drawdown = filtered_df['drawdown'].max() * 100

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Operaciones", f"{total_trades}")
                col1.metric("Victorias", f"{len(wins_df)}")
                col1.metric("Derrotas", f"{len(losses_df)}")
                
                col2.metric("Tasa de Acierto", f"{win_rate:.2f}%")
                col2.metric("Profit Factor", f"{profit_factor:.2f}")
                col2.metric("Expectativa / Trade", f"${expectancy:.2f}")

                col3.metric("Ganancia/Pérdida Neta", f"${net_pnl_usd:,.2f}", delta=f"{net_pnl_perc:.2f}%")
                col3.metric("Ganancia Media", f"${avg_win:,.2f}")
                col3.metric("Pérdida Media", f"${avg_loss:,.2f}")

                # El balance inicial y final no tienen sentido en una vista filtrada,
                # es mejor mostrar el balance global para dar contexto.
                col4.metric("Balance Inicial Global", f"${initial_balance_global:,.2f}")
                col4.metric("Balance Final Global", f"${results_df.iloc[-1]['balance_after_trade']:,.2f}")
                col4.metric("Máximo Drawdown", f"{max_drawdown:.2f}%", delta_color="inverse")

                # --- 2. ANÁLISIS POR TIPO DE SEÑAL ---
                st.subheader("Análisis por Tipo de Señal")
                analysis_by_type = filtered_df.groupby('type').agg(
                    total_trades=('type', 'count'),
                    pnl_usd=('pnl_usd', 'sum'),
                    wins=('outcome', lambda x: (x == 'WIN').sum())
                ).reset_index()
                analysis_by_type['win_rate'] = (analysis_by_type['wins'] / analysis_by_type['total_trades']) * 100
                analysis_by_type = analysis_by_type.sort_values(by='pnl_usd', ascending=False)
                st.dataframe(analysis_by_type, use_container_width=True,
                             column_config={"pnl_usd": st.column_config.NumberColumn(format="$%.2f"),
                                            "win_rate": st.column_config.NumberColumn(format="%.2f%%")})

                # --- 3. ANÁLISIS POR DIRECCIÓN (LONG/SHORT) ---
                st.subheader("Análisis por Dirección")
                filtered_df['direction'] = filtered_df['type'].apply(lambda x: 'LONG' if 'LONG' in x else 'SHORT')
                analysis_by_direction = filtered_df.groupby('direction').agg(
                    total_trades=('direction', 'count'),
                    pnl_usd=('pnl_usd', 'sum'),
                    wins=('outcome', lambda x: (x == 'WIN').sum())
                ).reset_index()
                analysis_by_direction['win_rate'] = (analysis_by_direction['wins'] / analysis_by_direction['total_trades']) * 100
                st.dataframe(analysis_by_direction, use_container_width=True,
                             column_config={"pnl_usd": st.column_config.NumberColumn(format="$%.2f"),
                                            "win_rate": st.column_config.NumberColumn(format="%.2f%%")})
                
                # --- 4. CURVA DE CAPITAL Y LOG DE TRADES ---
                st.subheader("Curva de Capital (Balance de Cuenta)")
                st.line_chart(filtered_df.set_index('timestamp')['balance_after_trade'])
                
                # --- 5. DISTRIBUCIÓN DE GANANCIAS Y PÉRDIDAS (HISTOGRAMA) ---
                st.subheader("Distribución de Ganancias y Pérdidas")
                if not filtered_df.empty:
                    fig_hist = go.Figure(data=[go.Histogram(x=filtered_df['pnl_usd'], nbinsx=50)])
                    fig_hist.update_layout(
                        title_text='Distribución de P&L por Trade (USD)',
                        xaxis_title_text='P&L (USD)',
                        yaxis_title_text='Frecuencia',
                        bargap=0.1
                    )
                    st.plotly_chart(fig_hist, use_container_width=True)

                st.subheader("Log de Trades Filtrados")
                st.dataframe(filtered_df, use_container_width=True)
            else:
                st.info("No hay trades que coincidan con los filtros seleccionados.")
            
            # --- MEJORA: Guardar presets ---
            with st.expander("Gestión de Presets"):
                st.write("Guarda la configuración actual de filtros (archivo y tipos de señal) para usarla más tarde.")
                new_preset_name = st.text_input("Nombre del nuevo preset:")
                if st.button("Guardar Preset"):
                    if new_preset_name:
                        presets[new_preset_name] = {
                            "file": selected_file,
                            "signal_types": selected_types
                        }
                        save_presets(presets)
                        st.success(f"¡Preset '{new_preset_name}' guardado! Refrescando...")
                        st.rerun()
                    else:
                        st.warning("Por favor, introduce un nombre para el preset.")
                
                st.divider()
                
                st.write("Elimina un preset guardado.")
                if not presets:
                    st.info("No hay presets guardados para eliminar.")
                else:
                    preset_to_delete = st.selectbox("Selecciona un preset para eliminar:", options=list(presets.keys()))
                    if st.button("Eliminar Preset Seleccionado", type="primary"):
                        if preset_to_delete in presets:
                            del presets[preset_to_delete]
                            save_presets(presets)
                            st.success(f"¡Preset '{preset_to_delete}' eliminado! Refrescando...")
                            st.rerun()
                        else:
                            st.error("El preset seleccionado ya no existe.")
with tab3:
    st.header("Historial Completo de Trades")
    trades_log_file = args.trades_log_file
    if os.path.exists(trades_log_file):
        trades_df = pd.read_csv(trades_log_file)
        st.dataframe(trades_df.tail(20).iloc[::-1], use_container_width=True) # Mostrar los últimos 20 trades
    else:
        st.info(f"El archivo de log de trades '{trades_log_file}' no existe todavía.")