# simulate_trades.py
import pandas as pd
import argparse
import logging
import uuid # Para dar un ID único a cada trade

logging.basicConfig(level="INFO", format='%(asctime)s - %(levelname)s - %(message)s')

def run_simulation(trades_file, historical_data_file, initial_balance, risk_per_trade):
    """
    Simula el resultado de los trades registrados en un archivo CSV
    utilizando datos históricos de precios y gestionando un balance de cuenta.
    --- VERSIÓN REFACTORIZADA PARA MANEJAR MÚLTIPLES POSICIONES ABIERTAS ---
    """
    logging.info(f"Iniciando simulación con Balance Inicial=${initial_balance:,.2f} y Riesgo por Trade={risk_per_trade:.2%}")

    try:
        trades_df = pd.read_csv(trades_file)
        historical_df = pd.read_csv(historical_data_file)
    except FileNotFoundError as e:
        logging.error(f"Error: No se encontró el archivo - {e}")
        return
    
    # --- MEJORA: Manejar archivo de trades vacío ---
    if trades_df.empty:
        logging.warning(f"El archivo de señales '{trades_file}' está vacío. No se encontraron trades para simular.")
        # Creamos un archivo de resultados vacío pero con encabezado para evitar errores en el dashboard.
        pd.DataFrame(columns=trades_df.columns.tolist() + ['outcome', 'pnl_percentage', 'pnl_usd', 'exit_price', 'exit_time', 'balance_after_trade']).to_csv(trades_file.replace('.csv', '_results.csv'), index=False)
        return

    # Convertir timestamps a datetime para poder comparar
    trades_df['timestamp'] = pd.to_datetime(trades_df['timestamp'])
    historical_df['timestamp'] = pd.to_datetime(historical_df['timestamp'], unit='ms')

    # Crear un diccionario de trades para un acceso rápido por timestamp
    trades_by_timestamp = {}
    for _, trade in trades_df.iterrows():
        ts = trade['timestamp']
        if ts not in trades_by_timestamp:
            trades_by_timestamp[ts] = []
        trades_by_timestamp[ts].append(trade.to_dict())

    current_balance = initial_balance
    open_positions = []
    closed_trades = []

    # Bucle principal: iterar sobre cada vela del historial
    for candle_index, candle in historical_df.iterrows():
        # --- 1. REVISAR POSICIONES ABIERTAS PARA CERRARLAS ---
        positions_to_close_indices = []
        for i, pos in enumerate(open_positions):
            exit_price = None
            outcome = None

            # Lógica para trades LONG
            if 'LONG' in pos['type'].upper():
                if candle['low'] <= pos['stop_loss']:
                    outcome, exit_price = 'LOSS', pos['stop_loss']
                elif candle['high'] >= pos['take_profit']:
                    outcome, exit_price = 'WIN', pos['take_profit']
            # Lógica para trades SHORT
            elif 'SHORT' in pos['type'].upper():
                if candle['high'] >= pos['stop_loss']:
                    outcome, exit_price = 'LOSS', pos['stop_loss']
                elif candle['low'] <= pos['take_profit']:
                    outcome, exit_price = 'WIN', pos['take_profit']

            if outcome:
                # Calcular P&L
                pnl_usd = (exit_price - pos['entry']) * pos['position_size'] if 'LONG' in pos['type'].upper() else (pos['entry'] - exit_price) * pos['position_size']
                pnl_percentage = (pnl_usd / pos['balance_at_entry']) * 100
                
                # Actualizar balance
                current_balance += pnl_usd

                # Guardar resultado
                pos.update({
                    'outcome': outcome, 'pnl_percentage': pnl_percentage, 'pnl_usd': pnl_usd,
                    'exit_price': exit_price, 'exit_time': candle['timestamp'],
                    'balance_after_trade': current_balance
                })
                closed_trades.append(pos)
                positions_to_close_indices.append(i)
                logging.info(f"  -> CERRADO trade {pos['id']}: {outcome}, P&L: ${pnl_usd:,.2f}, Nuevo Balance: ${current_balance:,.2f}")

        # Eliminar posiciones cerradas de la lista de abiertas (iterando en reversa para evitar problemas de índice)
        for i in sorted(positions_to_close_indices, reverse=True):
            del open_positions[i]

        # --- 2. BUSCAR NUEVAS SEÑALES PARA ABRIR ---
        if candle['timestamp'] in trades_by_timestamp:
            for new_trade_signal in trades_by_timestamp[candle['timestamp']]:
                # --- Lógica de Simulación de Cuenta Realista ---
                capital_at_risk_in_open_trades = sum(pos.get('risk_amount_usd', 0) for pos in open_positions)
                available_balance = current_balance - capital_at_risk_in_open_trades
                
                risk_amount_usd = available_balance * risk_per_trade
                risk_per_share = abs(new_trade_signal['entry'] - new_trade_signal['stop_loss'])
                position_size = risk_amount_usd / risk_per_share if risk_per_share > 0 else 0

                new_position = new_trade_signal.copy()
                new_position.update({
                    'id': str(uuid.uuid4())[:8], # ID corto y único
                    'position_size': position_size,
                    'risk_amount_usd': risk_amount_usd,
                    'balance_at_entry': current_balance
                })
                open_positions.append(new_position)
                logging.info(f"-> ABIERTO nuevo trade {new_position['id']} ({new_position['type']}) en {candle['timestamp']}. Riesgo: ${risk_amount_usd:,.2f}")

    # Al final, cualquier posición que quede abierta se marca como 'IN_PROGRESS'
    for pos in open_positions:
        pos.update({'outcome': 'IN_PROGRESS', 'balance_after_trade': current_balance})
        closed_trades.append(pos)

    # Guardar los resultados en un nuevo archivo CSV
    results_df = pd.DataFrame(closed_trades)
    # Ordenar por fecha de cierre para que la curva de capital tenga sentido
    if 'exit_time' in results_df.columns:
        results_df.sort_values(by='exit_time', inplace=True)

    output_file = trades_file.replace('.csv', '_results.csv')
    results_df.to_csv(output_file, index=False)
    logging.info(f"✅ Simulación completada. Resultados guardados en '{output_file}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulador de resultados de trades.")
    parser.add_argument("--trades-file", type=str, required=True, help="Archivo CSV con los trades a simular.")
    parser.add_argument("--data-file", type=str, required=True, help="Archivo CSV con los datos históricos de precios (ej: btc_1h_data.csv).")
    parser.add_argument("--initial-balance", type=float, default=10000, help="Balance inicial de la cuenta para la simulación.")
    parser.add_argument("--risk-per-trade", type=float, default=0.01, help="Porcentaje del capital a arriesgar por operación (ej: 0.01 para 1%).")
    args = parser.parse_args()
    run_simulation(args.trades_file, args.data_file, args.initial_balance, args.risk_per_trade)