# simulate_trades.py
import pandas as pd
import argparse
import logging

logging.basicConfig(level="INFO", format='%(asctime)s - %(levelname)s - %(message)s')

def run_simulation(trades_file, historical_data_file, initial_balance, risk_per_trade):
    """
    Simula el resultado de los trades registrados en un archivo CSV
    utilizando datos históricos de precios y gestionando un balance de cuenta.
    """
    logging.info(f"Iniciando simulación con Balance Inicial=${initial_balance:,.2f} y Riesgo por Trade={risk_per_trade:.2%}")

    try:
        trades_df = pd.read_csv(trades_file)
        historical_df = pd.read_csv(historical_data_file)
    except FileNotFoundError as e:
        logging.error(f"Error: No se encontró el archivo - {e}")
        return

    # Convertir timestamps a datetime para poder comparar
    trades_df['timestamp'] = pd.to_datetime(trades_df['timestamp'])
    historical_df['timestamp'] = pd.to_datetime(historical_df['timestamp'], unit='ms')

    current_balance = initial_balance
    results = []
    for index, trade in trades_df.iterrows():
        logging.info(f"Simulando trade #{index+1} ({trade['type']} en {trade['timestamp']})")

        # Filtrar los datos históricos posteriores al inicio del trade
        future_candles = historical_df[historical_df['timestamp'] > trade['timestamp']]

        outcome = 'IN_PROGRESS'
        pnl_percentage = 0
        exit_price = None
        exit_time = None
        pnl_usd = 0

        # --- Lógica de Simulación de Cuenta ---
        risk_amount_usd = current_balance * risk_per_trade
        risk_per_share = abs(trade['entry'] - trade['stop_loss'])
        position_size = risk_amount_usd / risk_per_share if risk_per_share > 0 else 0

        for _, candle in future_candles.iterrows():
            # Lógica para trades LONG
            if 'LONG' in trade['type'].upper():
                if candle['low'] <= trade['stop_loss']:
                    outcome = 'LOSS'
                    exit_price = trade['stop_loss']
                    exit_time = candle['timestamp']
                    pnl_usd = (exit_price - trade['entry']) * position_size
                    break
                elif candle['high'] >= trade['take_profit']:
                    outcome = 'WIN'
                    exit_price = trade['take_profit']
                    exit_time = candle['timestamp']
                    pnl_usd = (exit_price - trade['entry']) * position_size
                    break
            # Lógica para trades SHORT
            elif 'SHORT' in trade['type'].upper():
                if candle['high'] >= trade['stop_loss']:
                    outcome = 'LOSS'
                    exit_price = trade['stop_loss']
                    exit_time = candle['timestamp']
                    pnl_usd = (trade['entry'] - exit_price) * position_size
                    break
                elif candle['low'] <= trade['take_profit']:
                    outcome = 'WIN'
                    exit_price = trade['take_profit']
                    exit_time = candle['timestamp']
                    pnl_usd = (trade['entry'] - exit_price) * position_size
                    break
        
        # Actualizar P&L porcentual y balance
        pnl_percentage = (pnl_usd / current_balance) * 100 if current_balance > 0 else 0
        current_balance += pnl_usd

        trade['outcome'] = outcome
        trade['pnl_percentage'] = pnl_percentage
        trade['pnl_usd'] = pnl_usd
        trade['exit_price'] = exit_price
        trade['exit_time'] = exit_time
        trade['position_size'] = position_size
        trade['balance_after_trade'] = current_balance
        results.append(trade)

    # Guardar los resultados en un nuevo archivo CSV
    results_df = pd.DataFrame(results)
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