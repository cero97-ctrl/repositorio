# simulate_trades.py
import pandas as pd
import argparse
import logging
import uuid # Para dar un ID único a cada trade

logging.basicConfig(level="INFO", format='%(asctime)s - %(levelname)s - %(message)s')

def run_simulation(trades_file, historical_data_file, initial_balance, risk_per_trade, max_open_trades, partial_tp_count, trailing_sl_breakeven, trailing_sl_atr_mult):
    """
    Simula el resultado de los trades registrados en un archivo CSV
    utilizando datos históricos de precios y gestionando un balance de cuenta.
    --- VERSIÓN REFACTORIZADA PARA MANEJAR MÚLTIPLES POSICIONES ABIERTAS ---
    """
    logging.info(f"Iniciando simulación con Balance Inicial=${initial_balance:,.2f}, Riesgo por Trade={risk_per_trade:.2%}")
    logging.info(f"Límite de trades: {max_open_trades}, TPs Parciales: {partial_tp_count}, SL a BE: {trailing_sl_breakeven}")
    logging.info(f"Trailing Stop Dinámico ATR: {'Activado (x' + str(trailing_sl_atr_mult) + ')' if trailing_sl_atr_mult > 0 else 'Desactivado'}")

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
        # --- MEJORA: Añadir initial_balance al encabezado ---
        header = trades_df.columns.tolist() + ['outcome', 'pnl_percentage', 'pnl_usd', 'exit_price', 'exit_time', 'balance_after_trade', 'initial_balance']
        pd.DataFrame(columns=header).to_csv(trades_file.replace('.csv', '_results.csv'), index=False)
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
    pending_orders = [] # NUEVO: Lista para órdenes límite que esperan ser ejecutadas
    closed_trades = []

    # Bucle principal: iterar sobre cada vela del historial
    for candle_index, candle in historical_df.iterrows():
        # --- 1. REVISAR POSICIONES ABIERTAS PARA CERRARLAS ---
        positions_to_close_indices = []
        for i, pos in enumerate(open_positions):
            exit_price = None
            outcome = None # 'WIN', 'LOSS'
            close_reason = None # 'SL', 'TP_PARTIAL', 'TP_FINAL'

            # --- NUEVA LÓGICA DE ACTIVACIÓN DE TRAILING STOP ---
            # Mover a Break-Even si el precio ha avanzado 1R (1 x Riesgo)
            if trailing_sl_breakeven and not pos.get('sl_moved_to_be', False):
                risk_distance = abs(pos['entry'] - pos['original_stop_loss'])
                if 'LONG' in pos['type'].upper() and candle['high'] >= pos['entry'] + risk_distance:
                    pos['stop_loss'] = pos['entry']
                    pos['sl_moved_to_be'] = True
                elif 'SHORT' in pos['type'].upper() and candle['low'] <= pos['entry'] - risk_distance:
                    pos['stop_loss'] = pos['entry']
                    pos['sl_moved_to_be'] = True

            # --- LÓGICA DE TRAILING STOP DINÁMICO (ATR) ---
            if trailing_sl_atr_mult > 0 and pos.get('sl_moved_to_be', False): # Solo se activa si ya está en Break-Even
                if 'LONG' in pos['type'].upper():
                    # Actualizar el precio más alto alcanzado
                    pos['highest_price_since_be'] = max(pos.get('highest_price_since_be', pos['entry']), candle['high'])
                    new_trailing_sl = pos['highest_price_since_be'] - (pos['atr'] * trailing_sl_atr_mult)
                    # Mover el SL solo si el nuevo es más alto que el actual
                    if new_trailing_sl > pos['stop_loss']:
                        pos['stop_loss'] = new_trailing_sl
                elif 'SHORT' in pos['type'].upper():
                    pos['lowest_price_since_be'] = min(pos.get('lowest_price_since_be', pos['entry']), candle['low'])
                    new_trailing_sl = pos['lowest_price_since_be'] + (pos['atr'] * trailing_sl_atr_mult)
                    if new_trailing_sl < pos['stop_loss']:
                        pos['stop_loss'] = new_trailing_sl

            # Lógica para trades LONG
            if 'LONG' in pos['type'].upper():
                if candle['low'] <= pos['stop_loss']:
                    outcome, exit_price, close_reason = 'LOSS', pos['stop_loss'], 'SL'
                # Si no hay TPs parciales, la única salida con ganancia es por el Trailing Stop
                elif partial_tp_count == 0 and candle['low'] <= pos['stop_loss']:
                     outcome, exit_price, close_reason = 'WIN' if pos['stop_loss'] > pos['entry'] else 'LOSS', pos['stop_loss'], 'TRAILING_SL'
                elif partial_tp_count > 0:
                    # --- LÓGICA DE TP PARCIAL ---
                    for tp_level in pos.get('partial_tps', []):
                        if candle['high'] >= tp_level:

            # Lógica para trades SHORT
            elif 'SHORT' in pos['type'].upper():
                if candle['high'] >= pos['stop_loss']:
                    outcome, exit_price, close_reason = 'LOSS', pos['stop_loss'], 'SL'
                elif partial_tp_count == 0 and candle['high'] >= pos['stop_loss']:
                    outcome, exit_price, close_reason = 'WIN' if pos['stop_loss'] < pos['entry'] else 'LOSS', pos['stop_loss'], 'TRAILING_SL'
                elif partial_tp_count > 0:
                    # --- LÓGICA DE TP PARCIAL ---
                    for tp_level in pos.get('partial_tps', []):
                        if candle['low'] <= tp_level:
                            outcome, exit_price, close_reason = 'WIN', tp_level, 'TP_PARTIAL'
                            break

            if outcome:
                # Si no usamos TPs parciales, siempre cerramos la posición completa.
                if partial_tp_count == 0:
                    size_to_close = pos['current_size']
                else: # Lógica de TPs parciales existente
                    size_to_close = 0
                    if close_reason == 'SL':
                        size_to_close = pos['current_size']
                    elif close_reason == 'TP_PARTIAL':
                        size_to_close = pos['partial_size']
                        pos['partial_tps'].remove(exit_price)


                # Asegurarse de no cerrar más de lo que queda
                size_to_close = min(size_to_close, pos['current_size'])

                # Calcular P&L para la porción cerrada
                pnl_usd = (exit_price - pos['entry']) * size_to_close if 'LONG' in pos['type'].upper() else (pos['entry'] - exit_price) * size_to_close
                pnl_percentage = (pnl_usd / (pos['balance_at_entry'] * (size_to_close / pos['original_size']))) * 100 if pos['balance_at_entry'] > 0 and pos['original_size'] > 0 else 0
                
                # Actualizar balance
                current_balance += pnl_usd
                # Actualizar el tamaño de la posición
                pos['current_size'] -= size_to_close

                # Crear un registro de trade cerrado para esta porción
                closed_trade_record = pos.copy()
                # Borramos claves que no son relevantes para el registro cerrado
                closed_trade_record.pop('partial_tps', None)
                closed_trade_record.pop('partial_size', None)
                closed_trade_record.pop('current_size', None)
                closed_trade_record.update({
                    'id': f"{pos['id']}-{len(pos.get('partial_tps', []))}", # ID único para la porción
                    'outcome': outcome, 'pnl_percentage': pnl_percentage, 'pnl_usd': pnl_usd,
                    'exit_price': exit_price, 'exit_time': candle['timestamp'],
                    'balance_after_trade': current_balance,
                    'position_size': size_to_close, # Guardamos el tamaño de la porción cerrada
                    'notes': f"{pos['notes']}_{close_reason}"
                })
                closed_trades.append(closed_trade_record)
                logging.info(f"  -> CERRADO PARCIALMENTE trade {closed_trade_record['id']}: {outcome} ({close_reason}), P&L: ${pnl_usd:,.2f}, Nuevo Balance: ${current_balance:,.2f}")

                # Si la posición se ha cerrado completamente, marcarla para eliminar
                if pos['current_size'] < 1e-9 or close_reason == 'SL': # Usar una pequeña tolerancia para flotantes
                    positions_to_close_indices.append(i)
                    logging.info(f"  -> POSICIÓN COMPLETA CERRADA para trade {pos['id']}.")

        # Eliminar posiciones cerradas de la lista de abiertas (iterando en reversa para evitar problemas de índice)
        for i in sorted(positions_to_close_indices, reverse=True):
            del open_positions[i]

        # --- 2. REVISAR ÓRDENES PENDIENTES PARA ABRIRLAS ---
        orders_to_open_indices = []
        for i, order in enumerate(pending_orders):
            entry_price_reached = False
            stop_loss_breached_before_entry = False

            if 'LONG' in order['type'].upper():
                # El precio de entrada se alcanza si el 'low' de la vela es menor o igual a la entrada
                if candle['low'] <= order['entry']:
                    entry_price_reached = True
                # La orden se invalida si el precio toca el SL antes que la entrada
                if candle['low'] <= order['stop_loss']:
                    stop_loss_breached_before_entry = True
            
            elif 'SHORT' in order['type'].upper():
                # El precio de entrada se alcanza si el 'high' de la vela es mayor o igual a la entrada
                if candle['high'] >= order['entry']:
                    entry_price_reached = True
                # La orden se invalida si el precio toca el SL antes que la entrada
                if candle['high'] >= order['stop_loss']:
                    stop_loss_breached_before_entry = True

            if stop_loss_breached_before_entry and not entry_price_reached:
                logging.warning(f"  -> ORDEN CANCELADA {order['type']} de {order['timestamp']}. SL tocado antes de la entrada.")
                orders_to_open_indices.append(i) # Marcar para eliminar
            elif entry_price_reached:
                # --- MEJORA: Simulación de Orden a Mercado ---
                # Si es un patrón de momentum, la entrada real es el 'open' de la vela actual,
                # no el precio de la orden (que era el 'close' de la vela anterior).
                # Esto simula una orden a mercado que se ejecuta al inicio de la nueva vela.
                is_momentum_pattern = '3SOLDIERS' in order['type'].upper() or '3CROWS' in order['type'].upper()
                
                # Para órdenes límite, la entrada es el precio de la orden.
                # Para órdenes de mercado, la entrada es el open de la vela que la ejecuta.
                actual_entry_price = candle['open'] if is_momentum_pattern else order['entry']

                # Recalcular SL/TP si la entrada de mercado es muy diferente a la esperada
                # (esto evita que un gap grande invalide el riesgo)
                if is_momentum_pattern:
                    # Mantenemos el riesgo original (distancia de la señal al SL)
                    original_risk_per_share = abs(order['entry'] - order['stop_loss'])
                    if 'LONG' in order['type'].upper():
                        order['stop_loss'] = actual_entry_price - original_risk_per_share
                        order['take_profit'] = actual_entry_price + (original_risk_per_share * order['rr'])
                    else: # SHORT
                        order['stop_loss'] = actual_entry_price + original_risk_per_share
                        order['take_profit'] = actual_entry_price - (original_risk_per_share * order['rr'])
                    order['entry'] = actual_entry_price # Actualizamos la orden con el precio real de entrada

                # --- LÓGICA PARA ABRIR LA POSICIÓN ---
                if len(open_positions) < max_open_trades:
                    capital_at_risk_in_open_trades = sum(pos.get('risk_amount_usd', 0) for pos in open_positions)
                    available_balance = current_balance - capital_at_risk_in_open_trades
                    risk_amount_usd = available_balance * risk_per_trade
                    risk_per_share = abs(order['entry'] - order['stop_loss'])
                    position_size = risk_amount_usd / risk_per_share if risk_per_share > 0 else 0
                    
                    # --- LÓGICA DE TP PARCIAL ---
                    partial_tps = []
                    partial_size = 0
                    if partial_tp_count > 0:
                        total_tp_distance = abs(order['take_profit'] - order['entry'])
                        step_distance = total_tp_distance / partial_tp_count
                        partial_size = position_size / partial_tp_count
                        
                        for j in range(1, partial_tp_count + 1):
                            if 'LONG' in order['type'].upper():
                                tp_level = order['entry'] + (j * step_distance)
                            else: # SHORT
                                tp_level = order['entry'] - (j * step_distance)
                            partial_tps.append(tp_level)

                    new_position = order.copy()
                    new_position.update({
                        'id': str(uuid.uuid4())[:8],
                        'original_size': position_size, # Guardamos el tamaño original
                        'original_stop_loss': order['stop_loss'], # Guardamos el SL original para el cálculo de 1R
                        'current_size': position_size,  # y el tamaño actual
                        'risk_amount_usd': risk_amount_usd,
                        'balance_at_entry': current_balance,
                        'partial_tps': partial_tps,
                        'partial_size': partial_size
                    })
                    open_positions.append(new_position)
                    logging.info(f"-> ABIERTO nuevo trade {new_position['id']} ({new_position['type']}) en {candle['timestamp']}. Riesgo: ${risk_amount_usd:,.2f}")
                else:
                    logging.warning(f"  -> OMITIDO trade {order['type']} en {candle['timestamp']}. Límite de {max_open_trades} posiciones abiertas alcanzado.")
                
                orders_to_open_indices.append(i) # Marcar para eliminar de pendientes, ya sea abierta o omitida

        # Eliminar órdenes procesadas de la lista de pendientes
        for i in sorted(orders_to_open_indices, reverse=True):
            del pending_orders[i]

        # --- 3. BUSCAR NUEVAS SEÑALES PARA AÑADIR A ÓRDENES PENDIENTES ---
        if candle['timestamp'] in trades_by_timestamp:
            for new_trade_signal in trades_by_timestamp[candle['timestamp']]:
                pending_orders.append(new_trade_signal)
                logging.info(f"  -> NUEVA ORDEN PENDIENTE ({new_trade_signal['type']}) registrada en {candle['timestamp']}. Esperando entrada en {new_trade_signal['entry']:.2f}")

    # Al final, cualquier posición que quede abierta se marca como 'IN_PROGRESS'
    for pos in open_positions:
        pos.update({'outcome': 'IN_PROGRESS', 'balance_after_trade': current_balance})
        closed_trades.append(pos)

    # Guardar los resultados en un nuevo archivo CSV
    results_df = pd.DataFrame(closed_trades)
    # Ordenar por fecha de cierre para que la curva de capital tenga sentido
    # --- MEJORA: Añadir la columna de balance inicial a todos los trades ---
    results_df['initial_balance'] = initial_balance

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
    parser.add_argument("--max-open-trades", type=int, default=999, help="Número máximo de posiciones abiertas simultáneamente.")
    parser.add_argument("--partial-tp-count", type=int, default=1, help="Número de Take Profits parciales a utilizar (1 = sin parciales).")
    parser.add_argument("--partial-tp-count", type=int, default=0, help="Número de Take Profits parciales a utilizar (0 = sin parciales, solo Trailing Stop).")
    parser.add_argument("--trailing-sl-breakeven", action='store_true', help="Activa el movimiento del Stop Loss a break-even después del primer TP parcial.")
    parser.add_argument("--trailing-sl-atr-mult", type=float, default=0.0, help="Multiplicador de ATR para el trailing stop dinámico (0 para desactivar).")
    args = parser.parse_args()
    run_simulation(args.trades_file, args.data_file, args.initial_balance, args.risk_per_trade, args.max_open_trades, args.partial_tp_count, args.trailing_sl_breakeven, args.trailing_sl_atr_mult)