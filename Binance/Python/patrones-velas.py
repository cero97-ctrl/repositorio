# trading-v6.py (versión mejorada con ATR, Bollinger, POC, Backtesting y carga completa de parámetros desde dotenv)

import pandas as pd
import logging
import sys
import os
import common_utils as utils

# === CONFIRMACIÓN ===
def check_confirmation(df, state, args):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    pattern = state.get("pattern")

    message = ""

    if pattern == "hammer" and latest['close'] > previous['high']:
        entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.rr_ratio, args.sl_buffer)
        message = utils.format_risk_management_message(f"✅ Confirmación alcista para patrón {pattern.upper()}", entry, sl, tp, rr)
        utils.record_trade(args.trades_log_file, args.symbol, f'CONFIRMED_LONG_{pattern.upper()}', entry, sl, tp, latest['ATR'], rr, notes=f'confirmation_for_{pattern}', timestamp=latest['timestamp'])
    elif pattern == "shooting_star" and latest['close'] < previous['low']:
        entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.rr_ratio, args.sl_buffer)
        message = utils.format_risk_management_message(f"✅ Confirmación bajista para patrón {pattern.upper()}", entry, sl, tp, rr)
        utils.record_trade(args.trades_log_file, args.symbol, f'CONFIRMED_SHORT_{pattern.upper()}', entry, sl, tp, latest['ATR'], rr, notes=f'confirmation_for_{pattern}', timestamp=latest['timestamp'])
    else:
        message = f"❌ Sin confirmación para el patrón {pattern.upper()}"

    utils.clear_state()
    return message

# === EVALUACIÓN DE SEÑALES ===
def evaluate_trade(df, args, log_file=None):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    signals = []
    pending_state = None

    atr_mean = utils.get_atr_mean_for_volatility(df) # Usamos 50 como en wyckoff para consistencia
    if latest['ATR'] < atr_mean:
        logging.info("ATR bajo: mercado sin volatilidad significativa, no se generan señales.")
        return "⚠️ Volatilidad baja (ATR bajo). No se recomienda operar ahora."

    poc_zone = utils.check_poc_zone(latest, args.poc)

    # --- ESTRATEGIA: CRUCE DE EMAs (Golden/Death Cross) ---
    # Golden Cross (Cruce Dorado) -> Señal de Compra
    if previous['EMA_50'] <= previous['EMA_200'] and latest['EMA_50'] > latest['EMA_200']:
        signal_text = "📈 **Golden Cross** detectado (EMA 50 cruza por encima de EMA 200)"
        entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.rr_ratio, args.sl_buffer)
        formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'LONG_GOLDENCROSS', entry, sl, tp, latest['ATR'], rr, 'golden_cross', timestamp=latest['timestamp']) # type: ignore

    # Death Cross (Cruce de la Muerte) -> Señal de Venta
    if previous['EMA_50'] >= previous['EMA_200'] and latest['EMA_50'] < latest['EMA_200']:
        signal_text = "📉 **Death Cross** detectado (EMA 50 cruza por debajo de EMA 200)"
        entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.rr_ratio, args.sl_buffer)
        formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'SHORT_DEATHCROSS', entry, sl, tp, latest['ATR'], rr, 'death_cross', timestamp=latest['timestamp']) # type: ignore

    # if utils.is_hammer(latest['open'], latest['close'], latest['high'], latest['low'], args.hammer_multiplier):
    #     signal_text = "🕯️ Hammer detected"
    #     if latest['close'] <= latest['Boll_Lower']:
    #         signal_text += " tocando banda inferior de Bollinger 📉"
    #     if poc_zone:
    #         signal_text += f" en ZONA DE SOPORTE POC (${args.poc:.8f}) 🔥"
    #     if latest['ATR'] > atr_mean:
    #         signal_text += " con alta volatilidad 🔥"
    #     if latest['volume'] > latest['volume_sma'] * args.volume_multiplier:
    #         signal_text += " con volumen climático 📈"
    #     entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.rr_ratio, args.sl_buffer)
    #     formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
    #     signals.append(formatted_signal)
    #     if entry:
    #         utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'LONG_HAMMER', entry, sl, tp, latest['ATR'], rr, 'hammer', timestamp=latest['timestamp']) # type: ignore
    #     pending_state = {"pattern": "hammer", "price": latest['close']}

    # if utils.is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'], args.shooting_star_multiplier):
    #     signal_text = "🕯️ Shooting Star detected"
    #     if latest['close'] >= latest['Boll_Upper']:
    #         signal_text += " tocando banda superior de Bollinger 📈"
    #     if poc_zone:
    #         signal_text += f" en ZONA DE RESISTENCIA POC (${args.poc:.8f}) ⚠️"
    #     if latest['ATR'] > atr_mean:
    #         signal_text += " con fuerte volatilidad ⚡"
    #     if latest['volume'] > latest['volume_sma'] * args.volume_multiplier:
    #         signal_text += " con volumen climático 📉"
    #     entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.rr_ratio, args.sl_buffer)
    #     formatted_signal = utils.format_risk_management_message(signal_text, entry, sl, tp, rr)
    #     signals.append(formatted_signal)
    #     if entry:
    #         utils.record_trade(args.trades_log_file if log_file is None else log_file, args.symbol, 'SHORT_SHOOTINGSTAR', entry, sl, tp, latest['ATR'], rr, 'shooting_star', timestamp=latest['timestamp']) # type: ignore
    #     pending_state = {"pattern": "shooting_star", "price": latest['close']}

    if pending_state:
        pending_state["signal_time"] = pd.to_datetime(latest['timestamp'], unit='ms').isoformat()
        utils.save_state(pending_state)
        signals.append("⏳ Esperando vela de confirmación en el próximo ciclo...")

    return "\n".join(signals) if signals else "⏳ Sin señales claras."

# === MODO BACKTESTING ===
def run_backtest(args):
    logging.info(f"Iniciando backtest con simulación de trades desde: {args.backtest_file}")
    try:
        df = pd.read_csv(args.backtest_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    except Exception as e:
        logging.error(f"Error al cargar CSV: {e}")
        return

    if not {'timestamp', 'open', 'high', 'low', 'close', 'volume'}.issubset(df.columns):
        logging.error("El CSV debe contener columnas: timestamp, open, high, low, close, volume")
        return

    # --- CORRECCIÓN: Respetar el límite de velas para el backtest ---
    if args.limit > 0 and len(df) > args.limit:
        logging.info(f"El archivo de datos tiene {len(df)} velas. Usando las últimas {args.limit} para el backtest.")
        df = df.tail(args.limit).reset_index(drop=True)

    df = utils.calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)
    # --- MEJORA: Usar un archivo de log específico para el backtest ---
    backtest_log_file = args.trades_log_file.replace('.csv', '_backtest.csv')
    
    # Limpiar el log de trades antes de empezar un nuevo backtest
    if os.path.exists(backtest_log_file):
        os.remove(backtest_log_file)
    utils.ensure_trades_log_exists(backtest_log_file)

    logging.info("Recorriendo velas para encontrar y simular señales...")
    for i in range(201, len(df)): # Empezamos más tarde para asegurar que todos los indicadores están maduros
        # Creamos un sub-dataframe que representa el "historial" hasta la vela actual
        sub_df = df.iloc[:i].copy()
        latest = sub_df.iloc[-1]

        # --- Lógica de Detección de Señal (simplificada para el bucle) ---
        # Nota: Aquí se usa 'latest' para el entry_candle y 'latest' para el pattern_candle
        # porque en el backtest estamos evaluando la vela actual como la que genera la señal.

        # Golden Cross
        previous = sub_df.iloc[-2]
        if previous['EMA_50'] <= previous['EMA_200'] and latest['EMA_50'] > latest['EMA_200'] and latest['RSI'] < 50:
            entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.rr_ratio, args.sl_buffer)
            if entry:
                utils.record_trade(backtest_log_file, args.symbol, 'LONG_GOLDENCROSS', entry, sl, tp, latest['ATR'], rr, 'golden_cross', timestamp=latest['timestamp']) # type: ignore

        # Death Cross
        if previous['EMA_50'] >= previous['EMA_200'] and latest['EMA_50'] < latest['EMA_200'] and latest['RSI'] > 50:
            entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.rr_ratio, args.sl_buffer)
            if entry:
                utils.record_trade(backtest_log_file, args.symbol, 'SHORT_DEATHCROSS', entry, sl, tp, latest['ATR'], rr, 'death_cross', timestamp=latest['timestamp']) # type: ignore

        # # Hammer
        # if utils.is_hammer(latest['open'], latest['close'], latest['high'], latest['low'], args.hammer_multiplier) and latest['RSI'] < 50:
        #     entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'long', args.risk_stop_mult, args.rr_ratio, args.sl_buffer)
        #     if entry:
        #         utils.record_trade(backtest_log_file, args.symbol, 'LONG_HAMMER', entry, sl, tp, latest['ATR'], rr, 'hammer', timestamp=latest['timestamp']) # type: ignore

        # # Shooting Star
        # if utils.is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'], args.shooting_star_multiplier) and latest['RSI'] > 50:
        #     entry, sl, tp, rr = utils.compute_risk_levels(latest['close'], latest['ATR'], 'short', args.risk_stop_mult, args.rr_ratio, args.sl_buffer)
        #     # if entry:
        #     #     utils.record_trade(backtest_log_file, args.symbol, 'SHORT_SHOOTINGSTAR', entry, sl, tp, latest['ATR'], rr, 'shooting_star', timestamp=latest['timestamp'])

    # --- CORRECCIÓN CRÍTICA: Ordenar el log de backtest por fecha ---
    # Esto es esencial para que la simulación posterior sea cronológicamente correcta.
    logging.info(f"Ordenando el archivo de log de backtest '{backtest_log_file}' por fecha...")
    try:
        log_df = pd.read_csv(backtest_log_file)
        log_df['timestamp'] = pd.to_datetime(log_df['timestamp'])
        log_df.sort_values(by='timestamp', inplace=True)
        log_df.to_csv(backtest_log_file, index=False)
    except Exception as e:
        logging.error(f"No se pudo ordenar el archivo de log de backtest: {e}")

    logging.info(f"\n✅ Backtest de detección de señales completado. Se encontraron trades en '{backtest_log_file}'.")

# === EJECUCIÓN ===
# === MAIN ===
if __name__ == "__main__":
    args, telegram_token, chat_id = utils.setup_logging_and_config()

    logging.info("==========================================")
    logging.info("Iniciando bot con la siguiente configuración:")
    logging.info(f"Símbolo: {args.symbol} | Intervalo: {args.interval}")
    logging.info(f"POC: {args.poc} | ATR Window: {args.atr_window} | Bollinger Window: {args.bollinger_window}")
    logging.info(f"Volumen SMA Period: {args.volume_sma_period}")
    logging.info(f"Risk Stop Mult: {args.risk_stop_mult} | RR Ratio: {args.rr_ratio} | SL Buffer: {args.sl_buffer}")
    logging.info(f"Trades Log File: {args.trades_log_file}")
    logging.info("==========================================")

    if args.backtest:
        run_backtest(args)
        sys.exit(0)

    # Limpiar estado anterior al iniciar en modo live para evitar confirmaciones incorrectas
    utils.clear_state()
    logging.info("Estado anterior limpiado. Iniciando en modo de operación en vivo.")

    startup_message = (
        f"🚀 *Bot de Trading Iniciado* 🚀\n\n"
        f"Monitoreando: `{args.symbol}` en intervalo `{args.interval}`\n"
        f"POC configurado en: `{args.poc}`\n\n"
        "El bot está en línea y funcionando correctamente\\."
    )

    utils.run_bot_main_loop(args, telegram_token, chat_id, evaluate_trade, check_confirmation, startup_message)
