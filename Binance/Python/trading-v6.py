# trading-v6.py (versión mejorada con ATR, Bollinger, POC, Backtesting y carga completa de parámetros desde dotenv)

import pandas as pd
import time
import logging
import os
import sys
import common_utils as utils

# === CONFIRMACIÓN ===
def check_confirmation(df, state, args):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    pattern = state.get("pattern")

    message = ""

    if pattern in ["hammer"] and latest['close'] > previous['high']:
        message = add_risk_management(f"✅ Bullish confirmation for {pattern} pattern", 'long', latest, previous, args)
    elif pattern in ["shooting_star"] and latest['close'] < previous['low']:
        message = add_risk_management(f"✅ Bearish confirmation for {pattern} pattern", 'short', latest, previous, args)
    else:
        message = f"❌ No confirmation for {pattern} pattern"

    utils.clear_state()
    return message


# === GESTIÓN DE RIESGO ===
def add_risk_management(signal_text, direction, entry_candle, pattern_candle, args):
    """Añade los niveles de Stop Loss y Take Profit a una señal."""
    entry_price = entry_candle['close']
    
    if direction == 'long':
        stop_loss = pattern_candle['low'] * (1 - args.sl_buffer)
        risk = entry_price - stop_loss
        take_profit = entry_price + (risk * args.rr_ratio)
    else:  # short
        stop_loss = pattern_candle['high'] * (1 + args.sl_buffer)
        risk = stop_loss - entry_price
        if risk <= 0: # Evitar división por cero si la entrada es mala
            return signal_text, None, None, None, None
        take_profit = entry_price - (risk * args.rr_ratio)

    if risk <= 0:
        return f"{signal_text}\n\n⚠️ No se pudo calcular la gestión de riesgo (riesgo inválido)."

    return (f"{signal_text}\n\n"
            f"🎯 **Gestión de Riesgo (R:R 1:{args.rr_ratio})**\n"
            f"- Entrada: `${entry_price:.2f}`\n"
            f"- Stop Loss: `${stop_loss:.2f}`\n"
            f"- Take Profit: `${take_profit:.2f}`"), entry_price, stop_loss, take_profit, args.rr_ratio


# === EVALUACIÓN DE SEÑALES ===
def evaluate_trade(df, args):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    signals = []
    pending_state = None

    atr_mean = df['ATR'].rolling(50).mean().iloc[-1] # Usamos 50 como en wyckoff para consistencia
    if latest['ATR'] < atr_mean:
        logging.info("ATR bajo: mercado sin volatilidad significativa, no se generan señales.")
        return "⚠️ Volatilidad baja (ATR bajo). No se recomienda operar ahora."

    poc_zone = utils.check_poc_zone(latest, args.poc)

    # --- ESTRATEGIA: CRUCE DE EMAs (Golden/Death Cross) ---
    # Golden Cross (Cruce Dorado) -> Señal de Compra
    if previous['EMA_50'] <= previous['EMA_200'] and latest['EMA_50'] > latest['EMA_200']:
        signal_text = "📈 **Golden Cross** detectado (EMA 50 cruza por encima de EMA 200)"
        formatted_signal, entry, sl, tp, rr = add_risk_management(signal_text, 'long', latest, latest, args)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file, args.symbol, 'LONG_GOLDENCROSS', entry, sl, tp, latest['ATR'], rr, 'golden_cross')

    # Death Cross (Cruce de la Muerte) -> Señal de Venta
    if previous['EMA_50'] >= previous['EMA_200'] and latest['EMA_50'] < latest['EMA_200']:
        signal_text = "📉 **Death Cross** detectado (EMA 50 cruza por debajo de EMA 200)"
        formatted_signal, entry, sl, tp, rr = add_risk_management(signal_text, 'short', latest, latest, args)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file, args.symbol, 'SHORT_DEATHCROSS', entry, sl, tp, latest['ATR'], rr, 'death_cross')

    if utils.is_hammer(latest['open'], latest['close'], latest['high'], latest['low'], args.hammer_multiplier):
        signal_text = "🕯️ Hammer detected"
        if latest['close'] <= latest['Boll_Lower']:
            signal_text += " tocando banda inferior de Bollinger 📉"
        if poc_zone:
            signal_text += f" en ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥"
        if latest['ATR'] > atr_mean:
            signal_text += " con alta volatilidad 🔥"
        if latest['volume'] > latest['volume_sma'] * args.volume_multiplier:
            signal_text += " con volumen climático 📈"
        formatted_signal, entry, sl, tp, rr = add_risk_management(signal_text, 'long', latest, latest, args)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file, args.symbol, 'LONG_HAMMER', entry, sl, tp, latest['ATR'], rr, 'hammer')
        pending_state = {"pattern": "hammer", "price": latest['close']}

    if utils.is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'], args.shooting_star_multiplier):
        signal_text = "🕯️ Shooting Star detected"
        if latest['close'] >= latest['Boll_Upper']:
            signal_text += " tocando banda superior de Bollinger 📈"
        if poc_zone:
            signal_text += f" en ZONA DE RESISTENCIA POC (${args.poc:.2f}) ⚠️"
        if latest['ATR'] > atr_mean:
            signal_text += " con fuerte volatilidad ⚡"
        if latest['volume'] > latest['volume_sma'] * args.volume_multiplier:
            signal_text += " con volumen climático 📉"
        formatted_signal, entry, sl, tp, rr = add_risk_management(signal_text, 'short', latest, latest, args)
        signals.append(formatted_signal)
        if entry:
            utils.record_trade(args.trades_log_file, args.symbol, 'SHORT_SHOOTINGSTAR', entry, sl, tp, latest['ATR'], rr, 'shooting_star')
        pending_state = {"pattern": "shooting_star", "price": latest['close']}

    if pending_state:
        pending_state["signal_time"] = pd.to_datetime(latest['timestamp'], unit='ms').isoformat()
        utils.save_state(pending_state)
        signals.append("⏳ Esperando vela de confirmación en el próximo ciclo...")

    return "\n".join(signals) if signals else "⏳ Sin señales claras."

# === MODO BACKTESTING ===
def run_backtest(args):
    logging.info(f"Iniciando backtest con datos: {args.backtest_file}")
    try:
        df = pd.read_csv(args.backtest_file)
    except Exception as e:
        logging.error(f"Error al cargar CSV: {e}")
        return

    if not {'open','high','low','close','volume'}.issubset(df.columns):
        logging.error("El CSV debe contener columnas: open, high, low, close, volume")
        return

    df = utils.calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)
    total_signals = 0

    for i in range(51, len(df)):
        sub_df = df.iloc[:i+1]
        signal = evaluate_trade(sub_df, args)
        if "⏳" not in signal and "Volatilidad baja" not in signal:
            total_signals += 1
            print(f"[{i}] {signal} @ {sub_df.iloc[-1]['close']}")

    print(f"\n✅ Backtest completado. Señales detectadas: {total_signals}")

# === EJECUCIÓN ===
# === EJECUCIÓN ===
def execute_single_run(args, telegram_token, chat_id):
    logging.info(f"Analizando {args.symbol} {args.interval}...")
    df = utils.get_klines(args.symbol, args.interval, args.limit)
    if df.empty or len(df) < 2:
        logging.warning("Datos insuficientes.")
        return

    df = utils.calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)

    pending_state = utils.load_state()
    if pending_state:
        signal = check_confirmation(df, pending_state, args)
    else:
        signal = evaluate_trade(df, args)

    message = f"--- Análisis para {args.symbol} ({args.interval}) ---\n\n{signal}"
    logging.info(message)

    if "⏳" not in signal and "Volatilidad baja" not in signal and "❌ Sin confirmación" not in signal:
        utils.send_telegram_message(message, telegram_token, chat_id, pre_escaped=True)

# === MAIN ===
if __name__ == "__main__":
    args, telegram_token, chat_id = utils.load_config()
    
    # Configurar logging después de cargar la configuración para usar el nivel de log correcto
    logging.basicConfig(
        level=args.log.upper(), 
        format='%(asctime)s - %(levelname)s - %(message)s', 
        stream=sys.stdout
    )

    logging.info("==========================================")
    logging.info("Iniciando bot con la siguiente configuración:")
    logging.info(f"Símbolo: {args.symbol}")
    logging.info(f"Intervalo: {args.interval}")
    logging.info(f"POC: {args.poc}")
    logging.info(f"ATR Window: {args.atr_window}")
    logging.info(f"Bollinger Window: {args.bollinger_window}")
    logging.info(f"Volumen SMA Period: {args.volume_sma_period}")
    logging.info(f"Risk/Reward Ratio (para v6): {args.rr_ratio}")
    logging.info(f"Stop Loss Buffer (para v6): {args.sl_buffer}")
    logging.info("==========================================")

    if args.backtest:
        run_backtest(args)
        sys.exit(0)

    # Limpiar estado anterior al iniciar en modo live para evitar confirmaciones incorrectas
    utils.clear_state()
    logging.info("Estado anterior limpiado. Iniciando en modo de operación en vivo.")

    # Enviar mensaje de inicio a Telegram
    startup_message = (
        f"🚀 *Bot de Trading Iniciado* 🚀\n\n"
        f"Monitoreando: `{args.symbol}` en intervalo `{args.interval}`\n"
        f"POC configurado en: `{args.poc}`\n\n"
        "El bot está en línea y funcionando correctamente\\."
    )
    utils.send_telegram_message(startup_message, telegram_token, chat_id, pre_escaped=True)
    logging.info("Mensaje de inicio enviado a Telegram.")

    while True:
        try:
            execute_single_run(args, telegram_token, chat_id)
            logging.info(f"Análisis completado. Esperando {args.sleep} segundos para el próximo ciclo.")
            time.sleep(args.sleep)
        except KeyboardInterrupt:
            logging.info("Bot detenido manualmente. Limpiando estado...")
            utils.clear_state()
            sys.exit(0)
        except Exception as e:
            logging.error(f"Error inesperado en el ciclo principal: {e}")
            time.sleep(60) # Esperar un minuto antes de reintentar en caso de error grave
