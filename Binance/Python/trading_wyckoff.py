# trading-v6.py (versión con GESTIÓN DE RIESGO Opción A + Módulo Wyckoff simplificado)

import pandas as pd
import time
import logging
import os
import sys
from datetime import datetime
import common_utils as utils

# === CONFIRMACIÓN ===
def check_confirmation(df, state, args):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    pattern = state.get("pattern")

    message = ""
    trade_type = None
    
    if pattern == "hammer" and latest['close'] > previous['high']:
        trade_type = 'long'
        signal_text = f"✅ Confirmación alcista para patrón {pattern}"
    elif pattern == "shooting_star" and latest['close'] < previous['low']:
        trade_type = 'short'
        signal_text = f"✅ Confirmación bajista para patrón {pattern}"
    else:
        message = f"❌ Sin confirmación para el patrón {pattern}"

    utils.clear_state()
    if trade_type:
        entry, stop, tp, rr = compute_risk_levels(latest['close'], latest['ATR'], trade_type, args.risk_stop_mult, args.risk_tp_mult)
        message = f"{signal_text}\n- Entrada: {entry:.8f} | Stop: {stop:.8f} | TP: {tp:.8f} | R:R = {rr}"
        utils.record_trade(args.trades_log_file, args.symbol, f'CONFIRMED_{trade_type.upper()}_{pattern.upper()}', entry, stop, tp, latest['ATR'], rr, notes=f'confirmation_for_{pattern}')
    return message

# === GESTIÓN DE RIESGO (OPCIÓN A) ===
def compute_risk_levels(entry_price, atr, direction='long', stop_mult=1.5, tp_mult=2.5):
    """Devuelve stop_loss, take_profit, rr (risk:reward) dependiendo de la dirección."""
    if pd.isna(atr) or atr <= 0:
        return None, None, None
    if direction == 'long':
        stop_loss = entry_price - atr * stop_mult
        take_profit = entry_price + atr * tp_mult
        risk = entry_price - stop_loss
        reward = take_profit - entry_price
    else:  # short
        stop_loss = entry_price + atr * stop_mult
        take_profit = entry_price - atr * tp_mult
        risk = stop_loss - entry_price
        reward = entry_price - take_profit

    if risk == 0:
        rr = None
    else:
        rr = round(reward / risk, 2)
    return round(stop_loss, 8), round(take_profit, 8), rr

# === MÓDULO WYCKOFF SIMPLIFICADO ===
def detect_wyckoff_event(df, args):
    """Detecta Spring (long) o Upthrust (short) en la última vela según reglas simplificadas.
    Retorna (event_type, message, entry, stop, tp, atr, rr) o (None, None, ...).
    """
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    atr_mean = df['ATR'].rolling(50).mean().iloc[-1]

    # Condiciones comunes
    vol_ok = latest['volume'] > latest['volume_sma'] * args.wyckoff_volume_mult
    atr_ok = False
    if pd.notna(latest['ATR']) and pd.notna(atr_mean) and atr_mean > 0:
        atr_ok = latest['ATR'] > atr_mean * args.wyckoff_atr_thresh

    # SPRING: falsa ruptura a la baja seguida de recuperación
    if latest['low'] < previous['low'] and latest['close'] > previous['close'] and vol_ok and atr_ok:
        entry = latest['close']
        stop, tp, rr = compute_risk_levels(entry, latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult)
        msg = f"🌱 SPRING detectado | Entrada: {entry:.8f} | ATR: {latest['ATR']:.8f}\n"
        msg += "- Falsa ruptura por debajo del mínimo previo y recuperación\n"
        msg += f"- Volumen >= {args.wyckoff_volume_mult}×SMA volumen | ATR >= {args.wyckoff_atr_thresh}×ATR_mean\n"
        if stop and tp:
            msg += f"- Stop: {stop:.8f} | TP: {tp:.8f} | R:R = {rr}\n"
        return 'SPRING', msg, entry, stop, tp, latest['ATR'], rr

    # UPTHRUST: falsa ruptura al alza seguida de rechazo
    if latest['high'] > previous['high'] and latest['close'] < previous['close'] and vol_ok and atr_ok:
        entry = latest['close']
        stop, tp, rr = compute_risk_levels(entry, latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult)
        msg = f"🏔️ UPTHRUST detectado | Entrada: {entry:.8f} | ATR: {latest['ATR']:.8f}\n"
        msg += "- Falsa ruptura por encima del máximo previo y rechazo\n"
        msg += f"- Volumen >= {args.wyckoff_volume_mult}×SMA volumen | ATR >= {args.wyckoff_atr_thresh}×ATR_mean\n"
        if stop and tp:
            msg += f"- Stop: {stop:.8f} | TP: {tp:.8f} | R:R = {rr}\n"
        return 'UPTHRUST', msg, entry, stop, tp, latest['ATR'], rr

    return None, None, None, None, None, None, None

# === EVALUACIÓN DE SEÑALES (con gestión de riesgo y Wyckoff) ===
def evaluate_trade(df, args):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    signals = []
    pending_state = None

    atr_mean = df['ATR'].rolling(50).mean().iloc[-1]
    if pd.isna(latest['ATR']) or pd.isna(atr_mean) or latest['ATR'] < atr_mean:
        logging.info("ATR bajo: mercado sin volatilidad significativa, no se generan señales.")
        return "⏳ Volatilidad baja (ATR bajo). No se recomienda operar ahora."

    poc_zone = utils.check_poc_zone(latest, args.poc)

    # Patrones clásicos
    if utils.is_hammer(latest['open'], latest['close'], latest['high'], latest['low'], args.hammer_multiplier):
        entry = latest['close']
        stop_loss, take_profit, rr = compute_risk_levels(entry, latest['ATR'], 'long', args.risk_stop_mult, args.risk_tp_mult)
        signal_text = f"🕯️ Martillo detectado | Entrada: {entry:.8f} | ATR: {latest['ATR']:.8f}\n"
        if latest['close'] <= latest['Boll_Lower']:
            signal_text += "- Tocando banda inferior de Bollinger 📉\n"
        if poc_zone:
            signal_text += f"- En ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥\n"
        if latest['ATR'] > atr_mean:
            signal_text += "- Alta volatilidad 🔥\n"
        if stop_loss and take_profit:
            signal_text += f"- Stop: {stop_loss:.8f} | TP: {take_profit:.8f} | R:R = {rr}\n"
        signals.append(signal_text)
        utils.record_trade(args.trades_log_file, args.symbol, 'LONG_HAMMER', entry, stop_loss, take_profit, latest['ATR'], rr, notes='hammer', timestamp=latest['timestamp'])
        pending_state = {"pattern": "hammer", "price": entry}

    if utils.is_shooting_star(latest['open'], latest['close'], latest['high'], latest['low'], args.shooting_star_multiplier):
        entry = latest['close']
        stop_loss, take_profit, rr = compute_risk_levels(entry, latest['ATR'], 'short', args.risk_stop_mult, args.risk_tp_mult)
        signal_text = f"🕯️ Estrella Fugaz detectada | Entrada: {entry:.8f} | ATR: {latest['ATR']:.8f}\n"
        if latest['close'] >= latest['Boll_Upper']:
            signal_text += "- Tocando banda superior de Bollinger 📈\n"
        if poc_zone:
            signal_text += f"- En ZONA DE RESISTENCIA POC (${args.poc:.2f}) ⚠️\n"
        if latest['ATR'] > atr_mean:
            signal_text += "- Alta volatilidad ⚡\n"
        if stop_loss and take_profit:
            signal_text += f"- Stop: {stop_loss:.8f} | TP: {take_profit:.8f} | R:R = {rr}\n"
        signals.append(signal_text)
        utils.record_trade(args.trades_log_file, args.symbol, 'SHORT_SHOOTINGSTAR', entry, stop_loss, take_profit, latest['ATR'], rr, notes='shooting_star', timestamp=latest['timestamp'])
        pending_state = {"pattern": "shooting_star", "price": entry}

    # Wyckoff events (opcional)
    if args.wyckoff:
        ev_type, ev_msg, entry, stop, tp, ev_atr, ev_rr = detect_wyckoff_event(df, args)
        if ev_type == 'SPRING':
            signals.append(ev_msg)
            utils.record_trade(args.trades_log_file, args.symbol, 'LONG_WYCKOFF_SPRING', entry, stop, tp, ev_atr, ev_rr, notes='wyckoff_spring', timestamp=latest['timestamp'])
            pending_state = {"pattern": "wyckoff_spring", "price": entry}
        elif ev_type == 'UPTHRUST':
            signals.append(ev_msg)
            utils.record_trade(args.trades_log_file, args.symbol, 'SHORT_WYCKOFF_UPTHRUST', entry, stop, tp, ev_atr, ev_rr, notes='wyckoff_upthrust', timestamp=latest['timestamp'])
            pending_state = {"pattern": "wyckoff_upthrust", "price": entry}

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

    df = utils.calculate_indicators(df, args.volume_sma_period, args.atr_window, args.bollinger_window)
    
    # Limpiar el log de trades antes de empezar un nuevo backtest
    if os.path.exists(args.trades_log_file):
        os.remove(args.trades_log_file)
    utils.ensure_trades_log_exists(args.trades_log_file)

    logging.info("Recorriendo velas para encontrar y registrar señales...")
    for i in range(201, len(df)): # Empezamos más tarde para asegurar que todos los indicadores están maduros
        sub_df = df.iloc[:i].copy()
        # La función evaluate_trade ya se encarga de registrar los trades
        evaluate_trade(sub_df, args) # evaluate_trade ahora necesita el timestamp

    logging.info(f"\n✅ Backtest de detección de señales completado. Se encontraron trades en '{args.trades_log_file}'.")

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

    message = f"--- Análisis para {args.symbol} ({args.interval}) ---\n{signal}"
    logging.info(message)

    if "⏳" not in signal:
        # Los mensajes de señal ya tienen formato, pero los escapamos por seguridad
        # si no estamos seguros de su contenido.
        utils.send_telegram_message(message, telegram_token, chat_id, pre_escaped=False)

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
    logging.info(f"ATR Window: {args.atr_window}")
    logging.info(f"Bollinger Window: {args.bollinger_window}")
    logging.info(f"Volumen SMA Period: {args.volume_sma_period}")
    logging.info(f"POC: {args.poc}")
    logging.info(f"Símbolo: {args.symbol}")
    logging.info(f"Intervalo: {args.interval}")
    logging.info(f"Risk stop mult: {args.risk_stop_mult} | Risk TP mult: {args.risk_tp_mult}")
    logging.info(f"Trades log file: {args.trades_log_file}")
    logging.info(f"Wyckoff active: {args.wyckoff} | vol mult: {args.wyckoff_volume_mult} | atr thresh: {args.wyckoff_atr_thresh}")
    logging.info("==========================================")

    if args.backtest:
        run_backtest(args)
        sys.exit(0)

    # Limpiar estado anterior al iniciar en modo live para evitar confirmaciones incorrectas
    utils.clear_state()
    logging.info("Estado anterior limpiado. Iniciando en modo de operación en vivo.")

    # Enviar mensaje de inicio a Telegram
    startup_message = (
        f"🚀 *Bot Wyckoff Iniciado* 🚀\n\n"
        f"Monitoreando: `{args.symbol}` en intervalo `{args.interval}`\n"
        f"POC configurado en: `{args.poc}`\n"
        f"Wyckoff activado: `{'Sí' if args.wyckoff else 'No'}`\n\n"
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
            logging.info("Bot detenido manualmente.")
            utils.clear_state()
            sys.exit(0)
        except Exception as e:
            logging.error(f"Error en ciclo principal: {e}")
            time.sleep(60)
