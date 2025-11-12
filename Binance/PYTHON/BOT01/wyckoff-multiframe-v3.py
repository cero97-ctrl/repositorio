# wyckoff-multiframe-v3.py — (Modo profesional Wyckoff multi-timeframe + Gestión de riesgo básica)
# VERSIÓN AVANZADA con MÁQUINA DE ESTADOS para detección de fases HTF.
# Indicadores: EMA50/200, ATR, Volumen SMA.
# Parametros: POC (Point of Control), Backtesting, dotenv

import pandas as pd
import numpy as np
import time
import logging
import os
import sys
import argparse
import json
from datetime import datetime
import common_utils as utils

# -----------------------------
# GESTIÓN DE ESTADO HTF (para la máquina de estados)
# -----------------------------
HTF_STATE_FILE = "htf_state.json"


def save_htf_state(state):
    with open(HTF_STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)


def load_htf_state():
    if os.path.exists(HTF_STATE_FILE):
        with open(HTF_STATE_FILE, "r") as f:
            return json.load(f)
    # Estado inicial por defecto
    return {"phase": "SEARCHING"}


def clear_htf_state():
    if os.path.exists(HTF_STATE_FILE):
        os.remove(HTF_STATE_FILE)


# -----------------------------
# CONFIRMACIÓN (Sin cambios respecto a v2)
# -----------------------------
def check_confirmation(df, state, args):
    latest = df.iloc[-1]
    previous = df.iloc[-2]
    pattern = state.get("pattern")
    signal_time_str = state.get("signal_time")

    message = ""
    trade_type = None

    if signal_time_str:
        signal_time = pd.to_datetime(signal_time_str)
        if pd.to_datetime(latest["timestamp"], unit="ms") <= signal_time:
            return (
                f"⏳ Esperando vela de confirmación para el patrón {pattern.upper()}..."
            )

    sl_buffer_val = getattr(args, "sl_buffer", 0.0)

    if pattern == "hammer" and latest["close"] > previous["high"]:
        trade_type = "long"
        signal_text = f"✅ Confirmación alcista para patrón {pattern.upper()}"
    elif pattern == "shooting_star" and latest["close"] < previous["low"]:
        trade_type = "short"
        signal_text = f"✅ Confirmación bajista para patrón {pattern.upper()}"
    elif pattern == "wyckoff_spring" and latest["close"] > previous["high"]:
        trade_type = "long"
        signal_text = f"✅ Confirmación alcista para SPRING"
    elif pattern == "wyckoff_upthrust" and latest["close"] < previous["low"]:
        trade_type = "short"
        signal_text = f"✅ Confirmación bajista para UPTHRUST"
    else:
        message = f"❌ Sin confirmación para el patrón {pattern.upper()}"

    utils.clear_state()
    if trade_type:
        entry_price, stop, tp, rr = utils.compute_risk_levels(
            latest["close"],
            latest["ATR"],
            trade_type,
            args.risk_stop_mult,
            args.risk_tp_mult,
            sl_buffer_val,
        )
        message = utils.format_risk_management_message(
            signal_text, entry_price, stop, tp, rr
        )
        utils.record_trade(
            args.trades_log_file,
            args.symbol,
            f"CONFIRMED_{trade_type.upper()}_{pattern.upper()}",
            entry_price,
            stop,
            tp,
            latest["ATR"],
            rr,
            notes=f"confirmation_for_{pattern}",
            timestamp=latest["timestamp"],
        )
    return message


# -----------------------------
# MÓDULO WYCKOFF (LTF events) (Sin cambios respecto a v2)
# -----------------------------
def detect_wyckoff_event(df, args, sl_buffer_val):
    latest = df.iloc[-1]
    previous = df.iloc[-2]

    vol_ok = latest["volume"] > latest["volume_sma"] * args.wyckoff_volume_mult
    atr_mean = utils.get_atr_mean_for_volatility(df)
    atr_ok = False
    if pd.notna(latest["ATR"]) and pd.notna(atr_mean) and atr_mean > 0:
        atr_ok = latest["ATR"] > atr_mean * args.wyckoff_atr_thresh

    if (
        latest["low"] < previous["low"]
        and latest["close"] > previous["close"]
        and vol_ok
        and atr_ok
    ):
        entry = latest["close"]
        entry_price, stop, tp, rr = utils.compute_risk_levels(
            entry,
            latest["ATR"],
            "long",
            args.risk_stop_mult,
            args.risk_tp_mult,
            sl_buffer_val,
        )
        msg = f"🌱 SPRING detectado | Entrada: {entry_price:.8f} | ATR: {latest['ATR']:.8f}\n"
        msg += f"- Volumen >= {args.wyckoff_volume_mult}×SMA volumen | ATR >= {args.wyckoff_atr_thresh}×ATR_mean\n"
        if stop and tp:
            msg = utils.format_risk_management_message(msg, entry_price, stop, tp, rr)
        return "SPRING", msg, entry_price, stop, tp, latest["ATR"], rr

    if (
        latest["high"] > previous["high"]
        and latest["close"] < previous["close"]
        and vol_ok
        and atr_ok
    ):
        entry = latest["close"]
        entry_price, stop, tp, rr = utils.compute_risk_levels(
            entry,
            latest["ATR"],
            "short",
            args.risk_stop_mult,
            args.risk_tp_mult,
            sl_buffer_val,
        )
        msg = f"🏔️ UPTHRUST detectado | Entrada: {entry_price:.8f} | ATR: {latest['ATR']:.8f}\n"
        msg += f"- Volumen >= {args.wyckoff_volume_mult}×SMA volumen | ATR >= {args.wyckoff_atr_thresh}×ATR_mean\n"
        if stop and tp:
            msg = utils.format_risk_management_message(msg, entry_price, stop, tp, rr)
        return "UPTHRUST", msg, entry_price, stop, tp, latest["ATR"], rr

    return None, None, None, None, None, None, None


# -----------------------------
# MÁQUINA DE ESTADOS WYCKOFF HTF (MEJORA 3)
# -----------------------------


def is_selling_climax(candle, prev_candle, args):
    """Busca un evento de clímax de venta."""
    vol_mult = getattr(args, "htf_climactic_vol_mult", 2.5)
    is_climactic_volume = candle["volume"] > candle["volume_sma"] * vol_mult
    is_wide_range = (candle["high"] - candle["low"]) > candle["ATR"] * 1.5
    is_down_move = candle["low"] < prev_candle["low"]
    return is_climactic_volume and is_wide_range and is_down_move


def is_automatic_rally(df_slice):
    """Busca el punto más alto después del clímax."""
    return df_slice["high"].idxmax()


def is_secondary_test(candle, tr_support, args):
    """Busca un testeo del soporte con volumen bajo."""
    vol_mult = getattr(args, "htf_test_vol_mult", 0.8)
    is_low_volume = candle["volume"] < candle["volume_sma"] * vol_mult
    is_testing_support = (
        abs(candle["low"] - tr_support) / tr_support < 0.02
    )  # Testeo dentro del 2%
    return is_low_volume and is_testing_support


def is_sign_of_strength(candle, tr_resistance, args):
    """Busca una ruptura de la resistencia con volumen."""
    vol_mult = getattr(args, "htf_breakout_vol_mult", 1.5)
    is_breakout_volume = candle["volume"] > candle["volume_sma"] * vol_mult
    is_breaking_resistance = candle["close"] > tr_resistance
    return is_breakout_volume and is_breaking_resistance


def detect_htf_phase(df_htf, args):
    """
    MEJORA 3: Máquina de estados para detectar fases de Wyckoff en HTF.
    """
    if df_htf.empty or len(df_htf) < 50:
        return "unknown"

    htf_state = load_htf_state()
    current_phase = htf_state.get("phase", "SEARCHING")

    latest = df_htf.iloc[-1]
    previous = df_htf.iloc[-2]

    # --- FASE DE BÚSQUEDA ---
    if current_phase == "SEARCHING":
        if is_selling_climax(latest, previous, args):
            htf_state = {
                "phase": "ACCUMULATION_PHASE_A",
                "sc_low": latest["low"],
                "sc_time": latest["timestamp"].isoformat(),
            }
            logging.info(
                f"[HTF State] Selling Climax detectado. Cambiando a ACCUMULATION_PHASE_A. Soporte inicial en {htf_state['sc_low']:.2f}"
            )
            save_htf_state(htf_state)
            return htf_state["phase"]
        # Aquí iría la lógica para Buying Climax y entrar en Distribución

    # --- FASE A DE ACUMULACIÓN ---
    elif current_phase == "ACCUMULATION_PHASE_A":
        sc_time = datetime.fromisoformat(htf_state["sc_time"])
        df_after_sc = df_htf[df_htf["timestamp"] > sc_time]
        if not df_after_sc.empty:
            ar_index = is_automatic_rally(df_after_sc)
            ar_high = df_htf.loc[ar_index]["high"]
            htf_state.update(
                {
                    "phase": "ACCUMULATION_PHASE_B",
                    "ar_high": ar_high,
                    "tr_support": htf_state["sc_low"],
                    "tr_resistance": ar_high,
                }
            )
            logging.info(
                f"[HTF State] Automatic Rally detectado. Cambiando a ACCUMULATION_PHASE_B. Resistencia en {ar_high:.2f}"
            )
            save_htf_state(htf_state)
            return htf_state["phase"]

    # --- FASE B DE ACUMULACIÓN ---
    elif current_phase == "ACCUMULATION_PHASE_B":
        if is_secondary_test(latest, htf_state["tr_support"], args):
            logging.info(
                f"[HTF State] Secondary Test exitoso en {latest['low']:.2f} con bajo volumen. Se confirma Acumulación."
            )
            # Podríamos añadir más lógica para Phase C (Spring) aquí
            # Por ahora, un ST exitoso nos pone en un estado favorable para largos
            htf_state["phase"] = "ACCUMULATION_CONFIRMED"
            save_htf_state(htf_state)
            return htf_state["phase"]
        # Si rompemos la resistencia, pasamos a Markup
        if is_sign_of_strength(latest, htf_state["tr_resistance"], args):
            htf_state["phase"] = "MARKUP"
            logging.info(f"[HTF State] Sign of Strength detectado. Cambiando a MARKUP.")
            save_htf_state(htf_state)
            return htf_state["phase"]

    # --- FASE DE MARKUP ---
    elif current_phase in ["ACCUMULATION_CONFIRMED", "MARKUP"]:
        # Nos mantenemos en Markup mientras las EMAs sean alcistas
        if latest["EMA_50"] < latest["EMA_200"]:
            logging.info(
                "[HTF State] Cruce bajista de EMAs. Finalizando Markup. Volviendo a SEARCHING."
            )
            clear_htf_state()  # Reiniciar la máquina de estados
            return "SEARCHING"
        return current_phase

    # Si ninguna lógica coincide, simplemente devolvemos la fase actual
    return current_phase


# -----------------------------
# EVALUACIÓN DE SEÑALES (principal)
# -----------------------------
def evaluate_trade(df, args, log_file=None):
    latest = df.iloc[-1]
    signals = []
    pending_state = None

    atr_mean = utils.get_atr_mean_for_volatility(df)
    if pd.isna(latest["ATR"]) or pd.isna(atr_mean) or latest["ATR"] < atr_mean:
        logging.info(
            "ATR bajo: mercado sin volatilidad significativa, no se generan señales."
        )
        return "⚠️ Volatilidad baja (ATR bajo). No se recomienda operar ahora."

    poc_zone = utils.check_poc_zone(latest, args.poc)
    sl_buffer_val = getattr(args, "sl_buffer", 0.0)

    ev_type, ev_msg, entry_wyckoff, stop_wyckoff, tp_wyckoff, ev_atr, ev_rr = (
        detect_wyckoff_event(df, args, sl_buffer_val)
    )

    htf_phase = "unknown"
    if args.wyckoff_professional:
        df_htf = utils.get_klines(args.symbol, args.htf_interval, args.htf_limit)
        if df_htf.empty:
            logging.warning(
                "No se pudieron obtener datos HTF para confirmación Wyckoff profesional."
            )
        else:
            df_htf = utils.calculate_indicators(
                df_htf,
                args.volume_sma_period,
                args.atr_window,
                getattr(args, "bollinger_window", 20),
            )
            df_htf["timestamp"] = pd.to_datetime(
                df_htf["timestamp"], unit="ms"
            )  # Asegurar datetime
            htf_phase = detect_htf_phase(df_htf, args)
            logging.info(f"Fase HTF detectada (máquina de estados): {htf_phase}")

    valid_long_phases = ("MARKUP", "ACCUMULATION_CONFIRMED", "ACCUMULATION_PHASE_B")
    valid_short_phases = ("MARKDOWN", "DISTRIBUTION_CONFIRMED", "DISTRIBUTION_PHASE_B")

    # Hammer
    if utils.is_hammer(
        latest["open"],
        latest["close"],
        latest["high"],
        latest["low"],
        args.hammer_multiplier,
    ):
        entry_p, sl, tp_p, rr = utils.compute_risk_levels(
            latest["close"],
            latest["ATR"],
            "long",
            args.risk_stop_mult,
            args.risk_tp_mult,
            sl_buffer_val,
        )
        signal_text = f"🕯️ Martillo detectado | Entrada: {entry_p:.8f} | ATR: {latest['ATR']:.8f}\n"
        if poc_zone:
            signal_text += f"- En ZONA DE SOPORTE POC (${args.poc:.2f}) 🔥\n"
        if sl and tp_p:
            signal_text = utils.format_risk_management_message(
                signal_text, entry_p, sl, tp_p, rr
            )

        if args.wyckoff_professional:
            if htf_phase in valid_long_phases:
                signals.append(signal_text)
                utils.record_trade(
                    file_path=log_file or args.trades_log_file,
                    symbol=args.symbol,
                    ttype="LONG",
                    entry=entry_p,
                    stop_loss=sl,
                    take_profit=tp_p,
                    atr=latest["ATR"],
                    rr=rr,
                    notes="hammer",
                    timestamp=latest["timestamp"],
                )
                pending_state = {"pattern": "hammer", "price": entry_p}
            else:
                logging.info(
                    f"Martillo detectado pero HTF no compatible ({htf_phase}), descartado."
                )
        else:
            signals.append(signal_text)
            # Aplicar la misma mejora aquí y en el resto de llamadas a record_trade
            utils.record_trade(
                log_file or args.trades_log_file,
                args.symbol,
                "LONG",
                entry_p,
                sl,
                tp_p,
                latest["ATR"],
                rr,
                notes="hammer",
                timestamp=latest["timestamp"],
            )
            pending_state = {"pattern": "hammer", "price": entry_p}

    # Shooting star
    if utils.is_shooting_star(
        latest["open"],
        latest["close"],
        latest["high"],
        latest["low"],
        args.shooting_star_multiplier,
    ):
        entry_p, sl, tp_p, rr = utils.compute_risk_levels(
            latest["close"],
            latest["ATR"],
            "short",
            args.risk_stop_mult,
            args.risk_tp_mult,
            sl_buffer_val,
        )
        signal_text = f"🕯️ Estrella Fugaz detectada | Entrada: {entry_p:.8f} | ATR: {latest['ATR']:.8f}\n"
        if poc_zone:
            signal_text += f"- En ZONA DE RESISTENCIA POC (${args.poc:.2f}) ⚠️\n"
        if sl and tp_p:
            signal_text = utils.format_risk_management_message(
                signal_text, entry_p, sl, tp_p, rr
            )

        if args.wyckoff_professional:
            if htf_phase in valid_short_phases:
                signals.append(signal_text)
                utils.record_trade(
                    log_file or args.trades_log_file,
                    args.symbol,
                    "SHORT",
                    entry_p,
                    sl,
                    tp_p,
                    latest["ATR"],
                    rr,
                    "shooting_star",
                    latest["timestamp"],
                )
                pending_state = {"pattern": "shooting_star", "price": entry_p}
            else:
                logging.info(
                    f"Estrella Fugaz detectada pero HTF no compatible ({htf_phase}), descartado."
                )
        else:
            signals.append(signal_text)
            utils.record_trade(
                log_file or args.trades_log_file,
                args.symbol,
                "SHORT",
                entry_p,
                sl,
                tp_p,
                latest["ATR"],
                rr,
                "shooting_star",
                latest["timestamp"],
            )
            pending_state = {"pattern": "shooting_star", "price": entry_p}

    # Wyckoff LTF events
    if ev_type:
        if args.wyckoff_professional:
            if ev_type == "SPRING" and htf_phase in valid_long_phases:
                signals.append(ev_msg)
                utils.record_trade(
                    log_file or args.trades_log_file,
                    args.symbol,
                    "LONG_WYCKOFF",
                    entry_wyckoff,
                    stop_wyckoff,
                    tp_wyckoff,
                    ev_atr,
                    ev_rr,
                    "wyckoff_spring",
                    latest["timestamp"],
                )
                pending_state = {"pattern": "wyckoff_spring", "price": entry_wyckoff}
            elif ev_type == "UPTHRUST" and htf_phase in valid_short_phases:
                signals.append(ev_msg)
                utils.record_trade(
                    log_file or args.trades_log_file,
                    args.symbol,
                    "SHORT_WYCKOFF",
                    entry_wyckoff,
                    stop_wyckoff,
                    tp_wyckoff,
                    ev_atr,
                    ev_rr,
                    "wyckoff_upthrust",
                    latest["timestamp"],
                )
                pending_state = {"pattern": "wyckoff_upthrust", "price": entry_wyckoff}
            else:
                logging.info(
                    f"Evento Wyckoff {ev_type} detectado pero HTF no compatible ({htf_phase}), descartado."
                )
        else:
            signals.append(ev_msg)
            trade_type_note = (
                "wyckoff_spring" if ev_type == "SPRING" else "wyckoff_upthrust"
            )
            utils.record_trade(
                log_file or args.trades_log_file,
                args.symbol,
                f"{ev_type}_WYCKOFF",
                entry_wyckoff,
                stop_wyckoff,
                tp_wyckoff,
                ev_atr,
                ev_rr,
                trade_type_note,
                latest["timestamp"],
            )
            pending_state = {
                "pattern": f"wyckoff_{ev_type.lower()}",
                "price": entry_wyckoff,
            }

    if pending_state:
        pending_state["signal_time"] = pd.to_datetime(
            latest["timestamp"], unit="ms"
        ).isoformat()
        utils.save_state(pending_state)
        signals.append("⏳ Esperando vela de confirmación en el próximo ciclo...")

    return "\n".join(signals) if signals else "⏳ Sin señales claras."


# -----------------------------
# BACKTEST (Sin cambios respecto a v2)
# -----------------------------
def run_backtest(args):
    logging.info(f"Iniciando backtest con datos: {args.backtest_file}")
    try:
        df = pd.read_csv(args.backtest_file)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    except Exception as e:
        logging.error(f"Error al cargar CSV: {e}")
        return

    if not {"timestamp", "open", "high", "low", "close", "volume"}.issubset(df.columns):
        logging.error(
            "El CSV debe contener columnas: timestamp, open, high, low, close, volume"
        )
        return

    if args.limit > 0 and len(df) > args.limit:
        df = df.tail(args.limit).reset_index(drop=True)

    df = utils.calculate_indicators(
        df,
        args.volume_sma_period,
        args.atr_window,
        getattr(args, "bollinger_window", 20),
    )

    backtest_log_file = args.trades_log_file.replace(".csv", "_backtest.csv")

    if os.path.exists(backtest_log_file):
        os.remove(backtest_log_file)
    utils.ensure_trades_log_exists(backtest_log_file)

    logging.info("Limpiando estados de backtest anteriores...")
    utils.clear_state()
    clear_htf_state()

    logging.info("Recorriendo velas para encontrar y registrar señales...")
    start_index = max(201, args.atr_window, args.volume_sma_period)

    for i in range(start_index, len(df)):
        sub_df = df.iloc[: i + 1].copy()
        pending_state = utils.load_state()
        signal_message = ""

        if pending_state:
            signal_message = check_confirmation(sub_df, pending_state, args)
        else:
            signal_message = evaluate_trade(sub_df, args, backtest_log_file)

        if (
            signal_message
            and "⏳ Sin señales claras." not in signal_message
            and "⚠️ Volatilidad baja" not in signal_message
        ):
            logging.info(f"[{i}] {signal_message}")

    logging.info(
        f"Ordenando el archivo de log de backtest '{backtest_log_file}' por fecha..."
    )
    try:
        log_df = pd.read_csv(backtest_log_file)
        log_df["timestamp"] = pd.to_datetime(log_df["timestamp"])
        log_df.sort_values(by="timestamp", inplace=True)
        log_df.to_csv(backtest_log_file, index=False)
    except Exception as e:
        logging.error(f"No se pudo ordenar el archivo de log de backtest: {e}")

    logging.info(
        f"\n✅ Backtest de detección de señales completado. Se encontraron trades en '{backtest_log_file}'."
    )


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    args, telegram_token, chat_id = utils.setup_logging_and_config()

    logging.info("==========================================")
    logging.info(
        "Iniciando Bot Wyckoff Multi-Timeframe v3 (con Máquina de Estados HTF)"
    )
    logging.info(f"Símbolo: {args.symbol} | Intervalo: {args.interval}")
    logging.info(
        f"Wyckoff PROFESSIONAL: {args.wyckoff_professional} | HTF: {args.htf_interval}"
    )
    logging.info(
        f"HTF Vol Multipliers: Climactic={args.htf_climactic_vol_mult}, Test={args.htf_test_vol_mult}, Breakout={args.htf_breakout_vol_mult}"
    )
    logging.info("==========================================")

    if args.backtest:
        run_backtest(args)
        sys.exit(0)

    # Limpiar estados al iniciar en modo live
    utils.clear_state()
    clear_htf_state()
    logging.info(
        "Estados (LTF y HTF) limpiados. Iniciando en modo de operación en vivo."
    )

    startup_message = (
        f"🚀 *Bot Wyckoff Multi\\-Timeframe v3 Iniciado* 🚀\n\n"
        f"Monitoreando: `{args.symbol}` en intervalo `{args.interval}`\n"
        f"Modo profesional Wyckoff: `{'Sí' if args.wyckoff_professional else 'No'}`\n"
        f"Análisis HTF: `Máquina de Estados Activada`\n\n"
        "El bot está en línea y funcionando correctamente\\."
    )

    utils.run_bot_main_loop(
        args,
        telegram_token,
        chat_id,
        evaluate_trade,
        check_confirmation,
        startup_message,
    )
