"""
telegram_bot.py
---------------
Belimo Actuator Monitor — Telegram Bot

Commands:
  /start   → Welcome message + available commands
  /status  → Quick snapshot: current gap & torque stats
  /scan    → Full FFT analysis + anomaly report
  /plot    → Send the spectral chart as an image
  /monitor → Toggle auto-monitoring (every 60 sec)
  /stop    → Stop the bot polling

Configuration (via .env):
  TELEGRAM_TOKEN  → Your bot token from @BotFather
  TELEGRAM_CHAT_ID → (Optional) restrict to your own chat
"""

import os
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from telegram.constants import ParseMode

# Import our spectral engine
import sys
sys.path.insert(0, str(Path(__file__).parent))
from spectral_engine import analyze, format_report, load_data, compensate_jitter

# ─── Setup ────────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(
    format="%(asctime)s — %(levelname)s — %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN   = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")   # optional: restrict access

# Auto-monitor interval in seconds
MONITOR_INTERVAL_SEC = 60


# ─── Access Guard ─────────────────────────────────────────────────────────────
async def is_authorized(update: Update) -> bool:
    """
    If TELEGRAM_CHAT_ID is set in .env, only that chat can use the bot.
    This prevents strangers from accessing your actuator data.
    (strangers = unauthorized users who find or guess your bot)
    """
    if CHAT_ID and str(update.effective_chat.id) != str(CHAT_ID):
        await update.message.reply_text("⛔ Unauthorized access.")
        return False
    return True


# ─── Command Handlers ─────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Welcome message."""
    if not await is_authorized(update):
        return

    text = (
        "🤖 *Belimo Actuator Monitor* — Online\n\n"
        "I analyze your actuator telemetry using *Fourier Spectral Analysis*\n"
        "to detect latency gaps and mechanical stress anomalies.\n\n"
        "📋 *Available Commands:*\n"
        "/status  — Quick stats snapshot\n"
        "/scan    — Full FFT anomaly analysis\n"
        "/plot    — Send spectral chart image\n"
        "/monitor — Toggle auto-monitoring (60s)\n"
        "/stop    — Stop monitoring\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Quick snapshot: load data and show live gap/torque stats.
    No FFT here — just raw signal statistics.
    """
    if not await is_authorized(update):
        return

    await update.message.reply_text("⏳ Reading telemetry data...")

    try:
        df = load_data()
        import numpy as np
        gap = (df["setpoint_position_%"] - df["feedback_position_%"]).to_numpy()
        peak_gap    = round(float(np.max(np.abs(gap))),  3)
        mean_gap    = round(float(np.mean(np.abs(gap))), 3)
        peak_torque = round(float(df["motor_torque_Nmm"].abs().max()), 3)
        n_tests     = df["test_number"].nunique()
        last_ts     = df["timestamp"].iloc[-1].strftime("%Y-%m-%d %H:%M:%S UTC")

        status_ok = peak_gap <= 10.0 and peak_torque <= 1.5
        indicator = "✅ Normal" if status_ok else "🚨 Alert"

        text = (
            f"{indicator}\n\n"
            f"📡 *Live Telemetry Snapshot*\n"
            f"├ Last timestamp  : `{last_ts}`\n"
            f"├ Total samples   : `{len(df)}`\n"
            f"├ Test cycles     : `{n_tests}`\n"
            f"├ Mean latency gap: `{mean_gap}%`\n"
            f"├ Peak latency gap: `{peak_gap}%`\n"
            f"└ Peak torque     : `{peak_torque} Nmm`\n"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        logger.error(f"Status error: {e}")
        await update.message.reply_text(f"❌ Error reading data: `{e}`",
                                         parse_mode=ParseMode.MARKDOWN)


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Run the full FFT anomaly detection pipeline and report results.
    This is the main analysis command.
    """
    if not await is_authorized(update):
        return

    await update.message.reply_text("🔬 Running spectral analysis (FFT)... please wait.")

    try:
        report = analyze()
        text   = format_report(report)
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        logger.error(f"Scan error: {e}")
        await update.message.reply_text(f"❌ Analysis failed: `{e}`",
                                         parse_mode=ParseMode.MARKDOWN)


async def cmd_plot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Run FFT and send the spectral power chart as an image.
    Uses matplotlib output saved by the spectral engine.
    """
    if not await is_authorized(update):
        return

    await update.message.reply_text("📈 Generating spectral plot...")

    try:
        report = analyze()
        plot_file = Path(report.plot_path)

        if not plot_file.exists():
            await update.message.reply_text("❌ Plot file not found.")
            return

        caption = (
            "⚡ *Belimo Power Spectrum — Latency Gap Signal*\n"
            "Red dashed lines = dominant anomalous frequencies.\n"
            f"Peak gap: `{report.peak_gap}%` | Peak torque: `{report.peak_torque} Nmm`"
        )

        with open(plot_file, "rb") as f:
            await update.message.reply_photo(
                photo=f,
                caption=caption,
                parse_mode=ParseMode.MARKDOWN
            )

    except Exception as e:
        logger.error(f"Plot error: {e}")
        await update.message.reply_text(f"❌ Plot failed: `{e}`",
                                         parse_mode=ParseMode.MARKDOWN)


async def cmd_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Start periodic auto-monitoring: run a scan every MONITOR_INTERVAL_SEC seconds.
    Only sends a Telegram alert when an anomaly is detected.
    Uses context.job_queue to schedule repeating tasks.
    (schedule = plan for future execution at intervals)
    """
    if not await is_authorized(update):
        return

    chat_id = update.effective_chat.id

    # Remove existing job if any (toggle behavior)
    current_jobs = context.job_queue.get_jobs_by_name(f"monitor_{chat_id}")
    if current_jobs:
        for job in current_jobs:
            job.schedule_removal()
        await update.message.reply_text(
            "🛑 Auto-monitoring *stopped*.", parse_mode=ParseMode.MARKDOWN
        )
        return

    # Start new monitoring job
    context.job_queue.run_repeating(
        callback=_monitor_callback,
        interval=MONITOR_INTERVAL_SEC,
        first=5,   # start after 5 seconds
        chat_id=chat_id,
        name=f"monitor_{chat_id}",
    )
    await update.message.reply_text(
        f"✅ Auto-monitoring *started* — scanning every `{MONITOR_INTERVAL_SEC}s`.\n"
        "You will be alerted only if an anomaly is detected.\n"
        "Run `/monitor` again to stop.",
        parse_mode=ParseMode.MARKDOWN
    )


async def _monitor_callback(context: ContextTypes.DEFAULT_TYPE):
    """
    Internal callback that runs on schedule.
    Sends an alert ONLY if an anomaly is found.
    This avoids spamming the user with normal-state messages.
    (spamming = sending too many unnecessary messages)
    """
    try:
        report = analyze()
        if report.is_anomaly:
            text = f"🚨 *ANOMALY ALERT* — Auto-monitor triggered\n\n{format_report(report)}"
            await context.bot.send_message(
                chat_id=context.job.chat_id,
                text=text,
                parse_mode=ParseMode.MARKDOWN
            )
            # Also send the plot
            plot_file = Path(report.plot_path)
            if plot_file.exists():
                with open(plot_file, "rb") as f:
                    await context.bot.send_photo(
                        chat_id=context.job.chat_id,
                        photo=f,
                        caption="📊 Spectral chart from auto-scan"
                    )
    except Exception as e:
        logger.error(f"Monitor callback error: {e}")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop all monitoring jobs for this chat."""
    if not await is_authorized(update):
        return

    chat_id = update.effective_chat.id
    jobs = context.job_queue.get_jobs_by_name(f"monitor_{chat_id}")
    for job in jobs:
        job.schedule_removal()

    await update.message.reply_text("🛑 All monitoring stopped. Use /start to resume.")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not TOKEN:
        raise ValueError(
            "TELEGRAM_TOKEN not found!\n"
            "Create a .env file with: TELEGRAM_TOKEN=your_token_here"
        )

    logger.info("Starting Belimo Telegram Bot...")

    app = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    # Register all command handlers
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("scan",    cmd_scan))
    app.add_handler(CommandHandler("plot",    cmd_plot))
    app.add_handler(CommandHandler("monitor", cmd_monitor))
    app.add_handler(CommandHandler("stop",    cmd_stop))

    logger.info("Bot is running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
