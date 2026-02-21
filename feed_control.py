import asyncio
import datetime
import os
import subprocess
import sys
import time

import pandas as pd
from sklearn.linear_model import LinearRegression
from unitelabs.sdk import client, connect, core

from commons import get_logger, iterate, write_to_database

# The target flow rate the script will try to achieve to set in mL/min
TARGET_FLOW_RATE = 0.88  # mL/min

# The start flow rate (without proper calibration) in m^3/s
START_FLOW_RATE = 2.8125000000000008e-08

EXPECTED_WEIGHT_CHANGE_RATE = 0.88  # g/min

# The time the script will refrain from adjusting the flow rate after a flow rate event has been executed in s
MIN_FLOW_RATE_CHANGE_INTERVAL = 30 * 60  # s

# The number of datapoints to consider when calculating the actual flow rate
N_LAST = 120

# The time interval (in s) between measurements
INTERVAL = 10  # s

# The duration (in s) the script will run for before aborting and stopping the pump
DURATION = 600 * 60 * 60  # s

# upper flow rate lim in m^3/s
LOWER_LIM = 1.890000000000001e-10  # m^3/s

# lower flow rate lim in m^3/s
UPPER_LIM = 1.766666666666667e-5  # m^3/s

# Write the data into a file with the following path
os.makedirs("results", exist_ok=True)
FILEPATH = f"results/{datetime.datetime.now()}_Feed_data.csv"

UNIT_CONVERSION_FACTOR = ((0.1 * 0.1 * 0.1) / 1000) / 60  # (mL/min to m^3/s)


FLOW_NAME = "Feed"
logger = get_logger(FLOW_NAME)

# Shared state updated by subscription tasks and read by the control loop.
current_weight = 0
current_flow_rate = 0

history = []

error_counter_flow = 0
error_counter_weight = 0

pump_subscription = None
balance_subscription = None


async def prepare_pump(pump: core.Service):
    logger.info("Preparing pump")
    await pump.pump_actuator_controller_2.stop()
    await asyncio.sleep(1)
    await pump.pump_actuator_controller_2.set_flow_rate(flow_rate=START_FLOW_RATE)
    await asyncio.sleep(1)

    # Start asynchronously so the loop can continue without blocking on stream startup.
    asyncio.create_task(pump.pump_actuator_controller_2.start(flow_rate=START_FLOW_RATE))

    logger.info("Started pump")
    await asyncio.sleep(1)


async def set_flow_rate(pump: core.Service, flow_rate: float):
    await pump.pump_actuator_controller_2.set_flow_rate(flow_rate=flow_rate)


async def subscribe_flow_rate(subscription):
    global current_flow_rate, error_counter_flow
    async for item in subscription:
        if isinstance(item, float) or isinstance(item, int):
            error_counter_flow = 0
            current_flow_rate = item
        else:
            error_counter_flow += 1
            if (error_counter_flow < 100 and error_counter_flow % 10 == 0) or error_counter_flow == 1 or error_counter_flow % 500 == 0:
                await asyncio.sleep(1)


async def subscribe_flow_rate_with_timeout(pump: core.Service):
    global current_flow_rate
    # Re-open the stream if it stalls for too long.
    async with await pump.pump_actuator_controller_2.subscribe_flow_rate() as subscription:
        try:
            await asyncio.wait_for(subscribe_flow_rate(subscription), 300)
        except TimeoutError:
            pass


async def subscribe_weight(subscription):
    global current_weight, error_counter_weight
    async for item in subscription:
        if isinstance(item, float) or isinstance(item, int):
            error_counter_weight = 0
            current_weight = item
        else:
            error_counter_weight += 1
            if (error_counter_weight < 100 and error_counter_weight % 10 == 0) or error_counter_weight == 1 or error_counter_weight % 500 == 0:
                await asyncio.sleep(1)


async def subscribe_weight_with_timeout(balance: core.Service):
    global current_weight
    # Re-open the stream if it stalls for too long.
    async with await balance.weighing_service.subscribe_weight() as subscription:
        try:
            await asyncio.wait_for(subscribe_weight(subscription), 300)
        except TimeoutError:
            pass


async def run():
    global history, pump_subscription, balance_subscription
    pump = await connect(name="Reglo ICC 4x Anaerob")
    channel_addressing = await pump.device_service.get_channel_addressing()
    logger.info("Channel addressing is set to: %s", channel_addressing)
    if not await pump.device_service.get_channel_addressing():
        logger.info("Channel addressing was set to False. Switching to True")
        await pump.device_service.set_channel_addressing(mode=True)

        # Verify the device accepted the addressing mode change before proceeding.
        i = 10
        while True:
            await asyncio.sleep(5)
            channel_addressing = await pump.device_service.get_channel_addressing()
            logger.info("Channel addressing is now set to: %s", channel_addressing)
            if channel_addressing == True:
                break
            i -= 1
            if i == 0:
                raise RuntimeError("Changing channel addressing setting unsuccessful.")

    balance = await connect(name="KERN DS-20k0.1 Feed")
    try:
        pump_subscription = asyncio.create_task(subscribe_flow_rate_with_timeout(pump))
        balance_subscription = asyncio.create_task(subscribe_weight_with_timeout(balance))
        await prepare_pump(pump)
        await asyncio.sleep(7)

        start_weight = current_weight
        next_flow_rate = START_FLOW_RATE
        actual_flow_rate = 0
        r_sq = 0.00
        correction_factor = 0.00
        last_flow_rate_adjustment = time.time()
        async for iteration in iterate(INTERVAL, DURATION):
            # Keep long-running subscriptions alive.
            if balance_subscription.done() or balance_subscription.cancelled():
                balance_subscription = asyncio.create_task(subscribe_weight_with_timeout(balance))

            if pump_subscription.done() or pump_subscription.cancelled():
                pump_subscription = asyncio.create_task(subscribe_flow_rate_with_timeout(pump))

            timestamp = time.time()
            logger.info(
                "%s/%s [%s] weight measured: %s, flow_rate_setpoint: %s",
                iteration,
                int(DURATION / INTERVAL),
                timestamp,
                current_weight,
                current_flow_rate,
            )

            weight_expected = start_weight + (EXPECTED_WEIGHT_CHANGE_RATE * (iteration * INTERVAL / 60))
            weight_diff_abs = current_weight - weight_expected

            # Persist a full local trace on every control tick.
            history.append(
                {
                    "timestamp": timestamp,
                    "time rel.": iteration * INTERVAL,
                    "flow_rate_set": current_flow_rate,
                    "weight_measured": current_weight,
                    "weight_expected": weight_expected,
                    "weight_diff_abs": weight_diff_abs,
                    "set_flow_rate": next_flow_rate,
                    "actual_flow_rate": actual_flow_rate,
                    "r_sq": r_sq,
                    "correction_factor": correction_factor,
                }
            )
            pd.DataFrame.from_dict(history).to_csv(FILEPATH, index=False, sep=";")

            if len(history) >= N_LAST:
                # Estimate measured flow as slope of recent weight-vs-time.
                df_tmp = pd.DataFrame.from_dict(history[-N_LAST:])
                x = df_tmp["time rel."].values.reshape((-1, 1)) / 60  # in min
                y = df_tmp["weight_measured"].values  # in mL
                model = LinearRegression().fit(x, y)
                r_sq = float(model.score(x, y))
                await write_to_database(
                    measurement="controller",
                    tags={"type": "r_squared", "name": FLOW_NAME, "unit": "-"},
                    fields={"r_squared": r_sq},
                )
                actual_flow_rate = model.coef_[0]

                # Limit correction aggressiveness to avoid large setpoint jumps.
                if abs(actual_flow_rate) < 1e-12:
                    logger.warning(
                        "Actual flow rate is too close to zero (%s); skipping correction update.",
                        actual_flow_rate,
                    )
                    correction_factor = 1.0
                else:
                    correction_factor = abs(TARGET_FLOW_RATE / actual_flow_rate)
                    correction_factor = 2.0 if correction_factor > 2.0 else correction_factor
                    correction_factor = 0.5 if correction_factor < 0.5 else correction_factor
                    await write_to_database(
                        measurement="controller",
                        tags={"type": "correction_factor", "name": FLOW_NAME, "unit": "-"},
                        fields={"correction_factor": float(correction_factor)},
                    )
                    logger.info(
                        "Correction factor | Target flow rate | Actual flow rate: %s | %s | %s",
                        correction_factor,
                        TARGET_FLOW_RATE,
                        actual_flow_rate,
                    )

                # Apply corrections only on high-quality fits and after cooldown.
                if r_sq > 0.975:
                    if correction_factor >= 1.01 or correction_factor <= 0.99:
                        if (time.time() - last_flow_rate_adjustment) >= MIN_FLOW_RATE_CHANGE_INTERVAL:
                            logger.info(
                                "coefficient of determination: %s \nintercept: %s \nslope: %s \n ---> Changing flow rate",
                                r_sq, model.intercept_, model.coef_
                            )
                            logger.info(
                                "Applied Correction factor %s to old flow rate setpoint %s",
                                correction_factor, current_flow_rate
                            )
                            next_flow_rate *= correction_factor
                            logger.info("New setpoint to be set: %s", next_flow_rate)
                            if next_flow_rate >= UPPER_LIM:
                                logger.info("New setpoint %s violates constraint: upper lim: %s", next_flow_rate, UPPER_LIM)
                                next_flow_rate = UPPER_LIM
                            if next_flow_rate <= LOWER_LIM:
                                logger.info("New setpoint %s violates constraint: lower lim: %s", next_flow_rate, LOWER_LIM)
                                next_flow_rate = LOWER_LIM
                            await set_flow_rate(pump, next_flow_rate)
                            logger.info(
                                "%s: Adjusted flow rate setpoint from %s m^3/s to %s m^3/s",
                                datetime.datetime.now(), current_flow_rate, next_flow_rate
                            )
                            last_flow_rate_adjustment = time.time()
                else:
                    print(f"Measured: {current_weight}, Expected: {weight_expected}, R^2 {r_sq}")

            # Publish telemetry points for external monitoring.
            await write_to_database(
                measurement="flow-rates",
                tags={"type": "actual_flow_rate", "name": FLOW_NAME, "unit": "mL/min"},
                fields={"flow_rate": float(actual_flow_rate)},
            )
            await write_to_database(
                measurement="flow-rates",
                tags={"type": "set_flow_rate", "name": FLOW_NAME, "unit": "mL/min"},
                fields={"flow_rate": float(next_flow_rate / UNIT_CONVERSION_FACTOR)},
            )
            await write_to_database(
                measurement="flow-rates",
                tags={"type": "flow_rate_setpoint", "name": FLOW_NAME, "unit": "mL/min"},
                fields={"flow_rate": float(current_flow_rate / UNIT_CONVERSION_FACTOR)},
            )
    except Exception as e:
        logger.error(e)
    finally:
        # Always stop background readers on exit.
        if pump_subscription is not None:
            pump_subscription.cancel()
        if balance_subscription is not None:
            balance_subscription.cancel()


async def main():
    global pump_subscription
    try:
        async with client.Client():
            await run()
    except KeyboardInterrupt:
        logger.info("Interrupt")
        async with client.Client():
            pump = await connect(name="Reglo ICC 4x Anaerob")
            await pump.pump_actuator_controller_2.stop()
            if pump_subscription is not None:
                pump_subscription.cancel()
    except asyncio.CancelledError:
        print("Execution interrupted (CancelledError). Cleaning up...")

        async def cleanup():
            logger.warning("Pump NOT stopped.")

        await asyncio.shield(cleanup())
    except Exception as error:
        print(error)
        logger.error(error)
        async with client.Client():
            pump = await connect(name="Reglo ICC 4x Anaerob")
            await pump.pump_actuator_controller_2.stop()
            if pump_subscription is not None:
                pump_subscription.cancel()
    finally:
        # Final safety shutdown.
        logger.info("Done")
        async with client.Client():
            pump = await connect(name="Reglo ICC 4x Anaerob")
            await pump.pump_actuator_controller_2.stop()
            if pump_subscription is not None:
                pump_subscription.cancel()


if __name__ == "__main__":
    asyncio.run(main())
