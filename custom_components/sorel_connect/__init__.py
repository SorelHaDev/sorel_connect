from __future__ import annotations

import asyncio
import logging
import os
import shutil
import socket
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry, ConfigEntryNotReady
from homeassistant.const import CONF_HOST, CONF_PORT, CONF_USERNAME, CONF_PASSWORD, Platform
from homeassistant.helpers.dispatcher import async_dispatcher_send, async_dispatcher_connect

from .const import (
    DOMAIN,
    CONF_USE_HA_MQTT,
    CONF_BROKER_TLS,
    CONF_API_SERVER,
    CONF_API_URL,
    DEFAULT_API_SERVER,
    DEFAULT_API_URL,
    SIGNAL_MQTT_CONNECTION_STATE,
    SIGNAL_METADATA_CHANGED,
)
from .mqtt_client import MqttClient, HaMqttClient, CustomMqttClient
from .meta_client import MetaClient
from .coordinator import Coordinator
from .sensor_types import load_sensor_types, load_relay_modes

PLATFORMS = [Platform.SENSOR, Platform.BINARY_SENSOR]

_LOGGER = logging.getLogger(__name__)

def clear_metadata_cache(cache_dir: str = "/config/sorel_meta_cache") -> int:
    """
    Clear all cached metadata files.
    Returns the number of files deleted.
    """
    if not os.path.exists(cache_dir):
        _LOGGER.debug("Cache directory does not exist: %s", cache_dir)
        return 0

    try:
        file_count = len([f for f in os.listdir(cache_dir) if os.path.isfile(os.path.join(cache_dir, f))])
        shutil.rmtree(cache_dir)
        os.makedirs(cache_dir, exist_ok=True)
        _LOGGER.info("Cleared metadata cache: deleted %d files from %s", file_count, cache_dir)
        return file_count
    except Exception as e:
        _LOGGER.error("Failed to clear metadata cache at %s: %s", cache_dir, e)
        return 0

async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up the Sorel Connect integration from YAML (not used)."""

    # Register service once at module level
    async def handle_clear_cache(call):
        """Handle the clear_metadata_cache service call."""
        _LOGGER.info("Clear metadata cache service called")
        count = clear_metadata_cache(hass.config.path("sorel_meta_cache"))
        _LOGGER.info("Service cleared %d cached metadata files", count)

    # Only register if not already registered
    if not hass.services.has_service(DOMAIN, "clear_metadata_cache"):
        hass.services.async_register(DOMAIN, "clear_metadata_cache", handle_clear_cache)
        _LOGGER.debug("Registered clear_metadata_cache service")

    return True

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    # Load sensor types CSV early (before any sensors are created)
    sensor_types = load_sensor_types()
    _LOGGER.info("Sensor types loaded: %d types available", len(sensor_types))

    # Load relay modes CSV
    relay_modes = load_relay_modes()
    _LOGGER.info("Relay modes loaded: %d modes available", len(relay_modes))

    data = entry.data
    use_ha_mqtt = data.get(CONF_USE_HA_MQTT, True)

    # API settings: check options first, then data (backwards compat), then defaults
    api_server = entry.options.get(
        CONF_API_SERVER,
        data.get(CONF_API_SERVER, DEFAULT_API_SERVER)
    )
    api_url_template = entry.options.get(
        CONF_API_URL,
        data.get(CONF_API_URL, DEFAULT_API_URL)
    )

    # Determine MQTT mode for logging
    if use_ha_mqtt:
        _LOGGER.debug("-INIT 0/5: Setting up %s with HA MQTT integration, api_server=%s api_url=%s",
                      DOMAIN, api_server, api_url_template)
    else:
        host = data.get(CONF_HOST)
        port = data.get(CONF_PORT, 1883)
        tls = bool(data.get(CONF_BROKER_TLS, False))
        _LOGGER.debug("-INIT 0/5: Setting up %s: host=%s port=%s tls=%s api_server=%s api_url=%s",
                      DOMAIN, host, port, tls, api_server, api_url_template)

    # 1) Connect to MQTT (either HA MQTT or custom broker)
    try:
        async def on_msg(topic: str, payload: bytes):
            await hass.data[DOMAIN]["coordinator"].handle_message(topic, payload)

        async def on_connection_change(is_connected: bool):
            """Notify all listeners about MQTT connection state change."""
            _LOGGER.info("MQTT connection state changed: %s", "connected" if is_connected else "disconnected")
            async_dispatcher_send(hass, SIGNAL_MQTT_CONNECTION_STATE, is_connected)

        if use_ha_mqtt:
            # Use Home Assistant MQTT integration
            mqtt_client = HaMqttClient(
                hass=hass,
                on_message=on_msg,
                on_connection_change=on_connection_change
            )
        else:
            # Use custom MQTT broker
            host = data.get(CONF_HOST)
            port = data.get(CONF_PORT, 1883)
            username = data.get(CONF_USERNAME) or None
            password = data.get(CONF_PASSWORD) or None
            tls = bool(data.get(CONF_BROKER_TLS, False))

            mqtt_client = CustomMqttClient(
                host=host,
                port=port,
                username=username,
                password=password,
                tls_enabled=tls,
                on_message=on_msg,
                on_connection_change=on_connection_change
            )

        await mqtt_client.connect()  # Raises exception if connection fails

        _LOGGER.debug("-INIT 1/5: MQTT connected (%s mode)", "HA MQTT" if use_ha_mqtt else "custom broker")
    except asyncio.TimeoutError:
        if use_ha_mqtt:
            _LOGGER.error("-INIT 1/5: Timeout waiting for HA MQTT integration")
            raise ConfigEntryNotReady("Timeout waiting for Home Assistant MQTT integration. Ensure MQTT is configured.")
        else:
            _LOGGER.error("-INIT 1/5: MQTT connection timed out for %s:%s", host, port)
            raise ConfigEntryNotReady(f"MQTT broker connection timed out ({host}:{port}). Check if broker is responding.")
    except ConnectionRefusedError:
        _LOGGER.error("-INIT 1/5: MQTT broker refused connection on %s:%s (broker not running or wrong port)", host, port)
        raise ConfigEntryNotReady(f"MQTT connection refused by {host}:{port}. Check if broker is running and port is correct.")
    except socket.gaierror as err:
        _LOGGER.error("-INIT 1/5: Cannot resolve hostname '%s': %s", host, err)
        raise ConfigEntryNotReady(f"Cannot resolve MQTT broker hostname '{host}'. Check if the address is correct.")
    except ConnectionError as err:
        _LOGGER.error("-INIT 1/5: MQTT connection failed: %s", err)
        # Check if it's HA MQTT not configured
        if "MQTT integration is not configured" in str(err):
            raise ConfigEntryNotReady("Home Assistant MQTT integration is not configured. Please configure MQTT first.")
        # Check if it's an authentication error
        elif "Bad username or password" in str(err) or "Not authorized" in str(err):
            raise ConfigEntryNotReady(f"MQTT authentication failed. Check username/password.")
        else:
            raise ConfigEntryNotReady(f"Failed to connect to MQTT: {err}")
    except OSError as err:
        _LOGGER.error("-INIT 1/5: Network error connecting to MQTT: %s", err)
        raise ConfigEntryNotReady(f"Network error connecting to MQTT broker. Check network/firewall.")
    except Exception as err:
        _LOGGER.exception("-INIT 1/5: Unexpected error connecting to MQTT")
        raise ConfigEntryNotReady(f"Unexpected error connecting to MQTT: {err}")

    # 2) Initialize metadata client (use HA session)
    try:
        from homeassistant.helpers import aiohttp_client
        session = aiohttp_client.async_get_clientsession(hass)
        cache_dir = hass.config.path("sorel_meta_cache")
        meta = MetaClient(api_server, api_url_template, session, cache_dir=cache_dir)
        _LOGGER.debug("-INIT 2/5: Meta client initialized for %s%s (cache: %s)", api_server, api_url_template, cache_dir)
    except Exception as err:
        _LOGGER.exception("-INIT 2/5: Meta client init/healthcheck failed")
        mqtt_client.stop()
        raise ConfigEntryNotReady from err

    # 3) Create coordinator (but don't start it yet)
    try:
        coord = Coordinator(
            hass=hass,
            mqtt_gw=mqtt_client,
            meta_client=meta,
        )
        _LOGGER.debug("-INIT 3/5: Coordinator created")
    except Exception as err:
        _LOGGER.exception("-INIT 3/5: Coordinator creation failed")
        mqtt_client.stop()
        raise ConfigEntryNotReady from err

    # 4) Store state BEFORE loading platforms (platforms need access to coordinator)
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN] = {
        "mqtt": mqtt_client,
        "coordinator": coord,
        "meta_client": meta,
        "session": session,
    }
    entry.async_on_unload(entry.add_update_listener(async_reload_entry))

    # 5) Load platforms FIRST - this registers all signal callbacks (CRITICAL!)
    # Platforms must be loaded BEFORE coordinator starts to avoid race condition
    # where MQTT messages arrive before callbacks are registered
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    _LOGGER.debug("-INIT 4/5: Platforms loaded, callbacks registered")

    # 6) NOW start coordinator - platforms are ready to receive signals
    try:
        await coord.start()
        _LOGGER.debug("-INIT 5/5: Coordinator started, setup complete")
    except Exception as err:
        _LOGGER.exception("-INIT 5/5: Coordinator start failed")
        mqtt_client.stop()
        raise ConfigEntryNotReady from err

    # 7) Register metadata change notification handler
    async def on_metadata_changed(device_key: str):
        """Handle metadata change detection - create persistent notification."""
        _LOGGER.info("Metadata changed notification for device: %s", device_key)

        # Create persistent notification
        await hass.services.async_call(
            "persistent_notification",
            "create",
            {
                "title": "Sorel Connect: Metadata Updated",
                "message": (
                    f"Device metadata has been updated for **{device_key}**.\n\n"
                    "To apply the new sensor names and configurations, please reload the Sorel Connect integration:\n\n"
                    "**Settings → Devices & Services → Sorel Connect → ⋮ → Reload**"
                ),
                "notification_id": f"sorel_metadata_changed_{device_key.replace(':', '_')}",
            },
        )

    unsub_metadata = async_dispatcher_connect(
        hass, SIGNAL_METADATA_CHANGED, on_metadata_changed
    )
    entry.async_on_unload(unsub_metadata)

    return True

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    data = hass.data.get(DOMAIN)
    if not data:
        return True

    # Cleanup meta client retry tasks
    try:
        meta_client = data.get("meta_client")
        if meta_client:
            await meta_client.close()
            _LOGGER.debug("Meta client cleanup completed")
    except Exception as err:
        _LOGGER.warning("Error during meta client cleanup: %s", err, exc_info=True)

    # Stop MQTT gateway
    try:
        data["mqtt"].stop()
    except Exception as err:
        _LOGGER.warning("Error stopping MQTT gateway: %s", err, exc_info=True)

    hass.data.pop(DOMAIN, None)

    # Note: Service is NOT unregistered here since it's registered globally in async_setup()
    # It will be cleaned up when HA shuts down or the integration is fully removed

    return unload_ok

async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await async_unload_entry(hass, entry)
    await async_setup_entry(hass, entry)
