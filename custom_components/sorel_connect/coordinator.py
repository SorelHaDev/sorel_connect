from __future__ import annotations
import logging
import json
import time
from collections import defaultdict
from typing import Dict, Set, List, Tuple, Any, Optional
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send
from .const import DOMAIN, SIGNAL_NEW_DEVICE, SIGNAL_DP_UPDATE, SIGNAL_METADATA_CHANGED
from .topic_parser import parse_topic, ParsedTopic
from .sensor_types import (
    is_sensor_type_register,
    parse_sensor_name,
    is_relay_mode_register,
    decode_relay_value,
)

_LOGGER = logging.getLogger(__name__)

STALE_REGISTER_MAX_AGE = 10.0  # Maximum age in seconds for related registers to be considered fresh
REGISTER_CLEANUP_AGE = 3600.0  # Clean up registers older than 1 hour
TEMP_UNIT_REGISTER = 521  # Register holding temperature unit setting (0=°C, 1=°F)

class Coordinator:
    """Central coordinator for MQTT message handling, device discovery, and datapoint decoding."""

    def __init__(self, hass: HomeAssistant, mqtt_gw, meta_client) -> None:
        self.hass = hass
        self.mqtt = mqtt_gw
        self.meta = meta_client
        self._known_devices: Set[str] = set()

        # Register storage: device_key -> { address: (value, timestamp) }
        self._registers: dict[str, dict[int, Tuple[int, float]]] = defaultdict(dict)
        # Datapoint metadata: device_key -> List[dict]
        self._datapoints: dict[str, List[dict]] = defaultdict(list)
        # Decoded values: device_key -> { datapoint_start_address: decoded_value }
        self._dp_value_cache: dict[str, dict[int, Any]] = defaultdict(dict)

        # Sensor type tracking: device_key -> { "S1": type_id, "S2": type_id, ... }
        self._sensor_type_values: dict[str, dict[str, int]] = defaultdict(dict)
        # Relay mode tracking: device_key -> { "R1": mode_id, "R2": mode_id, ... }
        self._relay_mode_values: dict[str, dict[str, int]] = defaultdict(dict)
        # Temperature unit setting: device_key -> 0 (°C) or 1 (°F)
        self._temp_unit: dict[str, int] = {}
        # Parsed topics storage: device_key -> ParsedTopic
        self._parsed_topics: dict[str, ParsedTopic] = {}
        # Full metadata storage: device_key -> full metadata dict (including "meta" section)
        self._full_metadata: dict[str, dict] = {}
        # Devices with changed metadata (detected on startup refresh)
        self._metadata_changed_devices: Set[str] = set()

    async def start(self) -> None:
        """Start the coordinator by subscribing to MQTT topics."""
        self.mqtt.subscribe("+/device/+/+/+/+/dp/+/+")
        _LOGGER.debug("Subscribed to topic wildcard for device datapoints")

    async def handle_message(self, topic: str, payload: bytes) -> None:
        """Handle incoming MQTT message: parse topic, discover devices, decode datapoints."""
        pt = parse_topic(topic)
        if not pt:
            _LOGGER.debug("Ignored topic (no match): %s", topic)
            return

        # Store parsed topic for this device
        self._parsed_topics[pt.device_key] = pt

        # New device discovered?
        if pt.device_key not in self._known_devices:
            self._known_devices.add(pt.device_key)
            _LOGGER.info("Discovered new device: %s (%s:%s)", pt.device_key, getattr(pt, "oem_name", "?"), getattr(pt, "device_name", "?"))
            # Store parsed topic in hass.data for platform access
            self.hass.data.setdefault(DOMAIN, {}).setdefault("parsed_topics", {})[pt.device_key] = pt

            # Load metadata using IDs from MQTT topic
            # Convert hex IDs to decimal for API calls
            organization_id_hex = pt.oem_id
            device_enum_id_hex = getattr(pt, "device_id", None)

            # Convert organization ID from hex to decimal
            try:
                organization_id = str(int(organization_id_hex, 16))
                _LOGGER.debug(f"Converted organization_id {organization_id_hex} (hex) → {organization_id} (decimal)")
            except (ValueError, TypeError):
                _LOGGER.warning(f"Failed to convert oem_id '{organization_id_hex}' from hex to decimal, using as-is")
                organization_id = organization_id_hex

            # Convert device enum ID from hex to decimal
            if device_enum_id_hex:
                try:
                    device_enum_id = str(int(device_enum_id_hex, 16))
                    _LOGGER.debug(f"Converted device_id {device_enum_id_hex} (hex) → {device_enum_id} (decimal)")
                except (ValueError, TypeError):
                    _LOGGER.warning(f"Failed to convert device_id '{device_enum_id_hex}' from hex to decimal, using as-is")
                    device_enum_id = device_enum_id_hex
            else:
                device_enum_id = None

            if device_enum_id:
                try:
                    # Use refresh_metadata to detect changes from API
                    meta, has_changed = await self.meta.refresh_metadata(organization_id, device_enum_id)
                    if meta:
                        # Store full metadata (including "meta" section)
                        self._full_metadata[pt.device_key] = meta
                        datapoints = meta.get("datapoints", [])
                        self.register_datapoints(pt.device_key, datapoints)
                        _LOGGER.info("Metadata for device %s loaded (%d datapoints)", pt.device_key, len(datapoints))
                        # Track if metadata changed and notify
                        if has_changed:
                            self._metadata_changed_devices.add(pt.device_key)
                            _LOGGER.warning("Metadata has changed for device %s - reload integration to apply changes", pt.device_key)
                            async_dispatcher_send(self.hass, SIGNAL_METADATA_CHANGED, pt.device_key)
                    else:
                        _LOGGER.warning(f"No metadata available for device {pt.device_key}")
                except Exception as e:
                    _LOGGER.warning(f"Failed to load metadata for device {pt.device_key}: {e}")
            async_dispatcher_send(self.hass, SIGNAL_NEW_DEVICE, pt)

        # Attempt to extract register value
        address, value = self._extract_register(payload, pt)
        if address is not None and value is not None:
            _LOGGER.debug("Extracted register from topic %s: address=%s, value=%s", topic, address, value)
            self.update_register(pt.device_key, address, value)
        else:
            _LOGGER.debug("Failed to extract register from topic %s, payload=%s", topic, payload.decode('utf-8', errors='ignore')[:100])

    # --- Datapoint Management -------------------------------------------------

    def register_datapoints(self, device_key: str, datapoints: List[dict]) -> None:
        """Register metadata datapoints for a device."""
        self._datapoints[device_key] = datapoints

    def get_datapoint_value(self, device_key: str, address: int) -> Any:
        """Get decoded value for a specific datapoint address."""
        return self._dp_value_cache.get(device_key, {}).get(address)

    def is_device_metadata_available(self, device_key: str) -> bool:
        """
        Check if metadata is available for a device.
        Returns False if metadata fetch failed or device not found.
        """
        # Get parsed topic to extract device_id
        parsed_topics = self.hass.data.get(DOMAIN, {}).get("parsed_topics", {})
        pt = parsed_topics.get(device_key)
        if not pt:
            # Device not yet fully registered, assume unavailable
            return False

        # Check metadata status via meta client
        organization_id = pt.oem_id
        device_enum_id = getattr(pt, "device_id", None)
        if not device_enum_id:
            return False

        status = self.meta.get_device_status(organization_id, device_enum_id)
        return status == "ok"

    def get_sensor_type(self, device_key: str, sensor_name: str) -> Optional[int]:
        """
        Get sensor type ID for a sensor input (e.g., 'S1', 'S2').

        Args:
            device_key: Device identifier (mac::network_id)
            sensor_name: Sensor name like "S1", "S2", etc.

        Returns:
            Type ID (int) if known, None if not yet received
        """
        return self._sensor_type_values.get(device_key, {}).get(sensor_name)

    def get_temp_unit(self, device_key: str) -> int:
        """
        Get temperature unit setting for a device.

        Args:
            device_key: Device identifier (mac::network_id)

        Returns:
            0 for °C (default), 1 for °F
        """
        return self._temp_unit.get(device_key, 0)

    def get_metadata_info(self, device_key: str) -> Optional[dict]:
        """
        Get metadata information for a device.

        Args:
            device_key: Device identifier (mac::network_id)

        Returns:
            Dictionary with metadata info including status and meta fields, or None if device not known
        """
        pt = self._parsed_topics.get(device_key)
        if not pt:
            return None

        # Convert hex IDs to decimal (same logic as device discovery)
        organization_id_hex = pt.oem_id
        device_enum_id_hex = getattr(pt, "device_id", None)

        try:
            organization_id = str(int(organization_id_hex, 16))
        except (ValueError, TypeError):
            organization_id = organization_id_hex

        if device_enum_id_hex:
            try:
                device_enum_id = str(int(device_enum_id_hex, 16))
            except (ValueError, TypeError):
                device_enum_id = device_enum_id_hex
        else:
            device_enum_id = None

        # Get metadata status from meta client
        if device_enum_id:
            status_details = self.meta.get_status_details(organization_id, device_enum_id)
        else:
            status_details = {
                "status": "error",
                "message": "No device ID available",
                "retry_count": 0,
                "last_error_time": None,
            }

        result = {
            "status": status_details["status"],
            "status_message": status_details["message"],
            "retry_count": status_details["retry_count"],
            "organization_id_hex": organization_id_hex,
            "organization_id_decimal": organization_id,
            "device_enum_id_hex": device_enum_id_hex,
            "device_enum_id_decimal": device_enum_id,
        }

        # Add metadata fields if available
        full_meta = self._full_metadata.get(device_key)
        if full_meta and "meta" in full_meta:
            meta_section = full_meta["meta"]
            result.update({
                "device_description": meta_section.get("deviceDescription"),
                "language": meta_section.get("language"),
                "datapoint_count": meta_section.get("count"),
                "generated_at": meta_section.get("generatedAt"),
            })

        return result

    def get_relay_mode(self, device_key: str, relay_name: str) -> Optional[int]:
        """
        Get relay mode ID for a relay output (e.g., 'R1', 'R2').

        Args:
            device_key: Device identifier (mac::network_id)
            relay_name: Relay name like "R1", "R2", etc.

        Returns:
            Mode ID (int) if known, None if not yet received
        """
        return self._relay_mode_values.get(device_key, {}).get(relay_name)

    def get_metadata_changed_devices(self) -> Set[str]:
        """
        Get the set of devices that have changed metadata since last startup.

        Returns:
            Set of device_key strings with changed metadata
        """
        return self._metadata_changed_devices.copy()

    def clear_metadata_changed_devices(self) -> None:
        """Clear the set of devices with changed metadata (after notification sent)."""
        self._metadata_changed_devices.clear()

    def get_dp_at_address(self, device_key: str, address: int) -> Optional[dict]:
        """
        Get datapoint metadata at a specific address.

        Args:
            device_key: Device identifier (mac::network_id)
            address: Modbus register address

        Returns:
            Datapoint metadata dict or None if not found
        """
        datapoints = self._datapoints.get(device_key, [])
        return next((dp for dp in datapoints if int(dp.get("address", -1)) == address), None)

    # --- Register Update + Decoding -------------------------------------------

    def update_register(self, device_key: str, address: int, value: int) -> None:
        """Update register value and attempt to decode associated datapoints."""
        now = time.time()
        self._registers[device_key][address] = (value & 0xFFFF, now)
        _LOGGER.debug("Register updated for device %s: address=%s, value=%s (0x%04X)", device_key, address, value, value & 0xFFFF)

        # Periodically clean up old registers to prevent memory growth
        # Check randomly (1% chance per update) to avoid overhead
        import random
        if random.random() < 0.01:
            self._cleanup_old_registers()

        # Check if this is the temperature unit register
        if address == TEMP_UNIT_REGISTER:
            old_unit = self._temp_unit.get(device_key)
            self._temp_unit[device_key] = value
            _LOGGER.info("Temperature unit for device %s set to %s (%s)",
                        device_key, value, "°F" if value == 1 else "°C")
            if old_unit is not None and old_unit != value:
                _LOGGER.info("Temperature unit changed from %s to %s for device %s", old_unit, value, device_key)

        # Check datapoints linearly (optimization possible later)
        datapoints = self._datapoints.get(device_key, [])
        for dp in datapoints:
            start = int(dp.get("address", -1))
            if start < 0:
                continue
            length_bytes = int(dp.get("length", 0))
            if length_bytes <= 0:
                continue
            reg_needed = (length_bytes + 1) // 2
            if not (start <= address < start + reg_needed):
                continue
            # Address matches this datapoint's range
            dp_name = dp.get("name", "?")

            # Check if this is a sensor type register (e.g., "S1 Type")
            sensor_name = is_sensor_type_register(dp_name)

            # Check if this is a relay mode register (e.g., "R1 Mode")
            relay_name = is_relay_mode_register(dp_name)

            decoded = self._try_decode_dp(device_key, dp, start, reg_needed, length_bytes)
            if decoded is not None:
                _LOGGER.debug("Decoded datapoint '%s' at address %s: value=%s", dp_name, address, decoded)
                prev = self._dp_value_cache[device_key].get(start)
                if decoded != prev:
                    # Determine if we should cache this value
                    should_cache = True

                    # Check if this is an S<n> sensor without a known type
                    # If so, don't cache - wait until type is known
                    sensor_num = parse_sensor_name(dp_name)
                    if sensor_num is not None:
                        # This is an S<n> sensor - only cache if type is known
                        type_id = self.get_sensor_type(device_key, dp_name)
                        if type_id is None:
                            should_cache = False
                            _LOGGER.debug("Skipping cache for %s (type not yet known, will retry on next update)", dp_name)

                    # Check if address N+1 has a mode register - if so, this is a relay
                    # Use address-based detection instead of name pattern matching
                    mode_dp = self.get_dp_at_address(device_key, start + 1)
                    if mode_dp and is_relay_mode_register(mode_dp.get("name", "")):
                        # This is a relay - lookup mode by this datapoint's actual name
                        mode_id = self.get_relay_mode(device_key, dp_name)
                        if mode_id is None:
                            should_cache = False
                            _LOGGER.debug("Skipping cache for relay %s (mode not yet known, will retry on next update)", dp_name)
                        else:
                            # Decode relay value based on mode before caching
                            decoded = decode_relay_value(decoded, mode_id)
                            _LOGGER.debug("Decoded relay %s with mode %s: raw=%s, decoded=%s",
                                        dp_name, mode_id, self._try_decode_dp(device_key, dp, start, reg_needed, length_bytes), decoded)

                    # Cache the value if appropriate
                    if should_cache:
                        self._dp_value_cache[device_key][start] = decoded

                    # If this is a sensor type register, store the type mapping
                    if sensor_name and isinstance(decoded, (int, float)):
                        type_id = int(decoded)
                        old_type = self._sensor_type_values[device_key].get(sensor_name)
                        self._sensor_type_values[device_key][sensor_name] = type_id
                        _LOGGER.info("Sensor type for %s on device %s set to %s (type_id=%s)",
                                    sensor_name, device_key, dp_name, type_id)
                        if old_type is not None and old_type != type_id:
                            _LOGGER.warning("Sensor type changed from %s to %s for %s on device %s",
                                          old_type, type_id, sensor_name, device_key)

                    # If this is a relay mode register, store the mode mapping
                    # Use address-based detection: mode at address N describes relay at address N-1
                    if relay_name and isinstance(decoded, (int, float)):
                        mode_id = int(decoded)

                        # Look up the relay at address N-1 to get its actual name
                        relay_dp = self.get_dp_at_address(device_key, start - 1)
                        if relay_dp:
                            actual_relay_name = relay_dp.get("name", relay_name)
                            old_mode = self._relay_mode_values[device_key].get(actual_relay_name)
                            self._relay_mode_values[device_key][actual_relay_name] = mode_id
                            _LOGGER.info("Relay mode for %s (at address %s) on device %s set to %s (mode_id=%s) from mode register '%s' (at address %s)",
                                        actual_relay_name, start - 1, device_key, dp_name, mode_id, dp_name, start)
                            if old_mode is not None and old_mode != mode_id:
                                _LOGGER.warning("Relay mode changed from %s to %s for %s on device %s",
                                              old_mode, mode_id, actual_relay_name, device_key)
                        else:
                            _LOGGER.debug("No relay found at address %s for mode register '%s' (address %s)",
                                         start - 1, dp_name, start)

                    # Always dispatch signal (even if not cached)
                    _LOGGER.debug("Dispatching SIGNAL_DP_UPDATE for device=%s, address=%s, value=%s (prev=%s, cached=%s)",
                                 device_key, start, decoded, prev, should_cache)
                    async_dispatcher_send(
                        self.hass,
                        SIGNAL_DP_UPDATE,
                        device_key,
                        start,
                        decoded
                    )

    def _try_decode_dp(self, device_key: str, dp: dict, start: int, reg_count: int, length_bytes: int) -> Optional[Any]:
        """
        Attempt to decode a datapoint from accumulated registers.

        Returns decoded value or None if registers are incomplete/stale.
        """
        regs = self._registers.get(device_key, {})
        words: list[int] = []
        now = time.time()
        for off in range(reg_count):
            a = start + off
            item = regs.get(a)
            if item is None:
                return None  # Missing register, quietly skip
            val, ts = item
            if now - ts > STALE_REGISTER_MAX_AGE:
                return None  # Stale register, quietly skip
            words.append(val)

        raw_bytes = bytearray()
        for w in words:
            raw_bytes.extend(w.to_bytes(2, "big"))
        raw_bytes = raw_bytes[:length_bytes]

        dtype = (dp.get("type") or "").lower()
        try:

            value = None

            if dtype in ("uns8", "uint8"):
                value = int.from_bytes(raw_bytes, "big", signed=False)
            elif dtype in ("uns16", "uint16"):
                value = int.from_bytes(raw_bytes, "big", signed=False)
            elif dtype in ("int16","sig16"):
                value = int.from_bytes(raw_bytes, "big", signed=True)
            elif dtype in ("uns32", "uint32"):
                value =  int.from_bytes(raw_bytes.ljust(4, b"\x00"), "big", signed=False)
            elif dtype in ("int32","sig32"):
                value = int.from_bytes(raw_bytes.ljust(4, b"\x00"), "big", signed=True)
            elif dtype in ("float32", "float"):
                if len(raw_bytes) < 4:
                    return None
                import struct
                value = struct.unpack(">f", raw_bytes[:4])[0]
            elif dtype in ("bool", "boolean"):
                return bool(raw_bytes[0] & 0x01)
            elif dtype.startswith("str") or dtype.startswith("char"):
                return raw_bytes.decode("utf-8", errors="ignore").rstrip("\x00")

            if value is not None:
                # Check for sensor error codes across all integer types
                # -32767: sensor not connected
                # -32768: sensor error or does not exist
                if isinstance(value, int) and value in (-32767, -32768):
                    _LOGGER.debug("Detected sensor error code %d for '%s', returning unchanged",
                                 value, dp.get("name", "?"))
                    return value  # Skip step and format processing

                # Normal values: apply step multiplication
                value = value * dp.get("step", 1)

                if dp.get("format", 1) != "":
                    # if given the format is structured like {\r\n    \"0\": \"Off\",\r\n    \"1\": \"Daily\",\r\n    \"2\": \"Weekly\"\r\n}
                    try:
                        fmt = json.loads(dp.get("format"))
                        if isinstance(fmt, dict):
                            value_str = str(value)
                            if value_str in fmt:
                                return fmt[value_str]  # Return formatted string
                            else:
                                _LOGGER.debug(f"Value '{value_str}' not in format mapping for '{dp.get('name')}' (keys: {list(fmt.keys())})")
                        else:
                            _LOGGER.warning(f"Format for '{dp.get('name')}' is not a dictionary")
                    except Exception as e:
                        _LOGGER.warning(f"Error applying format for '{dp.get('name')}': {e}")

                return value

            # Fallback to hex representation
            _LOGGER.warning(f"Unknown data type: {dtype}, raw bytes: {raw_bytes.hex()}")
            return raw_bytes.hex()

        except Exception as e:
            _LOGGER.warning("Decoding failed for DP '%s' (type=%s): %s", dp.get("name", "?"), dtype, e)
            return None

    def _cleanup_old_registers(self) -> None:
        """Remove registers older than REGISTER_CLEANUP_AGE to prevent memory growth."""
        now = time.time()
        total_removed = 0

        for device_key in list(self._registers.keys()):
            device_regs = self._registers[device_key]
            addresses_to_remove = [
                addr for addr, (_, timestamp) in device_regs.items()
                if now - timestamp > REGISTER_CLEANUP_AGE
            ]

            for addr in addresses_to_remove:
                del device_regs[addr]
                total_removed += 1

            # Remove device entry if no registers remain
            if not device_regs:
                del self._registers[device_key]

        if total_removed > 0:
            _LOGGER.debug("Cleaned up %d old registers (age > %.0fs)", total_removed, REGISTER_CLEANUP_AGE)

    # --- Helper Functions -----------------------------------------------------

    def _extract_register(self, payload: bytes, pt: ParsedTopic) -> Tuple[Optional[int], Optional[int]]:
        """
        Extract register address and value from topic and payload.

        Address comes from ParsedTopic.address (topic segment 8).
        Value is numeric, either in JSON {"value": X} or plain text.

        Returns:
            Tuple of (address, value) or (None, None) if extraction fails.
        """
        # 1. Get address from parsed topic (segment 8)
        address = None
        try:
            address = int(pt.address)
        except (ValueError, AttributeError) as e:
            _LOGGER.debug("Failed to extract address from ParsedTopic.address='%s': %s", getattr(pt, 'address', None), e)
            return None, None

        # 2. Parse value from payload
        value = None
        try:
            text = payload.decode("utf-8", errors="ignore").strip()

            # Try JSON format: {"value": 123}
            if text.startswith("{"):
                try:
                    obj = json.loads(text)
                    if isinstance(obj, dict) and "value" in obj:
                        value = int(obj["value"])
                except (json.JSONDecodeError, ValueError, KeyError):
                    pass

            # Try plain numeric
            if value is None:
                try:
                    value = int(text)
                except ValueError:
                    pass

        except Exception as e:
            _LOGGER.debug("Failed to parse value from payload: %s", e)

        if value is None:
            _LOGGER.debug("Could not extract numeric value from payload: %s", payload.decode('utf-8', errors='ignore')[:100])
            return None, None

        return address, value
