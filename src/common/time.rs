use prost_types::Timestamp;

use crate::common::{
    consts::NANOS_PER_UNIT,
    error::{Result, TbankAdapterError},
};

/// Converts a protobuf timestamp to Unix nanoseconds.
pub fn timestamp_to_unix_nanos(timestamp: &Timestamp) -> Result<i128> {
    if timestamp.nanos < 0 || timestamp.nanos >= NANOS_PER_UNIT as i32 {
        return Err(TbankAdapterError::ConversionError(format!(
            "invalid timestamp nanos {}",
            timestamp.nanos
        )));
    }

    Ok(i128::from(timestamp.seconds) * i128::from(NANOS_PER_UNIT) + i128::from(timestamp.nanos))
}

/// Converts Unix nanoseconds to a protobuf timestamp.
pub fn unix_nanos_to_timestamp(nanos: i128) -> Result<Timestamp> {
    let seconds = nanos.div_euclid(i128::from(NANOS_PER_UNIT));
    let sub_nanos = nanos.rem_euclid(i128::from(NANOS_PER_UNIT));

    Ok(Timestamp {
        seconds: i64::try_from(seconds).map_err(|_| {
            TbankAdapterError::ConversionError(format!("timestamp seconds out of range: {seconds}"))
        })?,
        nanos: i32::try_from(sub_nanos).map_err(|_| {
            TbankAdapterError::ConversionError(format!("timestamp nanos out of range: {sub_nanos}"))
        })?,
    })
}
