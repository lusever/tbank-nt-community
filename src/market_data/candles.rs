pub(crate) const ONE_MINUTE_NANOS: i128 = 60_000_000_000;
pub(crate) const ONE_DAY_NANOS: i128 = 24 * 60 * ONE_MINUTE_NANOS;

pub(crate) fn one_minute_candle_query_chunks(
    from_nanos: i128,
    to_nanos: i128,
) -> Vec<(i128, i128)> {
    let mut chunks = Vec::new();
    let mut cursor = from_nanos;
    while cursor < to_nanos {
        let end = cursor.saturating_add(ONE_DAY_NANOS).min(to_nanos);
        chunks.push((cursor, end));
        cursor = end;
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_minute_candle_query_chunks_limit_requests_to_one_day() {
        let chunks = one_minute_candle_query_chunks(0, ONE_DAY_NANOS * 2 + ONE_MINUTE_NANOS);

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], (0, ONE_DAY_NANOS));
        assert_eq!(chunks[1], (ONE_DAY_NANOS, ONE_DAY_NANOS * 2));
        assert_eq!(
            chunks[2],
            (ONE_DAY_NANOS * 2, ONE_DAY_NANOS * 2 + ONE_MINUTE_NANOS)
        );
    }
}
