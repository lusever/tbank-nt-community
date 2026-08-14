use std::{sync::LazyLock, time::Duration};

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

/// T-Bank Invest API production gRPC endpoint.
pub const LIVE_ENDPOINT: &str = "https://invest-public-api.tbank.ru:443";
/// T-Bank Invest API sandbox gRPC endpoint.
pub const SANDBOX_ENDPOINT: &str = "https://sandbox-invest-public-api.tbank.ru:443";
/// Environment variable containing a production API token.
pub const LIVE_TOKEN_ENV: &str = "TBANK_INVEST_TOKEN";
/// Environment variable containing a sandbox API token.
pub const SANDBOX_TOKEN_ENV: &str = "TBANK_SANDBOX_INVEST_TOKEN";
/// Environment variable containing a production broker account ID.
pub const ACCOUNT_ID_ENV: &str = "TBANK_ACCOUNT_ID";
/// Environment variable containing a sandbox broker account ID.
pub const SANDBOX_ACCOUNT_ID_ENV: &str = "TBANK_SANDBOX_ACCOUNT_ID";
/// Canonical Nautilus client identifier string for this adapter.
pub const TBANK: &str = "TBANK";
/// Canonical Nautilus venue identifier string for Moscow Exchange instruments.
pub const MOEX: &str = "MOEX";
/// Canonical Nautilus venue identifier string for Saint Petersburg Exchange instruments.
pub const SPBE: &str = "SPBE";
/// ISO 10383 Market Identifier Code for Saint Petersburg Exchange.
pub const SPBX_MIC: &str = "SPBX";
/// ISO 10383 Market Identifier Code for the Moscow Exchange derivatives market.
pub const RTSX_MIC: &str = "RTSX";
/// Static Nautilus client identifier for this adapter.
pub static TBANK_CLIENT_ID: LazyLock<ClientId> = LazyLock::new(|| ClientId::new(Ustr::from(TBANK)));
/// Static Nautilus venue identifier for Moscow Exchange instruments.
pub static MOEX_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(MOEX)));
/// Static Nautilus venue identifier for Saint Petersburg Exchange instruments.
pub static SPBE_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(SPBE)));
/// Static broker venue identifier used by the multi-venue execution client.
pub static TBANK_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(TBANK)));
/// T-Bank class code for the main MOEX equity board.
pub const TQBR_CLASS_CODE: &str = "TQBR";
/// T-Bank class code for MOEX futures contracts.
pub const SPBFUT_CLASS_CODE: &str = "SPBFUT";
/// ISO currency code used by the supported instrument universe.
pub const RUB_CURRENCY: &str = "RUB";
/// Default timeout applied to unary gRPC requests.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Number of nanoseconds in one whole unit.
pub const NANOS_PER_UNIT: i64 = 1_000_000_000;
